import type { Pool, PoolClient } from 'pg'

export type SaleNumbers = {
  readonly stock: number
  readonly startMs: number
  readonly endMs: number
}

export type Snapshot = {
  readonly left: number
  readonly startMs: number
  readonly endMs: number
}

export type Win = {
  readonly buyerId: string
  /** The buyer's place in the queue, issued by Redis. */
  readonly seq: number
  readonly topic: string
  readonly partition: number
  readonly offset: number
}

/** `already-recorded` covers both unique constraints: a repeat buyer and a taken place. */
export type Recorded = 'written' | 'replayed' | 'already-recorded' | 'no-unit-left'

// Matches the SSE tick, so an open page never sees a number older than one tick.
export const CACHE_MS = 250

const READ_SALE = 'SELECT units_left, start_at, end_at FROM stock WHERE id = 1'

const READ_CAMPAIGN = 'SELECT total_units, start_at, end_at FROM stock WHERE id = 1'

// I insert first so every transaction takes the same lock in the same order.
// Without it, Postgres hit `deadlock detected` at 100 parallel records.
const CLAIM_OFFSET = `INSERT INTO queue_offsets (topic, partition, next_offset) VALUES ($1, $2, 0)
   ON CONFLICT (topic, partition) DO NOTHING`

const READ_OFFSET =
  'SELECT next_offset FROM queue_offsets WHERE topic = $1 AND partition = $2 FOR UPDATE'

// A rebalance can give two workers one partition for a moment. I use GREATEST
// to keep the later record from pulling the resume point backwards.
const BUMP_OFFSET = `INSERT INTO queue_offsets (topic, partition, next_offset) VALUES ($1, $2, $3)
   ON CONFLICT (topic, partition)
   DO UPDATE SET next_offset = GREATEST(queue_offsets.next_offset, EXCLUDED.next_offset)`

// The bare `ON CONFLICT` covers `orders_seq_key` as well as `orders_user_id_key`.
// Named, a repeat `seq` raised 23505, and the partition then wedged for good.
const TAKE_BUYER =
  'INSERT INTO orders (user_id, seq) VALUES ($1, $2) ON CONFLICT DO NOTHING RETURNING user_id'

// Postgres holds the one stock row for the transaction. A second worker reads the
// count only after the first commits. The `units_left > 0` check cannot oversell.
const TAKE_UNIT =
  'UPDATE stock SET units_left = units_left - 1 WHERE id = 1 AND units_left > 0 RETURNING units_left'

type SaleRow = { units_left: number; start_at: Date; end_at: Date }
type CampaignRow = { total_units: number; start_at: Date; end_at: Date }

const MISSING = 'the stock row is missing. Run npm run db:migrate.'

/**
 * Redis decides who wins. So this class never answers a buyer. It says what
 * the database does with one record from the queue.
 *
 * Kafka's exactly-once stops at the broker, so a Postgres write is outside it.
 * `record` writes the offset with the order in one transaction, then refuses any
 * record below that offset. Kafka says where to resume. Postgres says what was
 * applied, and only Postgres is a correctness claim.
 */
export class Gate {
  private readonly pool: Pool
  private readonly cacheMs: number
  private held: Snapshot | undefined
  private heldAtMs = 0

  constructor(pool: Pool, cacheMs: number = CACHE_MS) {
    this.pool = pool
    this.cacheMs = cacheMs
  }

  /**
   * I keep the total in its own column, never `units_left`. A restart mid-sale reads a count
   * already down, and Redis needs the total it started with.
   */
  async campaign(): Promise<SaleNumbers> {
    const { rows } = await this.pool.query<CampaignRow>(READ_CAMPAIGN)
    const row = rows[0]
    if (row === undefined) throw new Error(MISSING)
    return Object.freeze({
      stock: row.total_units,
      startMs: row.start_at.getTime(),
      endMs: row.end_at.getTime(),
    })
  }

  /** Writes one win in one transaction. The first step that refuses ends it. */
  async record(win: Win): Promise<Recorded> {
    const client: PoolClient = await this.pool.connect()
    const ahead = [win.topic, win.partition, win.offset + 1]
    try {
      await client.query('BEGIN')

      const where = [win.topic, win.partition]
      await client.query(CLAIM_OFFSET, where)
      const seen = await client.query<{ next_offset: string }>(READ_OFFSET, where)
      const read = Number(seen.rows[0]?.next_offset ?? 0)

      // Below the watermark the record is already applied. Stopping here is the
      // exactly-once guard. The unique `user_id` is only the second one.
      if (read > win.offset) {
        await client.query('COMMIT')
        return 'replayed'
      }

      const taken = await client.query(TAKE_BUYER, [win.buyerId, win.seq])
      if (taken.rowCount === 0) {
        await client.query(BUMP_OFFSET, ahead)
        await client.query('COMMIT')
        return 'already-recorded'
      }

      const unit = await client.query<{ units_left: number }>(TAKE_UNIT)
      if (unit.rowCount === 0) {
        // I keep the bump on one client. A second waits on this path and starves.
        await client.query('ROLLBACK')
        await client.query(BUMP_OFFSET, ahead)
        return 'no-unit-left'
      }

      await client.query(BUMP_OFFSET, ahead)
      await client.query('COMMIT')
      this.forget()
      return 'written'
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {})
      throw error
    } finally {
      client.release()
    }
  }

  /** The count and the window in one read, reused for `cacheMs`. */
  async snapshot(nowMs: number = Date.now()): Promise<Snapshot> {
    const cached = this.cached(nowMs)
    if (cached !== undefined) return cached
    const { rows } = await this.pool.query<SaleRow>(READ_SALE)
    const fresh = toSnapshot(rows[0])
    this.remember(fresh, nowMs)
    return fresh
  }

  async stockLeft(): Promise<number> {
    return (await this.snapshot()).left
  }

  async winners(): Promise<readonly { buyerId: string; seq: number }[]> {
    const { rows } = await this.pool.query<{ user_id: string; seq: string | null }>(
      'SELECT user_id, seq FROM orders ORDER BY seq',
    )
    return rows.map((row) => ({ buyerId: row.user_id, seq: Number(row.seq ?? 0) }))
  }

  /** The highest place Postgres holds. `orders_seq_key` makes it one index read. */
  async highestSeq(): Promise<number> {
    const { rows } = await this.pool.query<{ top: string | null }>('SELECT MAX(seq) AS top FROM orders')
    return Number(rows[0]?.top ?? 0)
  }

  /** 0 where no worker read the partition. One transaction wrote it and the order row. */
  async offsetOf(topic: string, partition: number): Promise<number> {
    const { rows } = await this.pool.query<{ next_offset: string }>(
      'SELECT next_offset FROM queue_offsets WHERE topic = $1 AND partition = $2',
      [topic, partition],
    )
    return rows[0] === undefined ? 0 : Number(rows[0].next_offset)
  }

  forget(): void {
    this.held = undefined
    this.heldAtMs = 0
  }

  private cached(nowMs: number = Date.now()): Snapshot | undefined {
    if (this.held === undefined) return undefined
    return nowMs - this.heldAtMs < this.cacheMs ? this.held : undefined
  }

  private remember(sale: Snapshot, nowMs: number = Date.now()): void {
    this.held = sale
    this.heldAtMs = nowMs
  }
}

function toSnapshot(row: SaleRow | undefined): Snapshot {
  if (row === undefined) throw new Error(MISSING)
  return { left: row.units_left, startMs: row.start_at.getTime(), endMs: row.end_at.getTime() }
}
