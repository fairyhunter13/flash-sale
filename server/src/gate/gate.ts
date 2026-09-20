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

/** One win, as it arrives from the queue. */
export type Win = {
  readonly buyerId: string
  /** The buyer's place in the queue, issued by Redis. */
  readonly seq: number
  readonly topic: string
  readonly partition: number
  readonly offset: number
}

/** What the transaction did with one record. */
export type Recorded = 'written' | 'replayed' | 'duplicate-buyer' | 'no-unit-left'

/**
 * How long a read of the sale is reused. It matches the SSE tick, so an open
 * page never sees a number older than one tick.
 */
export const CACHE_MS = 250

const READ_SALE = 'SELECT units_left, start_at, end_at FROM stock WHERE id = 1'

const READ_CAMPAIGN = 'SELECT total_units, start_at, end_at FROM stock WHERE id = 1'

// The row is created before it is locked, so every transaction takes the same
// lock in the same order. Without the insert, a transaction that found no row
// held no lock, took the stock row, and then waited for the offset row a
// second transaction held while that one waited for the stock row. Postgres
// reported `deadlock detected`, measured at 100 parallel records.
const CLAIM_OFFSET = `INSERT INTO queue_offsets (topic, partition, next_offset) VALUES ($1, $2, 0)
   ON CONFLICT (topic, partition) DO NOTHING`

const READ_OFFSET =
  'SELECT next_offset FROM queue_offsets WHERE topic = $1 AND partition = $2 FOR UPDATE'

// GREATEST, and never a plain assignment. During a group rebalance two
// workers can hold one partition for a moment, and the later record must not
// pull the resume point backwards.
//
// A strict `+ 1` was measured instead, and it stalls: a resume point of 0
// against a first record at offset 40 never moves, which a cleared table and a
// kept topic produce. So the point is a high-water mark, and the unique
// `user_id` stays the only guard against a repeat.
const BUMP_OFFSET = `INSERT INTO queue_offsets (topic, partition, next_offset) VALUES ($1, $2, $3)
   ON CONFLICT (topic, partition)
   DO UPDATE SET next_offset = GREATEST(queue_offsets.next_offset, EXCLUDED.next_offset)`

// ON CONFLICT DO NOTHING, and never a caught 23505. A duplicate then costs no
// error path, and the returned row count is the whole answer.
const TAKE_BUYER =
  'INSERT INTO orders (user_id, seq) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING RETURNING user_id'

// `units_left > 0` is what stops the oversell. Postgres locks the one stock row
// for the length of the transaction, so a second worker reads the count only
// after the first one commits or rolls back.
const TAKE_UNIT =
  'UPDATE stock SET units_left = units_left - 1 WHERE id = 1 AND units_left > 0 RETURNING units_left'

type SaleRow = { units_left: number; start_at: Date; end_at: Date }
type CampaignRow = { total_units: number; start_at: Date; end_at: Date }

const MISSING = 'the stock row is missing. Run npm run db:migrate.'

/**
 * The permanent state, and the only writer of it.
 *
 * Redis decides who wins and Kafka carries the win here, so this class never
 * answers a buyer. It answers one question instead: what does the database do
 * with one record from the queue.
 *
 * **Kafka's exactly-once stops at the broker.** A write to Postgres is an
 * external side effect outside it, so the offset cannot live in the broker.
 * `record` writes the order row, the unit and the offset in **one**
 * transaction. A record whose transaction rolled back keeps its old offset, so
 * a worker reads it again.
 *
 * **The buyer is the key, and the offset is only the resume point.** A record
 * read twice is refused by the unique user_id, and never by the offset alone.
 * During a group rebalance two workers hold one partition for a moment, so an
 * offset already past a record does not prove that record was written.
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
   * The campaign the migrations wrote: the total the sale started with, and
   * the window.
   *
   * The total is a column, and never `units_left`. A restart in the middle of
   * a live sale reads a count that is already down, and Redis needs the total
   * to know how many units it may still hand out.
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

  /**
   * Writes one win from the queue, in one transaction, and reports what it did.
   *
   * The steps run in order, and the first one that refuses ends the
   * transaction.
   *
   * 1. Create the offset row for this partition where it is absent, then lock
   *    it. Every path takes that lock first, so two workers on one partition
   *    run one after the other and never deadlock.
   * 2. Write the order row. A row already there means the buyer is recorded,
   *    so the record is consumed and no second unit leaves the count. A record
   *    the stored offset already passed is reported as a replay, and a record
   *    it has not is a second win for a buyer who already holds one.
   * 3. Take the unit. 0 rows means the database holds fewer units than the
   *    queue holds wins, which is the oversell this step refuses.
   */
  async record(win: Win): Promise<Recorded> {
    const client: PoolClient = await this.pool.connect()
    const ahead = [win.topic, win.partition, win.offset + 1]
    try {
      await client.query('BEGIN')

      const where = [win.topic, win.partition]
      await client.query(CLAIM_OFFSET, where)
      const seen = await client.query<{ next_offset: string }>(READ_OFFSET, where)
      const read = Number(seen.rows[0]?.next_offset ?? 0)

      const taken = await client.query(TAKE_BUYER, [win.buyerId, win.seq])
      if (taken.rowCount === 0) {
        await client.query(BUMP_OFFSET, ahead)
        await client.query('COMMIT')
        return read > win.offset ? 'replayed' : 'duplicate-buyer'
      }

      const unit = await client.query<{ units_left: number }>(TAKE_UNIT)
      if (unit.rowCount === 0) {
        // The offset still moves, because reading this record again would
        // refuse it again. The order row rolls back with the transaction.
        //
        // The bump runs on the same client, and never on a second one from the
        // pool. A second one starves: every client is held by a record on this
        // path, and each one then waits for a client that never comes free.
        await client.query('ROLLBACK')
        await client.query(BUMP_OFFSET, ahead)
        return 'no-unit-left'
      }

      await client.query(BUMP_OFFSET, ahead)
      await client.query('COMMIT')
      this.forget()
      return 'written'
    } catch (error) {
      // A rollback undoes the row, the unit and the offset together, so the
      // record is read again and nothing is half written.
      await client.query('ROLLBACK').catch(() => {})
      throw error
    } finally {
      client.release()
    }
  }

  /**
   * The count and the window in one read, reused for `cacheMs`. The window
   * comes from the table and never from the environment, so the answer names
   * the same window the sale runs in.
   */
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

  /** Every buyer the database records, and the place each one holds. */
  async winners(): Promise<readonly { buyerId: string; seq: number }[]> {
    const { rows } = await this.pool.query<{ user_id: string; seq: string | null }>(
      'SELECT user_id, seq FROM orders ORDER BY seq',
    )
    return rows.map((row) => ({ buyerId: row.user_id, seq: Number(row.seq ?? 0) }))
  }

  /**
   * How far the workers read one partition, and 0 where none read it.
   * A new owner of the partition seeks here, because this row and the order
   * row were written by the same transaction.
   */
  async offsetOf(topic: string, partition: number): Promise<number> {
    const { rows } = await this.pool.query<{ next_offset: string }>(
      'SELECT next_offset FROM queue_offsets WHERE topic = $1 AND partition = $2',
      [topic, partition],
    )
    return rows[0] === undefined ? 0 : Number(rows[0].next_offset)
  }

  /** Drops the cached read. The next call reads the table. */
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
