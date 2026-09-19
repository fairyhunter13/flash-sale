import type { Pool, PoolClient } from 'pg'
import { saleState } from './status.ts'

export type Outcome = 'won' | 'already-bought' | 'sold-out' | 'not-open' | 'over'

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

/**
 * How long a read of the sale is reused. It matches the SSE tick, so an open
 * page never sees a number older than one tick.
 */
export const CACHE_MS = 250

const READ_SALE = 'SELECT units_left, start_at, end_at FROM stock WHERE id = 1'

// ON CONFLICT DO NOTHING, and never a caught 23505. A duplicate then costs no
// error path, and the returned row count is the whole answer.
const TAKE_BUYER =
  'INSERT INTO orders (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING RETURNING user_id'

// `units_left > 0` is what stops the oversell. Postgres locks the one stock row
// for the length of the transaction, so a second buyer reads the count only
// after the first buyer commits or rolls back.
const TAKE_UNIT =
  'UPDATE stock SET units_left = units_left - 1 WHERE id = 1 AND units_left > 0 RETURNING units_left'

type SaleRow = { units_left: number; start_at: Date; end_at: Date }

const MISSING = 'the stock row is missing. The server did not seed the sale.'

/**
 * The whole decision, in one Postgres transaction. There is no second store to
 * keep in step, so no buyer holds a unit that no table records, and no unit
 * leaves the count with no buyer.
 *
 * In front of the transaction sits one cached read, described on `reserve`.
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
   * Writes the sale only when it is absent. A restart in the middle of a live
   * sale then keeps the real count, so units already sold stay sold.
   */
  async seed(sale: SaleNumbers): Promise<void> {
    await this.pool.query(
      `INSERT INTO stock (id, units_left, start_at, end_at)
       VALUES (1, $1, $2, $3)
       ON CONFLICT (id) DO NOTHING`,
      [sale.stock, new Date(sale.startMs), new Date(sale.endMs)],
    )
    // The row that is now there, which is not always the row above. A second
    // server, or a restart, reads the sale that is already running.
    this.forget()
    await this.snapshot()
  }

  /**
   * Answers one buyer.
   *
   * **The fast path.** A sale that has not opened, a sale that has closed and a
   * sale with no unit left are all answerable from the cached read, because the
   * window never moves and the count never goes up. Those buyers never reach
   * the database. In a sale of 1,000 units and 1,000,000 buyers, that is
   * 999,000 of them.
   *
   * **The slow path.** Everybody else opens one transaction and runs 3 steps in
   * order. The first step that fails ends the transaction with no write at all.
   *
   * 1. Read the sale again, this time inside the transaction. The cached read
   *    is never trusted for a `won`.
   * 2. Write the order row. A row that is already there means the buyer holds a
   *    unit, so the answer is `already-bought`.
   * 3. Take the unit. This step decides, because it holds the row lock. 0 rows
   *    means the last unit went while this buyer waited.
   *
   * The clock arrives as an argument, so a test needs no fake timer.
   */
  async reserve(buyerId: string, nowMs: number = Date.now()): Promise<Outcome> {
    const cached = this.cached()
    if (cached !== undefined) {
      const early = refusal(nowMs, cached)
      if (early !== undefined) return early
    }

    const client: PoolClient = await this.pool.connect()
    try {
      await client.query('BEGIN')

      const sale = await client.query<SaleRow>(READ_SALE)
      const fresh = toSnapshot(sale.rows[0])
      const late = refusal(nowMs, fresh)
      if (late !== undefined) {
        await client.query('ROLLBACK')
        this.remember(fresh)
        return late
      }

      const taken = await client.query(TAKE_BUYER, [buyerId])
      if (taken.rowCount === 0) {
        await client.query('ROLLBACK')
        return 'already-bought'
      }

      const unit = await client.query<{ units_left: number }>(TAKE_UNIT)
      if (unit.rowCount === 0) {
        await client.query('ROLLBACK')
        return 'sold-out'
      }

      await client.query('COMMIT')
      // The count the winner left behind, so the next page sees it at once.
      this.remember({ ...fresh, left: unit.rows[0]!.units_left })
      return 'won'
    } catch (error) {
      // A rollback undoes both writes together, so no buyer is recorded without
      // a unit, and no unit is lost without a buyer.
      await client.query('ROLLBACK').catch(() => {})
      throw error
    } finally {
      client.release()
    }
  }

  /**
   * The count and the window in one read, reused for `cacheMs`. The window
   * comes from the table and never from the environment, so the answer names
   * the same window that `reserve` enforces.
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

/**
 * The answer for a buyer who cannot win, or undefined where the buyer may. It
 * reads the same `saleState` the API reports, so the page and the gate can
 * never disagree.
 */
function refusal(nowMs: number, sale: Snapshot): Outcome | undefined {
  const state = saleState(nowMs, sale.left, { startMs: sale.startMs, endMs: sale.endMs })
  if (state === 'pending') return 'not-open'
  if (state === 'closed') return 'over'
  if (state === 'sold-out') return 'sold-out'
  return undefined
}

function toSnapshot(row: SaleRow | undefined): Snapshot {
  if (row === undefined) throw new Error(MISSING)
  return { left: row.units_left, startMs: row.start_at.getTime(), endMs: row.end_at.getTime() }
}
