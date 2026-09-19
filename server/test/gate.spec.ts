import type { Pool } from 'pg'
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it, vi } from 'vitest'
import { Gate } from '../src/gate/gate.ts'
import { poolFor } from './setup/db.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2026-06-02T00:00:00Z')
const DURING = START + 60_000

let pool: Pool
let gate: Gate

async function openSale(stock: number, cacheMs = 0): Promise<void> {
  await pool.query('DELETE FROM orders')
  await pool.query('DELETE FROM stock')
  gate = new Gate(pool, cacheMs)
  await gate.seed({ stock, startMs: START, endMs: END })
}

async function orderCount(): Promise<number> {
  const { rows } = await pool.query<{ n: number }>('SELECT count(*)::int AS n FROM orders')
  return rows[0]!.n
}

beforeAll(async () => {
  pool = await poolFor(inject('databaseUrl'), 't_gate')
})

afterAll(async () => {
  await pool.end()
})

beforeEach(async () => {
  await openSale(1000)
})

describe('the gate', () => {
  it('two buyers race for one unit', async () => {
    await openSale(1)

    const outcomes = (
      await Promise.all([gate.reserve('buyer-a', DURING), gate.reserve('buyer-b', DURING)])
    ).sort()

    expect(outcomes).toEqual(['sold-out', 'won'])
    expect(await gate.stockLeft()).toBe(0)
    expect(await orderCount()).toBe(1)
  })

  it('a repeat buyer is refused as already-bought', async () => {
    expect(await gate.reserve('buyer-a', DURING)).toBe('won')

    const again = await gate.reserve('buyer-a', DURING)

    expect(again).toBe('already-bought')
    expect(await gate.stockLeft()).toBe(999)
    expect(await orderCount()).toBe(1)
  })

  it('a purchase before the start is not-open', async () => {
    expect(await gate.reserve('buyer-a', START - 1)).toBe('not-open')
    expect(await gate.stockLeft()).toBe(1000)
    expect(await orderCount()).toBe(0)
  })

  it('a purchase after the end is over', async () => {
    expect(await gate.reserve('buyer-a', END + 1)).toBe('over')
    expect(await gate.stockLeft()).toBe(1000)
    expect(await orderCount()).toBe(0)
  })

  it('a thousand parallel calls take exactly the stock', async () => {
    await openSale(100)

    const answers = await Promise.all(
      Array.from({ length: 1000 }, (_unused, index) => gate.reserve(`buyer-${index}`, DURING)),
    )

    expect(answers.filter((one) => one === 'won')).toHaveLength(100)
    expect(answers.filter((one) => one === 'sold-out')).toHaveLength(900)
    expect(await gate.stockLeft()).toBe(0)
    // The 900 who lost hold nothing. A rolled-back transaction writes no order.
    expect(await orderCount()).toBe(100)
  })

  it('a restart does not give back a unit already sold', async () => {
    await gate.reserve('buyer-a', DURING)
    expect(await gate.stockLeft()).toBe(999)

    const second = new Gate(pool, 0)
    await second.seed({ stock: 1000, startMs: START, endMs: END })

    expect(await second.stockLeft()).toBe(999)
  })

  // The fast path is what keeps a traffic spike out of Postgres, so it is
  // measured by the one thing that runs out: a pooled connection.
  it('a sold-out buyer takes no database connection at all', async () => {
    await openSale(1, 60_000)
    expect(await gate.reserve('buyer-a', DURING)).toBe('won')

    // The count is read before the restore, because mockRestore also forgets
    // every call the spy saw.
    const taken = vi.spyOn(pool, 'connect')
    expect(await gate.reserve('buyer-b', DURING)).toBe('sold-out')
    const opened = taken.mock.calls.length
    taken.mockRestore()

    expect(opened).toBe(0)
  })

  it('the same buyer takes a connection when the cache is off', async () => {
    await openSale(1, 0)
    expect(await gate.reserve('buyer-a', DURING)).toBe('won')

    const taken = vi.spyOn(pool, 'connect')
    expect(await gate.reserve('buyer-b', DURING)).toBe('sold-out')
    const opened = taken.mock.calls.length
    taken.mockRestore()

    expect(opened).toBe(1)
  })
})
