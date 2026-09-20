import type { Pool } from 'pg'
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it } from 'vitest'
import { Gate, type Recorded, type Win } from '../src/gate/gate.ts'
import { poolFor } from './setup/db.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2026-06-02T00:00:00Z')
const TOPIC = 'sale.wins.test'

let pool: Pool
let gate: Gate
let nextOffset = 0

async function openSale(stock: number, cacheMs = 0): Promise<void> {
  await pool.query('DELETE FROM orders')
  await pool.query('DELETE FROM stock')
  await pool.query('DELETE FROM queue_offsets')
  nextOffset = 0
  gate = new Gate(pool, cacheMs)
  await gate.seed({ stock, startMs: START, endMs: END })
}

/** One record off the queue, with the offsets counted for you. */
function win(buyerId: string, seq: number, offset = nextOffset++): Win {
  return { buyerId, seq, topic: TOPIC, partition: 0, offset }
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
  it('one win writes one order row and takes one unit', async () => {
    expect(await gate.record(win('buyer-a', 1))).toBe('written')

    expect(await orderCount()).toBe(1)
    expect(await gate.stockLeft()).toBe(999)
    expect(await gate.offsetOf(TOPIC, 0)).toBe(1)
  })

  it('the same record read twice writes one row', async () => {
    const replayed = win('buyer-a', 1)
    expect(await gate.record(replayed)).toBe('written')

    expect(await gate.record(replayed)).toBe('replayed')

    expect(await orderCount()).toBe(1)
    expect(await gate.stockLeft()).toBe(999)
  })

  it('the same buyer in a second record takes no second unit', async () => {
    expect(await gate.record(win('buyer-a', 1))).toBe('written')

    expect(await gate.record(win('buyer-a', 2))).toBe('duplicate-buyer')

    expect(await orderCount()).toBe(1)
    expect(await gate.stockLeft()).toBe(999)
    // The record is consumed, so the worker never reads it again.
    expect(await gate.offsetOf(TOPIC, 0)).toBe(2)
  })

  it('a win past the stock is refused, and the offset still moves', async () => {
    await openSale(1)
    expect(await gate.record(win('buyer-a', 1))).toBe('written')

    expect(await gate.record(win('buyer-b', 2))).toBe('no-unit-left')

    expect(await orderCount()).toBe(1)
    expect(await gate.stockLeft()).toBe(0)
    expect(await gate.offsetOf(TOPIC, 0)).toBe(2)
  })

  it('the winners come back in the order Redis issued, not the order written', async () => {
    await gate.record(win('buyer-c', 3))
    await gate.record(win('buyer-a', 1))
    await gate.record(win('buyer-b', 2))

    expect(await gate.winners()).toEqual([
      { buyerId: 'buyer-a', seq: 1 },
      { buyerId: 'buyer-b', seq: 2 },
      { buyerId: 'buyer-c', seq: 3 },
    ])
  })

  it('a partition nobody read answers 0, and each one counts alone', async () => {
    expect(await gate.offsetOf(TOPIC, 3)).toBe(0)

    await gate.record({ buyerId: 'buyer-a', seq: 1, topic: TOPIC, partition: 3, offset: 40 })

    expect(await gate.offsetOf(TOPIC, 3)).toBe(41)
    expect(await gate.offsetOf(TOPIC, 0)).toBe(0)
  })

  // Four workers, each one reading its own partition in order. That is what
  // Kafka gives: order inside a partition, and no order across them.
  it('four partitions at once take exactly the stock', async () => {
    await openSale(40)
    const partitions = 4
    const each = 25

    const answers = (
      await Promise.all(
        Array.from({ length: partitions }, async (_unused, partition) => {
          const mine: Recorded[] = []
          for (let offset = 0; offset < each; offset += 1) {
            const seq = offset * partitions + partition + 1
            mine.push(await gate.record({ buyerId: `buyer-${seq}`, seq, topic: TOPIC, partition, offset }))
          }
          return mine
        }),
      )
    ).flat()

    expect(answers.filter((one) => one === 'written')).toHaveLength(40)
    expect(answers.filter((one) => one === 'no-unit-left')).toHaveLength(60)
    expect(await gate.stockLeft()).toBe(0)
    expect(await orderCount()).toBe(40)
    for (let partition = 0; partition < partitions; partition += 1) {
      expect(await gate.offsetOf(TOPIC, partition)).toBe(each)
    }
  })

  it('a restart does not give back a unit already sold', async () => {
    await gate.record(win('buyer-a', 1))
    expect(await gate.stockLeft()).toBe(999)

    const second = new Gate(pool, 0)
    await second.seed({ stock: 1000, startMs: START, endMs: END })

    expect(await second.stockLeft()).toBe(999)
  })

  it('a write drops the cached count, so the page never reads a stale number', async () => {
    await openSale(1000, 60_000)
    expect(await gate.stockLeft()).toBe(1000)

    await gate.record(win('buyer-a', 1))

    expect(await gate.stockLeft()).toBe(999)
  })
})
