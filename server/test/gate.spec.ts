import { Redis } from 'ioredis'
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it } from 'vitest'
import { Gate, KEY } from '../src/gate/gate.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2026-06-02T00:00:00Z')
const DURING = START + 60_000

let redis: Redis
let gate: Gate

async function openSale(stock: number) {
  await redis.flushdb()
  gate = new Gate(redis)
  await gate.seed({ stock, startMs: START, endMs: END })
}

beforeAll(() => {
  redis = new Redis(inject('redisUrl'), { db: 1 })
})

afterAll(async () => {
  await redis.quit()
})

beforeEach(async () => {
  await openSale(1000)
})

describe('the gate', () => {
  it('two buyers race for one unit', async () => {
    await openSale(1)

    const [first, second] = await Promise.all([
      gate.reserve('buyer-a', DURING),
      gate.reserve('buyer-b', DURING),
    ])

    const outcomes = [first.outcome, second.outcome].sort()
    expect(outcomes).toEqual(['sold-out', 'won'])
    expect(await gate.stockLeft()).toBe(0)
    expect(await redis.xlen(KEY.wins)).toBe(1)
  })

  it('a repeat buyer is refused as already-bought', async () => {
    expect((await gate.reserve('buyer-a', DURING)).outcome).toBe('won')

    const again = await gate.reserve('buyer-a', DURING)

    expect(again.outcome).toBe('already-bought')
    expect(again.outcome).not.toBe('sold-out')
    expect(await gate.stockLeft()).toBe(999)
    expect(await redis.xlen(KEY.wins)).toBe(1)
  })

  it('a purchase before the start is not-open', async () => {
    const answer = await gate.reserve('buyer-a', START - 1)

    expect(answer.outcome).toBe('not-open')
    expect(await gate.stockLeft()).toBe(1000)
    expect(await redis.scard(KEY.buyers)).toBe(0)
  })

  it('a purchase after the end is over', async () => {
    const answer = await gate.reserve('buyer-a', END + 1)

    expect(answer.outcome).toBe('over')
    expect(await gate.stockLeft()).toBe(1000)
  })

  it('a thousand parallel calls take exactly the stock', async () => {
    await openSale(100)

    const answers = await Promise.all(
      Array.from({ length: 1000 }, (_unused, index) => gate.reserve(`buyer-${index}`, DURING)),
    )

    const won = answers.filter((a) => a.outcome === 'won').length
    const soldOut = answers.filter((a) => a.outcome === 'sold-out').length

    expect(won).toBe(100)
    expect(soldOut).toBe(900)
    expect(await gate.stockLeft()).toBe(0)
    expect(await redis.xlen(KEY.wins)).toBe(100)
    expect(await redis.scard(KEY.buyers)).toBe(100)
  })

  it('a restart does not give back a unit already sold', async () => {
    await gate.reserve('buyer-a', DURING)
    expect(await gate.stockLeft()).toBe(999)

    await new Gate(redis).seed({ stock: 1000, startMs: START, endMs: END })

    expect(await gate.stockLeft()).toBe(999)
  })
})
