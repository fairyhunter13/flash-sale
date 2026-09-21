import type { Pool } from 'pg'
import { createClient } from 'redis'
import { afterAll, afterEach, beforeAll, describe, expect, inject, it } from 'vitest'
import { Gate } from '../../src/gate/gate.ts'
import { Pipeline } from '../../src/queue/pipeline.ts'
import { poolFor, writeCampaign } from '../setup/db.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2036-06-01T00:00:00Z')
const DURING = START + 60_000

let pool: Pool
let open: Pipeline[] = []
let run = 0

/** One sale, on its own keys and its own topic. */
async function start(stock: number, workers = 1): Promise<{ gate: Gate; pipeline: Pipeline }> {
  await pool.query('DELETE FROM orders')
  await pool.query('DELETE FROM queue_offsets')
  await writeCampaign(pool, { stock, startMs: START, endMs: END })
  run += 1
  const gate = new Gate(pool, 0)
  const sale = await gate.campaign()
  const pipeline = await Pipeline.start({
    redisUrl: inject('redisUrl'),
    kafkaBrokers: [inject('kafkaBroker')],
    workers,
    sale,
    gate,
    namespace: `t_pipe_${run}`,
  })
  open.push(pipeline)
  return { gate, pipeline }
}

beforeAll(async () => {
  pool = await poolFor(inject('databaseUrl'), 't_pipeline')
})

afterEach(async () => {
  for (const pipeline of open) await pipeline.close()
  open = []
})

afterAll(async () => {
  await pool.end()
})

describe('the pipeline', () => {
  it('Redis answers the buyer, and the database holds the same winners', async () => {
    const { gate, pipeline } = await start(3)

    const answers = await Promise.all(
      ['a', 'b', 'c', 'd', 'e'].map((one) => pipeline.reserve(one, DURING)),
    )

    expect(answers.filter((one) => one === 'won')).toHaveLength(3)
    expect(answers.filter((one) => one === 'sold-out')).toHaveLength(2)
    expect(await pipeline.left()).toBe(0)

    expect(await pipeline.drained()).toBe(true)
    const winners = await gate.winners()
    expect(winners).toHaveLength(3)
    expect(winners.map((one) => one.seq)).toEqual([1, 2, 3])
    expect(await gate.stockLeft()).toBe(0)
  })

  it('a buyer who lost is told sold-out again, and holds no place in the set', async () => {
    const { pipeline } = await start(1)
    expect(await pipeline.reserve('winner', DURING)).toBe('won')

    const lost = await pipeline.reserve('loser', DURING)
    const again = await pipeline.reserve('loser', DURING)

    expect(lost).toBe('sold-out')
    // Never already-bought. That buyer holds no order row, and the answer would
    // name a purchase that does not exist.
    expect(again).toBe('sold-out')
  })

  it('the set holds the stock, whatever the traffic is', async () => {
    const { pipeline } = await start(5)

    await Promise.all(
      Array.from({ length: 400 }, (_unused, index) => pipeline.reserve(`buyer-${index}`, DURING)),
    )

    // 400 buyers arrived. 5 of them hold a unit, so 5 members stay.
    expect(await pipeline.buyersHeld()).toBe(5)
    expect(await pipeline.left()).toBe(0)
  })

  it('a winner who asks again is told already-bought', async () => {
    const { pipeline } = await start(10)
    expect(await pipeline.reserve('buyer-a', DURING)).toBe('won')

    expect(await pipeline.reserve('buyer-a', DURING)).toBe('already-bought')
    expect(await pipeline.left()).toBe(9)
  })

  it('one buyer who asks 50 times at once wins once, with no transaction anywhere', async () => {
    const { gate, pipeline } = await start(10)

    const answers = await Promise.all(
      Array.from({ length: 50 }, () => pipeline.reserve('buyer-a', DURING)),
    )

    // SADD returns 1 to one caller and 0 to the other 49, so only one call
    // ever reaches INCR. I skip MULTI/EXEC.
    expect(answers.filter((one) => one === 'won')).toHaveLength(1)
    expect(answers.filter((one) => one === 'already-bought')).toHaveLength(49)
    expect(await pipeline.left()).toBe(9)
    expect(await pipeline.buyersHeld()).toBe(1)
    expect(await pipeline.drained()).toBe(true)
    expect(await gate.winners()).toHaveLength(1)
  })

  it('the window is answered before any store is read', async () => {
    const { pipeline } = await start(10)

    expect(await pipeline.reserve('early', START - 1)).toBe('not-open')
    expect(await pipeline.reserve('late', END + 1)).toBe('over')
    expect(await pipeline.left()).toBe(10)
  })

  it('a lost Redis is rebuilt from the order rows, and the next place is right', async () => {
    const { gate, pipeline } = await start(10)
    await Promise.all(['a', 'b', 'c'].map((one) => pipeline.reserve(one, DURING)))
    expect(await pipeline.drained()).toBe(true)

    const rebuilt = await pipeline.rehydrate()

    expect(rebuilt.buyers).toBe(3)
    // The count comes from max(seq). A row count would hand the next buyer a
    // place someone already holds.
    expect(rebuilt.highestSeq).toBe(3)
    expect(await pipeline.left()).toBe(7)
    expect(await pipeline.reserve('d', DURING)).toBe('won')
    expect(await pipeline.drained()).toBe(true)
    expect((await gate.winners()).map((one) => one.seq)).toEqual([1, 2, 3, 4])
  })

  it('four workers write every win once, and the order still reads right', async () => {
    const { gate, pipeline } = await start(50, 4)

    const answers = await Promise.all(
      Array.from({ length: 200 }, (_unused, index) => pipeline.reserve(`buyer-${index}`, DURING)),
    )

    expect(answers.filter((one) => one === 'won')).toHaveLength(50)
    expect(await pipeline.drained()).toBe(true)

    const winners = await gate.winners()
    expect(winners).toHaveLength(50)
    expect(winners.map((one) => one.seq)).toEqual(Array.from({ length: 50 }, (_u, i) => i + 1))
    expect(await gate.stockLeft()).toBe(0)
  })

  // A crash between the script and the Kafka send leaves the win here. Only the
  // sweep finds it, so a sold unit with no order row is what a failure looks like.
  it('a win left in the outbox reaches the database after one sweep', async () => {
    const { gate } = await start(3)
    const redis = createClient({ url: inject('redisUrl') })
    await redis.connect()
    const tail = `.t_pipe_${run}`

    await redis.sAdd(`sale:buyers${tail}`, 'stranded')
    await redis.set(`sale:sold${tail}`, '1')
    await redis.hSet(`sale:outbox${tail}`, 'stranded', '1')

    const until = Date.now() + 15_000
    let winners = await gate.winners()
    while (winners.length === 0 && Date.now() < until) {
      await new Promise((ready) => setTimeout(ready, 100))
      winners = await gate.winners()
    }

    expect(winners.map((one) => one.buyerId)).toEqual(['stranded'])
    expect(await gate.stockLeft()).toBe(2)
    expect(await redis.hLen(`sale:outbox${tail}`)).toBe(0)
    await redis.quit()
  })

  it('no buyer is told already-bought for a unit they never won', async () => {
    const { gate, pipeline } = await start(1)
    // 3 buyers, 8 calls each. Two of the 3 must lose at the counter.
    const buyers = Array.from({ length: 24 }, (_unused, index) => `buyer-${index % 3}`)

    const answers = await Promise.all(buyers.map((one) => pipeline.reserve(one, DURING)))
    expect(await pipeline.drained()).toBe(true)

    const winners = new Set((await gate.winners()).map((one) => one.buyerId))
    const claimed = new Set(buyers.filter((_unused, index) => answers[index] === 'already-bought'))
    expect([...claimed].filter((one) => !winners.has(one))).toEqual([])
  })

  it('a rebuild never lowers the counter', async () => {
    const { pipeline } = await start(5)
    await Promise.all(['a', 'b', 'c'].map((one) => pipeline.reserve(one, DURING)))
    expect(await pipeline.drained()).toBe(true)
    // The third order row disappears, so Postgres now reads behind Redis.
    await pool.query('DELETE FROM orders WHERE seq = 3')

    const rebuilt = await pipeline.rehydrate()

    expect(rebuilt.highestSeq).toBe(2)
    // 3 units are gone. A rebuild that wrote 2 would sell one place twice.
    expect(await pipeline.left()).toBe(2)
  })
})
