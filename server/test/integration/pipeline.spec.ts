import type { Pool } from 'pg'
import { createClient } from 'redis'
import { afterAll, afterEach, beforeAll, describe, expect, inject, it } from 'vitest'
import { Gate } from '../../src/gate/gate.ts'
import {
  BUYERS_KEY,
  ISSUED_KEY,
  LIVE_KEY,
  OUTBOX_KEY,
  Pipeline,
  SOLD_KEY,
} from '../../src/queue/pipeline.ts'
import { poolFor, writeCampaign } from '../setup/db.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2036-06-01T00:00:00Z')
const DURING = START + 60_000

let pool: Pool
let open: Pipeline[] = []
let run = 0

/** One sale, on its own keys and its own topic. */
async function start(
  stock: number,
  workers = 1,
): Promise<{ gate: Gate; pipeline: Pipeline; namespace: string }> {
  await pool.query('DELETE FROM orders')
  await pool.query('DELETE FROM queue_offsets')
  await writeCampaign(pool, { stock, startMs: START, endMs: END })
  run += 1
  const namespace = `t_pipe_${run}`
  const gate = new Gate(pool, 0)
  const sale = await gate.campaign()
  const pipeline = await Pipeline.start({
    redisUrl: inject('redisUrl'),
    kafkaBrokers: [inject('kafkaBroker')],
    workers,
    sale,
    gate,
    namespace,
  })
  open.push(pipeline)
  return { gate, pipeline, namespace }
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

  it('a Redis that loses the whole sale mid-run never issues a place twice', async () => {
    const { gate, pipeline } = await start(10)
    await Promise.all(['a', 'b', 'c'].map((one) => pipeline.reserve(one, DURING)))
    expect(await pipeline.drained()).toBe(true)

    const redis = createClient({ url: inject('redisUrl') })
    await redis.connect()
    const tail = `.t_pipe_${run}`
    // Every key goes, the way a restart with no append-only file loses them.
    await redis.del([
      `sale:sold${tail}`,
      `sale:buyers${tail}`,
      `sale:outbox${tail}`,
      `sale:live${tail}`,
      `sale:issued${tail}`,
    ])

    expect(await pipeline.reserve('d', DURING)).toBe('won')
    expect(await pipeline.drained()).toBe(true)

    // Without the rebuild the counter restarts at 1, and buyer `d` holds no row.
    expect((await gate.winners()).map((one) => one.seq)).toEqual([1, 2, 3, 4])
    await redis.quit()
  })

  it('a rebuild keeps a win that Kafka never confirmed', async () => {
    const { gate, pipeline } = await start(10)
    await Promise.all(['a', 'b', 'c'].map((one) => pipeline.reserve(one, DURING)))
    expect(await pipeline.drained()).toBe(true)

    const redis = createClient({ url: inject('redisUrl') })
    await redis.connect()
    const tail = `.t_pipe_${run}`
    // What a crash between the script and the Kafka send leaves behind. `RESERVE`
    // writes both hashes in one command, so a real crash never leaves only one.
    await redis.hSet(`sale:outbox${tail}`, 'in-flight', '4')
    await redis.hSet(`sale:issued${tail}`, 'in-flight', '4')

    // The old script ran DEL on the outbox, so this rebuild threw the win away.
    await pipeline.rehydrate()

    const until = Date.now() + 15_000
    let winners = await gate.winners()
    while (winners.length < 4 && Date.now() < until) {
      await new Promise((ready) => setTimeout(ready, 100))
      winners = await gate.winners()
    }

    expect(winners.map((one) => one.buyerId)).toContain('in-flight')
    // The rebuild also counts the place the outbox holds, or place 4 is issued twice.
    expect(await pipeline.left()).toBe(6)
    await redis.quit()
  })

  it('an erased counter alone never issues a place twice', async () => {
    const { gate, pipeline } = await start(10)
    await Promise.all(['a', 'b', 'c'].map((one) => pipeline.reserve(one, DURING)))
    expect(await pipeline.drained()).toBe(true)

    const redis = createClient({ url: inject('redisUrl') })
    await redis.connect()
    const tail = `.t_pipe_${run}`
    // Only the counter goes. `sale:live` survives, the way one evicted key does.
    await redis.del(`sale:sold${tail}`)

    expect(await pipeline.reserve('d', DURING)).toBe('won')
    expect(await pipeline.drained()).toBe(true)

    // The old script guarded on `sale:live` alone, so `INCR` restarted at 1 and
    // buyer `d` took place 1, which buyer `a` already holds.
    expect((await gate.winners()).map((one) => one.seq)).toEqual([1, 2, 3, 4])
    await redis.quit()
  })

  it('a new sale window takes effect with no restart', async () => {
    const { pipeline } = await start(10)
    expect(await pipeline.reserve('early-bird', DURING)).toBe('won')

    // What `npm run sale:window` writes. The process keeps running.
    await writeCampaign(pool, { stock: 10, startMs: START - 7_200_000, endMs: START - 3_600_000 })

    // The sweep runs every 250 ms. One wait of 2 s covers it, and a single attempt
    // after the wait keeps the stock intact, so the answer names the window alone.
    await new Promise((ready) => setTimeout(ready, 2_000))
    const answer = await pipeline.reserve('after-the-change', DURING)

    // The old code copied the window once in the constructor, so it answered `won` for ever.
    expect(answer).toBe('over')
  })

  it('the sweep rebuilds the counter when one erased key puts Redis behind Postgres', async () => {
    const { pipeline } = await start(10)
    await Promise.all(['a', 'b', 'c'].map((one) => pipeline.reserve(one, DURING)))
    expect(await pipeline.drained()).toBe(true)

    const redis = createClient({ url: inject('redisUrl') })
    await redis.connect()
    const tail = `.t_pipe_${run}`
    // `sale:live` survives, so only the sweep's own check can find the loss.
    await redis.set(`sale:sold${tail}`, '0')

    const until = Date.now() + 15_000
    while (Number(await redis.get(`sale:sold${tail}`)) < 3 && Date.now() < until) {
      await new Promise((ready) => setTimeout(ready, 100))
    }

    expect(Number(await redis.get(`sale:sold${tail}`))).toBe(3)
    expect(await pipeline.left()).toBe(7)
    await redis.quit()
  })

  it('every Redis key the sale writes has no expiry, and there are only five of them', async () => {
    const { pipeline, namespace } = await start(3)
    await pipeline.reserve('a', DURING)

    const redis = createClient({ url: inject('redisUrl') })
    await redis.connect()
    try {
      const tail = `.${namespace}`
      const allowed = [SOLD_KEY, BUYERS_KEY, OUTBOX_KEY, ISSUED_KEY, LIVE_KEY].map(
        (key) => `${key}${tail}`,
      )
      const found = await redis.keys(`*${tail}`)
      expect(found.length).toBeGreaterThan(0)
      expect(found.filter((key) => !allowed.includes(key))).toEqual([])

      // -1 means the key exists and never expires. Redis holds the only copy of a
      // place that Postgres has not recorded yet, so an expiry there loses a win.
      for (const key of found) expect(await redis.ttl(key)).toBe(-1)
    } finally {
      await redis.quit()
    }
  })

  it('the sale drops every Redis key once the window closes, and Postgres answers from then on', async () => {
    const { pipeline, namespace } = await start(10)
    await Promise.all(['a', 'b', 'c'].map((one) => pipeline.reserve(one, DURING)))
    expect(await pipeline.drained()).toBe(true)

    // The sweep reads the window from Postgres every 250 ms, so an end in the past
    // closes the sale with no restart. UPDATE keeps the 7 units the sale has left.
    await pool.query('UPDATE stock SET end_at = $1 WHERE id = 1', [new Date(DURING)])

    const redis = createClient({ url: inject('redisUrl') })
    await redis.connect()
    try {
      const tail = `.${namespace}`
      let found = await redis.keys(`*${tail}`)
      for (let tries = 0; tries < 40 && found.length > 0; tries += 1) {
        await new Promise((ready) => setTimeout(ready, 100))
        found = await redis.keys(`*${tail}`)
      }
      expect(found).toEqual([])

      // The counter is gone, so a wrong read path would answer 10 here.
      expect(await pipeline.left()).toBe(7)
      expect(await pipeline.reserve('late', Date.now())).toBe('over')

      // A retired sale must not rebuild. Without the guard the sweep reads every
      // order row and writes all five keys back, 4 times a second, for good.
      const before = pipeline.counts.rebuilds
      await new Promise((ready) => setTimeout(ready, 800))
      expect(pipeline.counts.rebuilds).toBe(before)
      expect(await redis.keys(`*${tail}`)).toEqual([])
    } finally {
      await redis.quit()
    }
  })
})
