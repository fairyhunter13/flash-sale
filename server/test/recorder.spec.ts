import { Redis } from 'ioredis'
import type { Pool } from 'pg'
import { poolFor } from './setup/db.ts'
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it } from 'vitest'
import { Gate, KEY } from '../src/gate/gate.ts'
import { GROUP, Recorder } from '../src/recorder/consumer.ts'
import { insertOrders } from '../src/recorder/db.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2036-06-01T00:00:00Z')
const DURING = START + 60_000

let redis: Redis
let pool: Pool
let gate: Gate

beforeAll(async () => {
  redis = new Redis(inject('redisUrl'), { db: 4 })
  pool = await poolFor(inject('databaseUrl'), 't_recorder')
})

afterAll(async () => {
  await Promise.all([redis.quit(), pool.end()])
})

beforeEach(async () => {
  await redis.flushdb()
  await pool.query('DELETE FROM orders')
  gate = new Gate(redis)
  await gate.seed({ stock: 1000, startMs: START, endMs: END })
})

function recorder(name: string) {
  return new Recorder(redis, pool, { consumerName: name, blockMs: 50, claimAfterMs: 0 })
}

async function countOrders(): Promise<number> {
  const { rows } = await pool.query<{ count: string }>('SELECT count(*) FROM orders')
  return Number(rows[0]?.count)
}

describe('the recorder', () => {
  it('a win becomes one order row', async () => {
    await gate.reserve('buyer-a', DURING)
    const it1 = recorder('one')
    await it1.ensureGroup()

    expect(await it1.drainOnce()).toBe(1)

    const { rows } = await pool.query<{ user_id: string }>('SELECT user_id FROM orders')
    expect(rows.map((r) => r.user_id)).toEqual(['buyer-a'])
    // The entry is acknowledged, so the pending list is empty.
    expect(await redis.xpending(KEY.wins, GROUP)).toMatchObject([0, null, null, null])
  })

  it('a restart drains the pending entries', async () => {
    for (let n = 0; n < 50; n += 1) await gate.reserve(`buyer-${n}`, DURING)

    // A reader that dies after XREADGROUP and before the insert leaves all 50
    // entries pending. The second reader must take every one of them.
    const dead = recorder('dead')
    await dead.ensureGroup()
    await redis.xreadgroup(
      'GROUP', GROUP, 'dead', 'COUNT', 50, 'STREAMS', KEY.wins, '>',
    )
    expect(await countOrders()).toBe(0)

    const alive = recorder('alive')
    expect(await alive.drainOnce()).toBe(50)

    expect(await countOrders()).toBe(50)
    expect(await redis.xpending(KEY.wins, GROUP)).toMatchObject([0, null, null, null])
  })

  it('the unique index refuses a duplicate', async () => {
    await insertOrders(pool, ['buyer-a'])

    // The same buyer arrives again. The insert names the conflict, so the
    // batch commits and the table keeps one row.
    expect(await insertOrders(pool, ['buyer-a', 'buyer-b'])).toBe(1)

    expect(await countOrders()).toBe(2)
  })

  it('a replayed batch writes nothing new', async () => {
    await gate.reserve('buyer-a', DURING)
    const it1 = recorder('one')
    await it1.ensureGroup()
    await it1.drainOnce()

    // The whole batch is replayed by hand, exactly as a crash before the XACK
    // would replay it.
    expect(await insertOrders(pool, ['buyer-a'])).toBe(0)
    expect(await countOrders()).toBe(1)
  })

  it('the running loop drains a win that arrives later', async () => {
    const it1 = recorder('one')
    await it1.ensureGroup()
    it1.start()

    await gate.reserve('buyer-late', DURING)
    await waitFor(async () => (await countOrders()) === 1)
    await it1.stop()

    expect(await countOrders()).toBe(1)
  })
})

async function waitFor(ready: () => Promise<boolean>, limitMs = 5000): Promise<void> {
  const until = Date.now() + limitMs
  while (!(await ready())) {
    if (Date.now() > until) throw new Error('the condition did not hold in time')
    await new Promise((done) => setTimeout(done, 25))
  }
}
