import Fastify from 'fastify'
import { Pool } from 'pg'
import { poolFor } from './setup/db.ts'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, inject, it } from 'vitest'
import type { Config } from '../src/config.ts'
import { Gate } from '../src/gate/gate.ts'
import type { Pipeline } from '../src/queue/pipeline.ts'
import { registerRoutes } from '../src/routes/index.ts'
import { buildApp, type App } from '../src/server.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2036-06-01T00:00:00Z')

function config(stock: number): Config {
  return Object.freeze({
    stock,
    startMs: START,
    endMs: END,
    databaseUrl: 'postgres://unused/unused',
    dbPoolMax: 4,
    redisUrl: inject('redisUrl'),
    kafkaBrokers: [inject('kafkaBroker')],
    queueWorkers: 1,
    port: 0,
    host: '127.0.0.1',
  })
}

/**
 * A store that answers nothing. The routes must report a fault, and never a
 * sold-out sale.
 */
const deadPipeline = {
  left: async () => {
    throw new Error('the hot state is down')
  },
  reserve: async () => {
    throw new Error('the hot state is down')
  },
} as unknown as Pipeline

let pool: Pool
let app: App

beforeAll(async () => {
  pool = await poolFor(inject('databaseUrl'), 't_routes')
})

afterAll(async () => {
  await pool.end()
})

let run = 0

beforeEach(async () => {
  await pool.query('DELETE FROM orders')
  await pool.query('DELETE FROM stock')
  await pool.query('DELETE FROM queue_offsets')
  // Its own keys and its own topic, so each test starts on an empty sale.
  run += 1
  app = await buildApp(config(1000), pool, undefined, `t_routes_${run}`)
})

afterEach(async () => {
  await app.fastify.close()
})

describe('the routes', () => {
  it('GET /api/sale answers the state', async () => {
    const answer = await app.fastify.inject({ method: 'GET', url: '/api/sale' })

    expect(answer.statusCode).toBe(200)
    expect(answer.json()).toEqual({
      state: 'open',
      stockLeft: 1000,
      startsAt: new Date(START).toISOString(),
      endsAt: new Date(END).toISOString(),
    })
  })

  it('POST /api/purchase answers one outcome', async () => {
    const won = await app.fastify.inject({
      method: 'POST',
      url: '/api/purchase',
      payload: { userId: 'buyer-a' },
    })
    expect(won.statusCode).toBe(200)
    expect(won.json()).toEqual({ outcome: 'won' })

    const again = await app.fastify.inject({
      method: 'POST',
      url: '/api/purchase',
      payload: { userId: 'buyer-a' },
    })
    expect(again.json()).toEqual({ outcome: 'already-bought' })

    const sale = await app.fastify.inject({ method: 'GET', url: '/api/sale' })
    expect(sale.json().stockLeft).toBe(999)
  })

  it('an empty userId is refused before the database is touched', async () => {
    const answer = await app.fastify.inject({
      method: 'POST',
      url: '/api/purchase',
      payload: { userId: '   ' },
    })

    expect(answer.statusCode).toBe(400)
    expect(answer.json()).not.toHaveProperty('outcome')
    expect(await app.gate.stockLeft()).toBe(1000)
  })

  it('a purchase that wins reaches the database through the queue', async () => {
    const won = await app.fastify.inject({
      method: 'POST',
      url: '/api/purchase',
      payload: { userId: 'buyer-z' },
    })
    expect(won.json()).toEqual({ outcome: 'won' })

    expect(await app.pipeline.drained()).toBe(true)

    expect(await app.gate.winners()).toEqual([{ buyerId: 'buyer-z', seq: 1 }])
    expect(await app.gate.stockLeft()).toBe(999)
  })

  it('a dead database gives 500 and no outcome', async () => {
    // Port 1 answers nothing, so the routes hold a real Gate over a pool that
    // cannot reach a server.
    const dead = new Pool({
      connectionString: 'postgres://flash:flash@127.0.0.1:1/flash',
      connectionTimeoutMillis: 250,
      max: 1,
    })
    dead.on('error', () => {})
    const broken = Fastify({ logger: false })
    registerRoutes(broken, new Gate(dead, 0), deadPipeline)

    const sale = await broken.inject({ method: 'GET', url: '/api/sale' })
    expect(sale.statusCode).toBe(500)
    expect(sale.json()).not.toHaveProperty('state')

    const purchase = await broken.inject({
      method: 'POST',
      url: '/api/purchase',
      payload: { userId: 'buyer-a' },
    })
    expect(purchase.statusCode).toBe(500)
    expect(purchase.json()).not.toHaveProperty('outcome')

    await broken.close()
    await dead.end().catch(() => {})
  })
})
