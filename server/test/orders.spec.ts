import Fastify from 'fastify'
import { Pool } from 'pg'
import { poolFor } from './setup/db.ts'
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it } from 'vitest'
import { registerOrderRoute } from '../src/orders.ts'

let pool: Pool
let app: ReturnType<typeof Fastify>

beforeAll(async () => {
  pool = await poolFor(inject('databaseUrl'), 't_orders')
})

afterAll(async () => {
  await pool.end()
})

beforeEach(async () => {
  await pool.query('DELETE FROM orders')
  app = Fastify({ logger: false })
  registerOrderRoute(app, pool)
})

describe('the purchase state', () => {
  it('the purchase state reads the record', async () => {
    await pool.query('INSERT INTO orders(user_id) VALUES ($1)', ['buyer-a'])

    const held = await app.inject({ method: 'GET', url: '/api/purchase/buyer-a' })
    expect(held.statusCode).toBe(200)
    expect(held.json().held).toBe(true)
    expect(Date.parse(held.json().at)).not.toBeNaN()

    const none = await app.inject({ method: 'GET', url: '/api/purchase/buyer-b' })
    expect(none.statusCode).toBe(200)
    expect(none.json()).toEqual({ held: false })
  })

  it('a dead Postgres gives 503 and no held field', async () => {
    // Port 1 answers nothing, so the query rejects rather than returning no row.
    const dead = new Pool({ connectionString: 'postgres://flash:flash@127.0.0.1:1/flash' })
    const broken = Fastify({ logger: false })
    registerOrderRoute(broken, dead)

    const answer = await broken.inject({ method: 'GET', url: '/api/purchase/buyer-a' })

    expect(answer.statusCode).toBe(503)
    expect(answer.json()).not.toHaveProperty('held')

    await broken.close()
    await dead.end()
  })
})
