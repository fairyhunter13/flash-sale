import type { Pool } from 'pg'
import { poolFor } from './setup/db.ts'
import { afterAll, beforeAll, describe, expect, inject, it } from 'vitest'

let pool: Pool

beforeAll(async () => {
  pool = await poolFor(inject('databaseUrl'), 't_schema')
})

afterAll(async () => {
  await pool.end()
})

describe('the order table', () => {
  it('the order table refuses a duplicate user', async () => {
    await pool.query('DELETE FROM orders')
    await pool.query('INSERT INTO orders(user_id) VALUES ($1)', ['buyer-a'])

    await expect(pool.query('INSERT INTO orders(user_id) VALUES ($1)', ['buyer-a'])).rejects.toThrow(
      /orders_user_id_key/,
    )

    const { rows } = await pool.query<{ count: string }>('SELECT count(*) FROM orders')
    expect(rows[0]?.count).toBe('1')
  })

  it('the second insert writes no row when it names the conflict', async () => {
    await pool.query('DELETE FROM orders')
    await pool.query('INSERT INTO orders(user_id) VALUES ($1)', ['buyer-b'])

    const again = await pool.query(
      'INSERT INTO orders(user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING',
      ['buyer-b'],
    )

    expect(again.rowCount).toBe(0)
  })
})
