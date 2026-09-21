import type { Pool } from 'pg'
import { afterAll, beforeAll, beforeEach, describe, expect, inject, it } from 'vitest'
import { migrate, migrations } from '../../src/db/migrate.ts'
import { poolFor, writeCampaign } from '../setup/db.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2036-06-01T00:00:00Z')

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

  it('the second insert writes no row when `ON CONFLICT` names the constraint', async () => {
    await pool.query('DELETE FROM orders')
    await pool.query('INSERT INTO orders(user_id) VALUES ($1)', ['buyer-b'])

    const again = await pool.query(
      'INSERT INTO orders(user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING',
      ['buyer-b'],
    )

    expect(again.rowCount).toBe(0)
  })
})

describe('the stock table', () => {
  beforeEach(async () => {
    await writeCampaign(pool, { stock: 5, startMs: START, endMs: END })
  })

  it('the stock row can never go below zero', async () => {
    await pool.query('UPDATE stock SET units_left = 0 WHERE id = 1')

    await expect(pool.query('UPDATE stock SET units_left = units_left - 1 WHERE id = 1')).rejects.toThrow(
      /stock_never_negative/,
    )
  })

  it('the units left can never climb past the total', async () => {
    await expect(pool.query('UPDATE stock SET units_left = 6 WHERE id = 1')).rejects.toThrow(
      /stock_never_over_total/,
    )
  })

  it('a second sale row is refused, because there is one sale', async () => {
    await expect(
      pool.query(
        'INSERT INTO stock (id, total_units, units_left, start_at, end_at) VALUES (2, 5, 5, $1, $2)',
        [new Date(START), new Date(END)],
      ),
    ).rejects.toThrow(/stock_single_row/)
  })

  it('a campaign that ends before it starts is refused', async () => {
    await expect(pool.query('UPDATE stock SET end_at = start_at WHERE id = 1')).rejects.toThrow(
      /stock_window_ordered/,
    )
  })
})

describe('the migrations', () => {
  it('the ledger names every file, so a second run applies nothing', async () => {
    const { rows } = await pool.query<{ name: string }>('SELECT name FROM schema_migrations ORDER BY name')

    expect(rows.map((row) => row.name)).toEqual(migrations().map((one) => one.name))
    expect(await migrate(pool)).toEqual([])
  })

  it('the campaign comes from a migration, and never from the environment', async () => {
    await pool.query('DELETE FROM stock')
    await pool.query('DELETE FROM schema_migrations')

    await migrate(pool)

    const { rows } = await pool.query<{ total_units: number }>(
      'SELECT total_units FROM stock WHERE id = 1',
    )
    expect(rows[0]?.total_units).toBe(1000)
  })
})
