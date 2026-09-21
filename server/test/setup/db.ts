import { Pool } from 'pg'
import { migrate } from '../../src/db/migrate.ts'

/**
 * One Postgres schema per test file. The files run in parallel, and a DELETE in
 * one wiped rows another had just written.
 */
export async function poolFor(databaseUrl: string, name: string): Promise<Pool> {
  const admin = new Pool({ connectionString: databaseUrl, max: 1 })
  try {
    await admin.query(`CREATE SCHEMA IF NOT EXISTS ${name}`)
  } finally {
    await admin.end()
  }
  const pool = new Pool({
    connectionString: databaseUrl,
    max: 4,
    options: `-c search_path=${name}`,
  })
  await migrate(pool)
  return pool
}

export type Campaign = {
  readonly stock: number
  readonly startMs: number
  readonly endMs: number
}

/**
 * The migration wrote 1,000 units over a wide window. Most tests want a different number.
 */
export async function writeCampaign(pool: Pool, sale: Campaign): Promise<void> {
  await pool.query('DELETE FROM stock')
  await pool.query(
    'INSERT INTO stock (id, total_units, units_left, start_at, end_at) VALUES (1, $1, $1, $2, $3)',
    [sale.stock, new Date(sale.startMs), new Date(sale.endMs)],
  )
}
