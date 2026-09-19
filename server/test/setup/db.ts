import { readFileSync } from 'node:fs'
import { Pool } from 'pg'

const SCHEMA = readFileSync(new URL('../../sql/schema.sql', import.meta.url), 'utf8')

/**
 * One Postgres schema per test file, so two files never share the orders
 * table. With one shared table a DELETE in one file wiped the rows another
 * file had just written, and the files run in parallel.
 */
export async function poolFor(databaseUrl: string, name: string): Promise<Pool> {
  const admin = new Pool({ connectionString: databaseUrl, max: 1 })
  try {
    await admin.query(`CREATE SCHEMA IF NOT EXISTS ${name}`)
    // The two run in one call, so the CREATE TABLE lands inside the schema the
    // SET names. A second call would arrive on a reset connection.
    await admin.query(`SET search_path TO ${name}; ${SCHEMA}`)
  } finally {
    await admin.end()
  }
  return new Pool({
    connectionString: databaseUrl,
    max: 4,
    options: `-c search_path=${name}`,
  })
}
