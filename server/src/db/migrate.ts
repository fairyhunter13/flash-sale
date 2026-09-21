import { readFileSync, readdirSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { Pool, type PoolClient } from 'pg'
import { readConfig } from '../config.ts'

const DIRECTORY = new URL('../../sql/migrations/', import.meta.url)

/** Postgres hands this lock to one caller, so 2 servers booting together
 * apply each file once between them. */
const LOCK_KEY = 8_713_220_101

const LEDGER = `CREATE TABLE IF NOT EXISTS schema_migrations (
  name       text        PRIMARY KEY,
  applied_at timestamptz NOT NULL DEFAULT now()
)`

export type Migration = {
  readonly name: string
  readonly sql: string
}

/** Every file in `server/sql/migrations`, in name order. */
export function migrations(): readonly Migration[] {
  const names = readdirSync(DIRECTORY)
    .filter((name) => name.endsWith('.sql'))
    .sort()
  return names.map((name) => ({ name, sql: readFileSync(new URL(name, DIRECTORY), 'utf8') }))
}

/**
 * Each file commits with its own ledger row, so a failed file leaves no
 * half-applied schema. Never edit an applied file. A change is a new file.
 */
export async function migrate(pool: Pool): Promise<readonly string[]> {
  const client: PoolClient = await pool.connect()
  const applied: string[] = []
  try {
    await client.query('SELECT pg_advisory_lock($1)', [LOCK_KEY])
    await client.query(LEDGER)
    const done = await client.query<{ name: string }>('SELECT name FROM schema_migrations')
    const seen = new Set(done.rows.map((row) => row.name))

    for (const one of migrations()) {
      if (seen.has(one.name)) continue
      try {
        await client.query('BEGIN')
        await client.query(one.sql)
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [one.name])
        await client.query('COMMIT')
      } catch (error) {
        await client.query('ROLLBACK').catch(() => {})
        throw new Error(`the migration ${one.name} failed: ${(error as Error).message}`)
      }
      applied.push(one.name)
    }
  } finally {
    await client.query('SELECT pg_advisory_unlock($1)', [LOCK_KEY]).catch(() => {})
    client.release()
  }
  return applied
}

/** `npm run db:migrate`. The server migrates at boot, so this is for a database it never opens. */
async function main(): Promise<void> {
  const pool = new Pool({ connectionString: readConfig().databaseUrl, max: 1 })
  try {
    const applied = await migrate(pool)
    console.log(applied.length === 0 ? 'the database is up to date' : `applied ${applied.join(', ')}`)
  } finally {
    await pool.end()
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    console.error(error)
    process.exit(1)
  })
}
