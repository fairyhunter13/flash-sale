import { existsSync } from 'node:fs'
import autocannon, { type Result } from 'autocannon'
import { Pool } from 'pg'

// See run.ts for why the root .env is loaded here.
const ENV_FILE = new URL('../.env', import.meta.url)
if (existsSync(ENV_FILE)) process.loadEnvFile(ENV_FILE)

const BASE_URL = process.env['BASE_URL'] ?? 'http://127.0.0.1:3000'
const DATABASE_URL = process.env['DATABASE_URL'] ?? 'postgres://flash:flash@127.0.0.1:5432/flash'
const CONNECTIONS = Number(process.env['BENCH_CONNECTIONS'] ?? 500)
const SECONDS = Number(process.env['BENCH_SECONDS'] ?? 10)
/** The server reuses one read of the sale for this long, so a reset waits it out. */
const CACHE_MS = 250

/**
 * Puts every unit back. Without it a sold-out sale answers every POST from the
 * cached read, so the number below would measure the fast path.
 */
async function openTheSale(): Promise<void> {
  const pool = new Pool({ connectionString: DATABASE_URL, application_name: 'bench' })
  try {
    await pool.query('TRUNCATE orders')
    const { rowCount } = await pool.query('UPDATE stock SET units_left = total_units WHERE id = 1')
    if (rowCount === 0) throw new Error('the campaign row is missing. Run npm run db:migrate.')
    await new Promise((done) => setTimeout(done, CACHE_MS * 2))
  } finally {
    await pool.end()
  }
}

/**
 * Throughput only, and never the counts. autocannon reads `amount` as a
 * per-connection quota, so it sent 999,969 for a requested 1,000,000 with no
 * error (issue #228). run.ts owns every count. This file owns the latency.
 */
async function measure(name: string, options: autocannon.Options): Promise<Result> {
  console.log(`\n${name}`)
  const result = await autocannon({ connections: CONNECTIONS, duration: SECONDS, ...options })
  console.log(`  ${Math.round(result.requests.average)} requests a second`)
  console.log(`  latency: p50 ${result.latency.p50} ms, p99 ${result.latency.p99} ms`)
  console.log(`  non-2xx: ${result.non2xx}, errors: ${result.errors}`)
  return result
}

async function main(): Promise<void> {
  console.log(`${BASE_URL}, ${CONNECTIONS} connections, ${SECONDS} s each`)
  await openTheSale()

  await measure('GET /api/sale', { url: `${BASE_URL}/api/sale` })

  // One buyer id for every request. The first one wins, and the rest read
  // already-bought, so each request opens a transaction and runs 2 statements
  // in it. That is the slow path, and it is the one worth a number.
  await measure('POST /api/purchase (one repeat buyer)', {
    url: `${BASE_URL}/api/purchase`,
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ userId: 'bench-buyer' }),
  })
}

main().catch((error: unknown) => {
  console.error(error)
  process.exit(1)
})
