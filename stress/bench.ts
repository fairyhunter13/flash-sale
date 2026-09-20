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
const STOCK = Number(process.env['SALE_STOCK'] ?? 1000)
const START_MS = Date.parse(process.env['SALE_START'] ?? '2026-01-01T00:00:00Z')
const END_MS = Date.parse(process.env['SALE_END'] ?? '2036-01-01T00:00:00Z')
/** The server reuses one read of the sale for this long, so a reset waits it out. */
const CACHE_MS = 250

/**
 * Opens the sale again. Without it a sold-out sale answers every POST from the
 * cached read, and the number below would measure the fast path and not the
 * transaction.
 */
async function openTheSale(): Promise<void> {
  const pool = new Pool({ connectionString: DATABASE_URL, application_name: 'bench' })
  try {
    await pool.query('TRUNCATE orders')
    await pool.query('DELETE FROM stock')
    await pool.query(
      'INSERT INTO stock (id, units_left, start_at, end_at) VALUES (1, $1, $2, $3)',
      [STOCK, new Date(START_MS), new Date(END_MS)],
    )
    await new Promise((done) => setTimeout(done, CACHE_MS * 2))
  } finally {
    await pool.end()
  }
}

/**
 * Throughput only, and never the counts.
 *
 * autocannon reads `amount` as a per-connection quota, so the total it sends
 * is not the total asked for. Issue #228 measured 999,969 sent for a requested
 * 1,000,000 with no error reported. So run.ts owns every count, and this file
 * owns the requests a second and the latency.
 *
 * A bench that also checked a count would be wrong, not merely incomplete.
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
