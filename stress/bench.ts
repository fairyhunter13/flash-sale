import { existsSync } from 'node:fs'
import autocannon, { type Result } from 'autocannon'

// See run.ts for why the root .env is loaded here.
const ENV_FILE = new URL('../.env', import.meta.url)
if (existsSync(ENV_FILE)) process.loadEnvFile(ENV_FILE)

const BASE_URL = process.env['BASE_URL'] ?? 'http://127.0.0.1:3000'
const CONNECTIONS = Number(process.env['BENCH_CONNECTIONS'] ?? 500)
const SECONDS = Number(process.env['BENCH_SECONDS'] ?? 10)

/**
 * Throughput only, and never the counts.
 *
 * autocannon reads `amount` as a per-connection quota, so the total it sends
 * is not the total asked for. Issue #228 measured 999,969 sent for a requested
 * 1,000,000 with no error reported. So run.ts owns every count, and this file
 * owns the requests a second and the latency.
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

  await measure('GET /api/sale', { url: `${BASE_URL}/api/sale` })

  // One buyer id for every request. The first one may win, and the rest read
  // already-bought, so the Lua script runs its whole path on each request.
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
