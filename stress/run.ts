import { existsSync } from 'node:fs'
import { Redis } from 'ioredis'
import { Pool } from 'pg'
import pLimit from 'p-limit'
import { Agent, request } from 'undici'

// The root .env holds the ports this box uses, and nothing else loads it for a
// plain `node` run. loadEnvFile never overwrites a variable already set.
const ENV_FILE = new URL('../.env', import.meta.url)
if (existsSync(ENV_FILE)) process.loadEnvFile(ENV_FILE)

const BASE_URL = process.env['BASE_URL'] ?? 'http://127.0.0.1:3000'
const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6399'
const DATABASE_URL = process.env['DATABASE_URL'] ?? 'postgres://flash:flash@127.0.0.1:5499/flash'
const STOCK = Number(process.env['SALE_STOCK'] ?? 1000)
const BUYERS = Number(process.env['STRESS_BUYERS'] ?? 10_000)
const CONNECTIONS = Number(process.env['STRESS_CONNECTIONS'] ?? 500)
const START_MS = Date.parse(process.env['SALE_START'] ?? '2026-01-01T00:00:00Z')
const END_MS = Date.parse(process.env['SALE_END'] ?? '2036-01-01T00:00:00Z')
/** How long the recorder may take to move every win into Postgres. */
const DRAIN_LIMIT_MS = 30_000

const KEY = { stock: 'sale:stock', buyers: 'sale:buyers', wins: 'sale:wins', window: 'sale:window' }

/**
 * The reset below deletes the sale and empties the orders table, so this file
 * must never reach a shared host. A hostname outside the list is refused, and
 * the operator can widen it with STRESS_ALLOW_HOST.
 */
const LOCAL = new Set(['localhost', '127.0.0.1', '::1', '[::1]'])

function assertLocal(name: string, url: string): void {
  const host = new URL(url).hostname
  if (LOCAL.has(host)) return
  if (process.env['STRESS_ALLOW_HOST'] === host) return
  throw new Error(
    `${name} points at ${host}. This run empties the sale and the orders table, so it refuses ` +
      `any host but localhost. Set STRESS_ALLOW_HOST=${host} to override.`,
  )
}

type Tally = Record<string, number>

async function reset(redis: Redis, pool: Pool): Promise<void> {
  await redis.del(KEY.stock, KEY.buyers, KEY.wins, KEY.window)
  await redis
    .multi()
    .set(KEY.stock, String(STOCK))
    .hset(KEY.window, 'start_ms', String(START_MS), 'end_ms', String(END_MS))
    .exec()
  await pool.query('TRUNCATE orders')
}

async function drive(): Promise<{ tally: Tally; seconds: number }> {
  // One agent for the whole run, so 500 sockets are opened once and reused.
  const agent = new Agent({ connections: CONNECTIONS, pipelining: 1 })
  const limit = pLimit(CONNECTIONS)
  const tally: Tally = {}

  const startedAt = performance.now()
  await Promise.all(
    Array.from({ length: BUYERS }, (_unused, n) =>
      limit(async () => {
        try {
          const answer = await request(`${BASE_URL}/api/purchase`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ userId: `buyer-${n}` }),
            dispatcher: agent,
          })
          const body = (await answer.body.json()) as { outcome?: string }
          const key =
            answer.statusCode === 200 ? (body.outcome ?? 'no-outcome') : `http-${answer.statusCode}`
          tally[key] = (tally[key] ?? 0) + 1
        } catch (error) {
          const key = `error-${(error as { code?: string }).code ?? 'unknown'}`
          tally[key] = (tally[key] ?? 0) + 1
        }
      }),
    ),
  )
  const seconds = (performance.now() - startedAt) / 1000

  await agent.close()
  return { tally, seconds }
}

async function countOrders(pool: Pool): Promise<number> {
  const { rows } = await pool.query<{ count: string }>('SELECT count(*) FROM orders')
  return Number(rows[0]?.count ?? 0)
}

/** The recorder runs in its own process, so the last wins land after the drive ends. */
async function waitForDrain(pool: Pool, want: number): Promise<number> {
  const until = Date.now() + DRAIN_LIMIT_MS
  let seen = await countOrders(pool)
  while (seen < want && Date.now() < until) {
    await new Promise((done) => setTimeout(done, 250))
    seen = await countOrders(pool)
  }
  return seen
}

type Check = { readonly name: string; readonly got: number; readonly want: number }

function report(checks: readonly Check[], tally: Tally, seconds: number): boolean {
  const width = Math.max(...checks.map((it) => it.name.length))
  let allMatch = true
  for (const { name, got, want } of checks) {
    const mark = got === want ? 'ok  ' : 'BAD '
    if (got !== want) allMatch = false
    console.log(`${mark}${name.padEnd(width)}  ${String(got).padStart(6)}  (want ${want})`)
  }

  const unexpected = Object.entries(tally).filter(([key]) => key !== 'won' && key !== 'sold-out')
  for (const [key, count] of unexpected) {
    console.log(`     ${key.padEnd(width)}  ${String(count).padStart(6)}`)
  }

  console.log(`\n${BUYERS} buyers over ${CONNECTIONS} connections in ${seconds.toFixed(2)} s`)
  console.log(`${Math.round(BUYERS / seconds)} purchase requests a second`)
  return allMatch
}

async function main(): Promise<void> {
  assertLocal('REDIS_URL', REDIS_URL)
  assertLocal('DATABASE_URL', DATABASE_URL)

  const redis = new Redis(REDIS_URL)
  const pool = new Pool({ connectionString: DATABASE_URL })
  try {
    await reset(redis, pool)
    console.log(`reset: sale:stock=${STOCK}, orders=0 rows`)
    console.log(`driving ${BUYERS} buyers, ${CONNECTIONS} connections`)

    const { tally, seconds } = await drive()
    const orders = await waitForDrain(pool, STOCK)

    const checks: Check[] = [
      { name: 'won', got: tally['won'] ?? 0, want: STOCK },
      { name: 'sold-out', got: tally['sold-out'] ?? 0, want: BUYERS - STOCK },
      { name: 'other', got: BUYERS - (tally['won'] ?? 0) - (tally['sold-out'] ?? 0), want: 0 },
      { name: 'redis stock left', got: Number(await redis.get(KEY.stock)), want: 0 },
      { name: 'redis buyers', got: await redis.scard(KEY.buyers), want: STOCK },
      { name: 'pg orders', got: orders, want: STOCK },
    ]

    const passed = report(checks, tally, seconds)
    console.log(passed ? '\nPASS' : '\nFAIL')
    if (!passed) process.exitCode = 1
  } finally {
    await Promise.all([redis.quit(), pool.end()])
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exit(1)
})
