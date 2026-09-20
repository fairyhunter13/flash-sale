import { existsSync } from 'node:fs'
import { Pool } from 'pg'
import pLimit from 'p-limit'
import { createClient } from 'redis'
import { Agent, request } from 'undici'

// The root .env holds the ports this box uses, and nothing else loads it for a
// plain `node` run. loadEnvFile never overwrites a variable already set.
const ENV_FILE = new URL('../.env', import.meta.url)
if (existsSync(ENV_FILE)) process.loadEnvFile(ENV_FILE)

const BASE_URL = process.env['BASE_URL'] ?? 'http://127.0.0.1:3000'
const DATABASE_URL = process.env['DATABASE_URL'] ?? 'postgres://flash:flash@127.0.0.1:5432/flash'
const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://127.0.0.1:6379'
/** The two keys the sale keeps hot. They must match server/src/queue/pipeline.ts. */
const SOLD_KEY = 'sale:sold'
const BUYERS_KEY = 'sale:buyers'
/** How long the run waits for the workers to write the last win into Postgres. */
const DRAIN_MS = Number(process.env['STRESS_DRAIN_MS'] ?? 60_000)
const STOCK = Number(process.env['SALE_STOCK'] ?? 1000)
const BUYERS = Number(process.env['STRESS_BUYERS'] ?? 10_000)
const CONNECTIONS = Number(process.env['STRESS_CONNECTIONS'] ?? 500)
const START_MS = Date.parse(process.env['SALE_START'] ?? '2026-01-01T00:00:00Z')
const END_MS = Date.parse(process.env['SALE_END'] ?? '2036-01-01T00:00:00Z')
/** The server reuses one read of the sale for this long, so a reset waits it out. */
const CACHE_MS = 250

/**
 * The reset below empties the sale and the orders table, so this file must
 * never reach a shared host. A hostname outside the list is refused, and the
 * operator can widen it with STRESS_ALLOW_HOST.
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

/**
 * Empties the sale in both stores.
 *
 * Redis holds the live count, so a reset that touches Postgres alone leaves the
 * sale sold out and the next run wins nothing.
 *
 * `queue_offsets` stays. Each row is the point one worker reads from, so a
 * worker that lost it would seek back to offset 0 and write the records of the
 * previous run into the fresh sale.
 */
async function reset(pool: Pool, redis: RedisLike): Promise<void> {
  await pool.query('TRUNCATE orders')
  await pool.query('DELETE FROM stock')
  await pool.query('INSERT INTO stock (id, units_left, start_at, end_at) VALUES (1, $1, $2, $3)', [
    STOCK,
    new Date(START_MS),
    new Date(END_MS),
  ])
  await redis.del([SOLD_KEY, BUYERS_KEY])
  // The running server still holds the old count for one cache window.
  await new Promise((done) => setTimeout(done, CACHE_MS * 2))
}

type RedisLike = { del: (keys: string[]) => Promise<number>; quit: () => Promise<unknown> }

/**
 * Waits until the order rows stop arriving, and reports how long that took.
 *
 * A buyer is told `won` by Redis, and the row lands later, through Kafka. So a
 * count read the moment the drive ends is short by whatever the queue still
 * holds. The wait ends on the wanted count, or on 10 seconds with no new row.
 *
 * 10 and not 2. A fetch pause of 2.9 seconds was measured mid-drain, and the
 * run then reported 760 of 1,000 rows although all 1,000 landed a moment
 * later. A quiet window shorter than the longest pause reports a healthy queue
 * as a failure.
 */
async function drain(pool: Pool, wanted: number): Promise<{ rows: number; ms: number }> {
  const startedAt = performance.now()
  const until = startedAt + DRAIN_MS
  let rows = await countOrders(pool)
  let quietSince = performance.now()
  while (rows < wanted && performance.now() < until) {
    await new Promise((done) => setTimeout(done, 50))
    const now = await countOrders(pool)
    if (now !== rows) quietSince = performance.now()
    rows = now
    if (performance.now() - quietSince > 10_000) break
  }
  return { rows, ms: Math.round(performance.now() - startedAt) }
}

/**
 * Samples how many Postgres backends the server holds while the run is in
 * flight. It is the number the connection cap is there to bound.
 */
function watchBackends(pool: Pool): { stop: () => number } {
  let peak = 0
  const timer = setInterval(() => {
    void pool
      .query<{ n: number }>(
        `SELECT count(*)::int AS n FROM pg_stat_activity
         WHERE datname = current_database() AND application_name <> 'stress'`,
      )
      .then(({ rows }) => {
        peak = Math.max(peak, rows[0]?.n ?? 0)
      })
      .catch(() => {})
  }, 50)
  return {
    stop: () => {
      clearInterval(timer)
      return peak
    },
  }
}

async function drive(): Promise<{ tally: Tally; seconds: number }> {
  // One agent for the whole run, so the sockets are opened once and reused.
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

async function unitsLeft(pool: Pool): Promise<number> {
  const { rows } = await pool.query<{ units_left: number }>(
    'SELECT units_left FROM stock WHERE id = 1',
  )
  return rows[0]?.units_left ?? -1
}

type Check = { readonly name: string; readonly got: number; readonly want: number }

function report(checks: readonly Check[], tally: Tally, seconds: number, peak: number): boolean {
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
  console.log(`${peak} Postgres backends at the peak, for ${CONNECTIONS} open sockets`)
  return allMatch
}

async function main(): Promise<void> {
  assertLocal('DATABASE_URL', DATABASE_URL)

  const pool = new Pool({ connectionString: DATABASE_URL, application_name: 'stress' })
  const redis = createClient({ url: REDIS_URL })
  await redis.connect()
  try {
    await reset(pool, redis as unknown as RedisLike)
    console.log(`reset: units_left=${STOCK}, orders=0 rows, ${SOLD_KEY} and ${BUYERS_KEY} dropped`)
    console.log(`driving ${BUYERS} buyers, ${CONNECTIONS} connections`)

    const watcher = watchBackends(pool)
    const { tally, seconds } = await drive()
    const peak = watcher.stop()

    const drained = await drain(pool, STOCK)
    console.log(`queue drained in ${drained.ms} ms`)

    const checks: Check[] = [
      { name: 'won', got: tally['won'] ?? 0, want: STOCK },
      { name: 'sold-out', got: tally['sold-out'] ?? 0, want: BUYERS - STOCK },
      { name: 'other', got: BUYERS - (tally['won'] ?? 0) - (tally['sold-out'] ?? 0), want: 0 },
      { name: 'units left', got: await unitsLeft(pool), want: 0 },
      { name: 'pg orders', got: drained.rows, want: STOCK },
    ]

    const passed = report(checks, tally, seconds, peak)
    console.log(passed ? '\nPASS' : '\nFAIL')
    if (!passed) process.exitCode = 1
  } finally {
    await pool.end()
    await redis.quit()
  }
}

main().catch((error: unknown) => {
  console.error(error)
  process.exit(1)
})
