import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import Fastify, { type FastifyInstance } from 'fastify'
import fastifyStatic from '@fastify/static'
import { Pool } from 'pg'
import { readConfig, type Config } from './config.ts'
import { migrate } from './db/migrate.ts'
import { Gate } from './gate/gate.ts'
import { Pipeline } from './queue/pipeline.ts'
import { registerOrderRoute } from './routes/orders.ts'
import { registerRoutes } from './routes/sale.ts'
import { SaleTicker, registerStream } from './routes/stream.ts'

export type App = {
  readonly fastify: FastifyInstance
  readonly gate: Gate
  readonly pipeline: Pipeline
  readonly ticker: SaleTicker
}

/**
 * No listen call. Tests drive it through `fastify.inject`, which needs no port.
 */
export async function buildApp(
  config: Config,
  pool: Pool,
  tickMs?: number,
  // A test passes its own name here. Two test files then never share a Redis
  // key or a topic. The server runs one sale. It passes nothing.
  namespace = '',
): Promise<App> {
  const gate = new Gate(pool)
  const sale = await gate.campaign()

  // The pipeline rebuilds the hot state from the order rows when Redis holds no sale.
  const pipeline = await Pipeline.start({
    redisUrl: config.redisUrl,
    kafkaBrokers: config.kafkaBrokers,
    workers: config.queueWorkers,
    sale,
    gate,
    namespace,
  })

  // A hijacked SSE socket is never idle. Without the flag, a shutdown waits forever.
  const fastify = Fastify({ logger: false, forceCloseConnections: true })
  const ticker =
    tickMs === undefined ? new SaleTicker(gate, pipeline) : new SaleTicker(gate, pipeline, tickMs)
  registerRoutes(fastify, gate, pipeline)
  registerOrderRoute(fastify, pool)
  registerStream(fastify, ticker)
  fastify.addHook('onClose', async () => {
    ticker.closeAll()
    await pipeline.close()
  })

  return { fastify, gate, pipeline, ticker }
}

const WEB_DIST = fileURLToPath(new URL('../../web/dist/', import.meta.url))

/**
 * One URL runs the whole app. In development Vite serves the page and proxies
 * /api here. The handler does nothing then.
 */
async function serveWeb(fastify: FastifyInstance): Promise<void> {
  if (!existsSync(WEB_DIST)) {
    console.warn(`${WEB_DIST} is absent, so no page is served. Run npm run build first.`)
    return
  }
  await fastify.register(fastifyStatic, { root: WEB_DIST })
}

async function main(): Promise<void> {
  const config = readConfig()
  // The schema and the campaign row arrive together. A fresh clone needs no hand-written SQL.
  const migrations = new Pool({ connectionString: config.databaseUrl, max: 1 })
  const applied = await migrate(migrations).finally(() => migrations.end())
  if (applied.length > 0) console.log(`applied ${applied.join(', ')}`)

  // Postgres never sees more than DB_POOL_MAX connections, whatever the number of
  // open sockets in front of it.
  const pool = new Pool({ connectionString: config.databaseUrl, max: config.dbPoolMax })
  // A checked-out client does not reach `pool.on('error')`. Without the
  // second line, its error ends the process mid-sale.
  pool.on('error', (error: Error) => console.error(`an idle database client failed: ${error.message}`))
  pool.on('connect', (client) => client.on('error', () => {}))
  const app = await buildApp(config, pool)
  await serveWeb(app.fastify)
  await app.fastify.listen({ port: config.port, host: config.host })
}

// The entry runs only when node runs this file directly. So a test can import
// the builder above without starting a listener.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    console.error(error)
    process.exit(1)
  })
}
