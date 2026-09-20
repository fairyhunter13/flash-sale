import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import Fastify, { type FastifyInstance } from 'fastify'
import fastifyStatic from '@fastify/static'
import { Pool } from 'pg'
import { readConfig, type Config } from './config.ts'
import { Gate } from './gate/gate.ts'
import { registerOrderRoute } from './orders.ts'
import { Pipeline } from './queue/pipeline.ts'
import { registerRoutes } from './routes/index.ts'
import { SaleTicker, registerStream } from './routes/stream.ts'

export type App = {
  readonly fastify: FastifyInstance
  readonly gate: Gate
  readonly pipeline: Pipeline
  readonly ticker: SaleTicker
}

/**
 * Builds the server without listening, so a test drives it through
 * `fastify.inject` and needs no port.
 */
export async function buildApp(
  config: Config,
  pool: Pool,
  tickMs?: number,
  // The server runs one sale, so it passes nothing. A test passes its own name
  // here, and two test files then never share a Redis key or a topic.
  namespace = '',
): Promise<App> {
  const sale = { stock: config.stock, startMs: config.startMs, endMs: config.endMs }
  const gate = new Gate(pool)
  await gate.seed(sale)

  // The pipeline starts after the seed, because it rebuilds the hot state from
  // the order rows when Redis holds no sale.
  const pipeline = await Pipeline.start({
    redisUrl: config.redisUrl,
    kafkaBrokers: config.kafkaBrokers,
    workers: config.queueWorkers,
    sale,
    gate,
    namespace,
  })

  // A hijacked SSE socket is never idle, so a shutdown waits forever without
  // the flag. The onClose hook below ends each stream first.
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
 * Serves the built page from the same origin as the API, so a reviewer runs
 * `npm run build` then `npm start` and opens one URL. In development Vite
 * serves the page instead and proxies /api here, so this does nothing.
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
  // The cap is the whole answer to "what if a million people arrive". Postgres
  // never sees more than DB_POOL_MAX connections from this process, whatever
  // the number of open sockets in front of it.
  const pool = new Pool({ connectionString: config.databaseUrl, max: config.dbPoolMax })
  // A client the pool still holds sends its error to `pool.on('error')`. A
  // client a request already checked out does not, so without the second line
  // that error reaches no handler and Node ends the process mid-sale.
  pool.on('error', (error: Error) => console.error(`an idle database client failed: ${error.message}`))
  pool.on('connect', (client) => client.on('error', () => {}))
  const app = await buildApp(config, pool)
  await serveWeb(app.fastify)
  await app.fastify.listen({ port: config.port, host: config.host })
}

// The entry runs only when node was pointed at this file. An import never
// starts a listener, so a test can import the builder above.
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error: unknown) => {
    console.error(error)
    process.exit(1)
  })
}
