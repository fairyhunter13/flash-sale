import { existsSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import Fastify, { type FastifyInstance } from 'fastify'
import fastifyStatic from '@fastify/static'
import { Pool } from 'pg'
import { readConfig, type Config } from './config.ts'
import { Gate } from './gate/gate.ts'
import { registerOrderRoute } from './orders.ts'
import { registerRoutes } from './routes/index.ts'
import { SaleTicker, registerStream } from './routes/stream.ts'

export type App = {
  readonly fastify: FastifyInstance
  readonly gate: Gate
  readonly ticker: SaleTicker
}

/**
 * Builds the server without listening, so a test drives it through
 * `fastify.inject` and needs no port.
 */
export async function buildApp(config: Config, pool: Pool, tickMs?: number): Promise<App> {
  const gate = new Gate(pool)
  await gate.seed({ stock: config.stock, startMs: config.startMs, endMs: config.endMs })

  // A hijacked SSE socket is never idle, so a shutdown waits forever without
  // this. The onClose hook below ends each stream first.
  const fastify = Fastify({ logger: false, forceCloseConnections: true })
  const ticker = tickMs === undefined ? new SaleTicker(gate) : new SaleTicker(gate, tickMs)
  registerRoutes(fastify, gate)
  registerOrderRoute(fastify, pool)
  registerStream(fastify, ticker)
  fastify.addHook('onClose', async () => ticker.closeAll())

  return { fastify, gate, ticker }
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
