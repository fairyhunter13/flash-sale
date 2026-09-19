import { fileURLToPath } from 'node:url'
import Fastify, { type FastifyInstance } from 'fastify'
import { Redis } from 'ioredis'
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
export async function buildApp(
  config: Config,
  redis: Redis,
  pool: Pool,
  tickMs?: number,
): Promise<App> {
  const gate = new Gate(redis)
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

async function main(): Promise<void> {
  const config = readConfig()
  const redis = new Redis(config.redisUrl)
  const pool = new Pool({ connectionString: config.databaseUrl })
  const app = await buildApp(config, redis, pool)
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
