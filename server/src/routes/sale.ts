import type { FastifyInstance } from 'fastify'
import type { Gate } from '../gate/gate.ts'
import type { Pipeline } from '../queue/pipeline.ts'
import { saleState, type SaleState } from '../gate/status.ts'

export type SaleView = {
  readonly state: SaleState
  readonly stockLeft: number
  readonly startsAt: string
  readonly endsAt: string
}

/**
 * The window comes from the table. A restart reads the table back, not Redis.
 * The count comes from Redis, because Redis decides. A Postgres count lags the
 * sale by whatever the queue still holds, and the page then offers a unit already gone.
 */
export async function readSale(gate: Gate, pipeline: Pipeline, nowMs: number = Date.now()): Promise<SaleView> {
  const { startMs, endMs } = await gate.snapshot()
  const left = await pipeline.left()
  return {
    state: saleState(nowMs, left, { startMs, endMs }),
    stockLeft: left,
    startsAt: new Date(startMs).toISOString(),
    endsAt: new Date(endMs).toISOString(),
  }
}

export function registerRoutes(app: FastifyInstance, gate: Gate, pipeline: Pipeline): void {
  app.get('/api/sale', async (_request, reply) => {
    try {
      return await readSale(gate, pipeline)
    } catch {
      // A store that does not answer is a fault, and never a sold-out sale.
      return reply.code(500).send({ error: 'the sale cannot be read' })
    }
  })

  app.post('/api/purchase', async (request, reply) => {
    const body = request.body as { userId?: unknown } | null
    const userId = typeof body?.userId === 'string' ? body.userId.trim() : ''
    if (userId === '') {
      return reply.code(400).send({ error: 'userId is required' })
    }

    try {
      return { outcome: await pipeline.reserve(userId) }
    } catch (error) {
      // A silent 500 hides which store failed, and a stress run then reads as a
      // count with no cause. The buyer still sees one sentence.
      request.log.error({ err: error }, 'the purchase cannot be decided')
      return reply.code(500).send({ error: 'the purchase cannot be decided' })
    }
  })
}
