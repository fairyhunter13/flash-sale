import type { FastifyInstance } from 'fastify'
import type { Gate } from '../gate/gate.ts'
import { saleState, type SaleState } from '../gate/status.ts'

export type SaleView = {
  readonly state: SaleState
  readonly stockLeft: number
  readonly startsAt: string
  readonly endsAt: string
}

export async function readSale(gate: Gate, nowMs: number = Date.now()): Promise<SaleView> {
  const { left, startMs, endMs } = await gate.snapshot()
  return {
    state: saleState(nowMs, left, { startMs, endMs }),
    stockLeft: left,
    startsAt: new Date(startMs).toISOString(),
    endsAt: new Date(endMs).toISOString(),
  }
}

export function registerRoutes(app: FastifyInstance, gate: Gate): void {
  app.get('/api/sale', async (_request, reply) => {
    try {
      return await readSale(gate)
    } catch {
      // A Redis that does not answer is a fault, and never a sold-out sale.
      // The body carries no state field at all, so no caller can read one.
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
      const { outcome } = await gate.reserve(userId)
      return { outcome }
    } catch {
      return reply.code(500).send({ error: 'the purchase cannot be decided' })
    }
  })
}
