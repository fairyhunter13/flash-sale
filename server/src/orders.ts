import type { FastifyInstance } from 'fastify'
import type { Pool } from 'pg'

export type Held = {
  readonly held: boolean
  /** The instant the order row was written. Absent when the buyer holds nothing. */
  readonly at?: string
}

export async function readHeld(pool: Pool, userId: string): Promise<Held> {
  const { rows } = await pool.query<{ created_at: Date }>(
    'SELECT created_at FROM orders WHERE user_id = $1',
    [userId],
  )
  const row = rows[0]
  return row === undefined ? { held: false } : { held: true, at: row.created_at.toISOString() }
}

export function registerOrderRoute(app: FastifyInstance, pool: Pool): void {
  app.get('/api/purchase/:userId', async (request, reply) => {
    const userId = (request.params as { userId: string }).userId.trim()
    if (userId === '') return reply.code(400).send({ error: 'userId is required' })

    try {
      return await readHeld(pool, userId)
    } catch {
      // 503 and never 200 with held false. "No row" and "cannot read" are
      // different answers, and a buyer who holds a unit must never be told
      // that they hold nothing.
      return reply.code(503).send({ error: 'the purchase state cannot be read' })
    }
  })
}
