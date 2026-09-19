import type { Pool, PoolClient } from 'pg'

// ON CONFLICT DO NOTHING, and never a caught 23505. The insert then writes no
// dead row, and a replayed batch costs nothing.
const INSERT = 'INSERT INTO orders(user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING'

/**
 * Writes every buyer in one transaction. Either the whole batch is in the
 * table, or none of it is. So a crash mid-batch replays the whole batch.
 */
export async function insertOrders(pool: Pool, buyerIds: readonly string[]): Promise<number> {
  if (buyerIds.length === 0) return 0

  const client: PoolClient = await pool.connect()
  try {
    await client.query('BEGIN')
    let written = 0
    for (const buyerId of buyerIds) {
      const answer = await client.query(INSERT, [buyerId])
      written += answer.rowCount ?? 0
    }
    await client.query('COMMIT')
    return written
  } catch (error) {
    await client.query('ROLLBACK')
    throw error
  } finally {
    client.release()
  }
}
