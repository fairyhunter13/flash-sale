import type { Pool } from 'pg'
import type { Redis } from 'ioredis'
import { KEY } from '../gate/gate.ts'
import { insertOrders } from './db.ts'

export const GROUP = 'recorder'

export type RecorderOptions = {
  /** Names this reader inside the group. Two recorders need two names. */
  readonly consumerName?: string
  /** How long one XREADGROUP waits for a new entry. */
  readonly blockMs?: number
  /** How long an entry may sit unacknowledged before another reader claims it. */
  readonly claimAfterMs?: number
  readonly batchSize?: number
}

type Entry = { readonly id: string; readonly buyerId: string }

/**
 * Drains sale:wins into the orders table.
 *
 * The row is committed before the entry is acknowledged. So a crash between
 * the two replays the batch, and the replay writes nothing new because the
 * insert names the conflict. A lost win is the failure that matters, and a
 * repeated win is not.
 */
export class Recorder {
  private readonly consumerName: string
  private readonly blockMs: number
  private readonly claimAfterMs: number
  private readonly batchSize: number
  private running = false
  private loop: Promise<void> | undefined

  constructor(
    private readonly redis: Redis,
    private readonly pool: Pool,
    options: RecorderOptions = {},
  ) {
    this.consumerName = options.consumerName ?? `recorder-${process.pid}`
    this.blockMs = options.blockMs ?? 1000
    this.claimAfterMs = options.claimAfterMs ?? 30_000
    this.batchSize = options.batchSize ?? 200
  }

  /** Creates the group. MKSTREAM makes the first run work before any win. */
  async ensureGroup(): Promise<void> {
    try {
      await this.redis.xgroup('CREATE', KEY.wins, GROUP, '0', 'MKSTREAM')
    } catch (error) {
      if (!String(error).includes('BUSYGROUP')) throw error
    }
  }

  /**
   * Takes over entries another reader left unacknowledged, then reads new ones.
   * Returns how many rows the orders table gained.
   */
  async drainOnce(): Promise<number> {
    const claimed = await this.claimStale()
    const fresh = await this.readNew()
    const entries = [...claimed, ...fresh]
    if (entries.length === 0) return 0

    const written = await insertOrders(
      this.pool,
      entries.map((it) => it.buyerId),
    )
    await this.redis.xack(KEY.wins, GROUP, ...entries.map((it) => it.id))
    return written
  }

  start(): void {
    if (this.running) return
    this.running = true
    this.loop = this.run()
  }

  async stop(): Promise<void> {
    this.running = false
    await this.loop
    this.loop = undefined
  }

  private async run(): Promise<void> {
    while (this.running) {
      try {
        await this.drainOnce()
      } catch (error) {
        // One bad batch must not stop the recorder, or every later win is lost.
        console.error('the recorder could not drain a batch', error)
        await new Promise((done) => setTimeout(done, this.blockMs))
      }
    }
  }

  private async claimStale(): Promise<Entry[]> {
    const answer = await this.redis.xautoclaim(
      KEY.wins,
      GROUP,
      this.consumerName,
      this.claimAfterMs,
      '0-0',
      'COUNT',
      this.batchSize,
    )
    return readEntries((answer as unknown[])[1])
  }

  private async readNew(): Promise<Entry[]> {
    const answer = await this.redis.xreadgroup(
      'GROUP',
      GROUP,
      this.consumerName,
      'COUNT',
      this.batchSize,
      'BLOCK',
      this.blockMs,
      'STREAMS',
      KEY.wins,
      '>',
    )
    if (answer === null) return []
    const streams = answer as [string, unknown[]][]
    return streams.flatMap(([, rows]) => readEntries(rows))
  }
}

/** Reads the `[id, [field, value, ...]]` shape both commands return. */
function readEntries(rows: unknown): Entry[] {
  if (!Array.isArray(rows)) return []
  const entries: Entry[] = []
  for (const row of rows as [string, string[]][]) {
    const [id, fields] = row
    const at = fields.indexOf('buyer_id')
    const buyerId = at === -1 ? undefined : fields[at + 1]
    if (buyerId !== undefined) entries.push({ id, buyerId })
  }
  return entries
}
