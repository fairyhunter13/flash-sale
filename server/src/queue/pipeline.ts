import { Kafka, Partitioners, type Admin, type Consumer, type Producer } from 'kafkajs'
import { createClient, type RedisClientType } from 'redis'
import type { Gate, SaleNumbers } from '../gate/gate.ts'
import { saleState, type Outcome } from '../gate/status.ts'
import { REHYDRATE, RESERVE } from './scripts.ts'

export const TOPIC = 'sale.wins'
export const PARTITIONS = 4
const GROUP = 'sale-writers'

// The only three keys in Redis.
export const SOLD_KEY = 'sale:sold'
export const BUYERS_KEY = 'sale:buyers'
export const OUTBOX_KEY = 'sale:outbox'

/** A win that never reached Kafka waits here. The sweep is the only thing that finds it. */
const SWEEP_MS = 250

/** Records between two Kafka commits. An uncommitted tail replays, and the fence refuses it. */
const COMMIT_EVERY = 25

export type PipelineOptions = {
  readonly redisUrl: string
  readonly kafkaBrokers: readonly string[]
  readonly workers: number
  readonly sale: SaleNumbers
  readonly gate: Gate
  /**
   * A test sets it, so two test files that share one Redis never read each other's sale.
   * A second campaign would use the same field. See `docs/design-experiments.md`.
   */
  readonly namespace?: string
}

export type PipelineCounts = {
  produced: number
  consumed: number
  written: number
  replayed: number
  duplicateBuyers: number
  refusedByDatabase: number
  refusedByFastPath: number
  losersRemoved: number
}

/**
 * Kafka keeps order inside one partition only, and several workers write at once.
 * So I carry the number `INCR` returns into `orders.seq`. `ORDER BY seq` reads
 * arrival order, not insertion order.
 */
export class Pipeline {
  private readonly redis: RedisClientType
  private readonly kafka: Kafka
  private readonly admin: Admin
  private readonly producer: Producer
  private readonly consumers: Consumer[] = []
  private readonly gate: Gate
  private readonly stock: number
  private readonly window: { startMs: number; endMs: number }
  readonly topic: string
  private readonly group: string
  private readonly soldKey: string
  private readonly buyersKey: string
  private readonly outboxKey: string
  private readonly shas = new Map<string, string>()
  private sweep: NodeJS.Timeout | undefined
  private sweeping = false
  private readonly sinceCommit = new Map<number, number>()
  readonly counts: PipelineCounts = {
    produced: 0,
    consumed: 0,
    written: 0,
    replayed: 0,
    duplicateBuyers: 0,
    refusedByDatabase: 0,
    refusedByFastPath: 0,
    losersRemoved: 0,
  }

  private constructor(options: PipelineOptions) {
    this.gate = options.gate
    this.stock = options.sale.stock
    this.window = { startMs: options.sale.startMs, endMs: options.sale.endMs }
    const tail = options.namespace === undefined || options.namespace === '' ? '' : `.${options.namespace}`
    this.topic = `${TOPIC}${tail}`
    this.group = `${GROUP}${tail}`
    this.soldKey = `${SOLD_KEY}${tail}`
    this.buyersKey = `${BUYERS_KEY}${tail}`
    this.outboxKey = `${OUTBOX_KEY}${tail}`
    this.redis = createClient({ url: options.redisUrl })
    this.redis.on('error', (error: Error) => console.error(`redis: ${error.message}`))
    this.kafka = new Kafka({
      clientId: 'flash-sale',
      brokers: [...options.kafkaBrokers],
      retry: { retries: 8 },
      logLevel: 1,
    })
    this.admin = this.kafka.admin()
    this.producer = this.kafka.producer({
      // The broker drops a record it already holds. So a retried send after
      // a timeout writes one record, not two.
      idempotent: true,
      maxInFlightRequests: 5,
      createPartitioner: Partitioners.DefaultPartitioner,
    })
  }

  static async start(options: PipelineOptions): Promise<Pipeline> {
    const pipeline = new Pipeline(options)
    await pipeline.redis.connect()
    await pipeline.admin.connect()
    await pipeline.admin.createTopics({
      topics: [{ topic: pipeline.topic, numPartitions: PARTITIONS, replicationFactor: 1 }],
    })
    await pipeline.producer.connect()
    for (const source of [RESERVE, REHYDRATE]) pipeline.shas.set(source, await pipeline.redis.scriptLoad(source))
    await pipeline.restoreIfEmpty()
    // The sweep starts after the rebuild, or it reads a hash that `rehydrate` is clearing.
    pipeline.sweep = setInterval(() => void pipeline.sweepOutbox(), SWEEP_MS)
    pipeline.sweep.unref()
    for (let id = 0; id < Math.max(1, options.workers); id += 1) await pipeline.startWorker()
    return pipeline
  }

  /**
   * Redis runs the whole script as one command, so no other client sees a half-done
   * reserve. The script writes `sale:outbox` with the win, and the Kafka send clears it.
   * A crash between the two leaves the row for the sweep.
   */
  async reserve(buyerId: string, nowMs: number = Date.now()): Promise<Outcome> {
    const state = saleState(nowMs, 1, this.window)
    if (state === 'pending') return 'not-open'
    if (state === 'closed') return 'over'

    const [outcome, seq, why] = (await this.runScript(
      RESERVE,
      [this.buyersKey, this.soldKey, this.outboxKey],
      [buyerId, String(this.stock)],
    )) as [string, string, string]

    if (outcome === 'sold-out') {
      if (why === 'fast') this.counts.refusedByFastPath += 1
      else this.counts.losersRemoved += 1
      return 'sold-out'
    }
    if (outcome === 'already-bought') return 'already-bought'

    // The buyer holds the unit from here. Kafka delivery is the sweep's job now.
    this.counts.produced += 1
    await this.deliver(buyerId, Number(seq)).catch((error: Error) =>
      console.error(`kafka send failed, the sweep holds the win: ${error.message}`),
    )
    return 'won'
  }

  async left(): Promise<number> {
    return Math.max(0, this.stock - Number((await this.redis.get(this.soldKey)) ?? 0))
  }

  /** A refused buyer is removed, so this follows the stock, not the traffic. */
  async buyersHeld(): Promise<number> {
    return this.redis.sCard(this.buyersKey)
  }

  /** Waits until Postgres holds every win this process produced. */
  async drained(limitMs = 30_000): Promise<boolean> {
    const until = Date.now() + limitMs
    while (Date.now() < until) {
      // A replay and a duplicate buyer are the same record twice. Neither counts.
      // I counted them once, and the check went true at 49 of 50 rows on a 4-worker run.
      const done = this.counts.written + this.counts.refusedByDatabase
      if (done >= this.counts.produced) return true
      await new Promise((ready) => setTimeout(ready, 25))
    }
    return false
  }

  /**
   * I use `max(seq)` for the counter, not the row count. A count hands the next buyer a place
   * someone already holds. A win still in Kafka has no order row. The rebuild is exact only after
   * the queue drains.
   *
   * The script never lowers `sale:sold`, so a rebuild against a live counter is safe.
   */
  async rehydrate(): Promise<{ buyers: number; highestSeq: number; ms: number }> {
    const startedAt = Date.now()
    const winners = await this.gate.winners()
    const highest = winners.reduce((top, one) => Math.max(top, one.seq), 0)
    await this.runScript(
      REHYDRATE,
      [this.buyersKey, this.soldKey, this.outboxKey],
      [String(highest), ...winners.map((one) => one.buyerId)],
    )
    return { buyers: winners.length, highestSeq: highest, ms: Date.now() - startedAt }
  }

  async close(): Promise<void> {
    if (this.sweep !== undefined) clearInterval(this.sweep)
    for (const consumer of this.consumers) await consumer.disconnect().catch(() => {})
    await this.producer.disconnect().catch(() => {})
    await this.admin.disconnect().catch(() => {})
    await this.redis.quit().catch(() => {})
  }

  /**
   * `EVALSHA` sends the hash, never the body. A Redis restart drops the body, and the
   * `NOSCRIPT` error below is the only warning. The fallback loads it again.
   */
  private async runScript(source: string, keys: string[], args: string[]): Promise<unknown> {
    const sha = this.shas.get(source)
    if (sha !== undefined) {
      try {
        return await this.redis.evalSha(sha, { keys, arguments: args })
      } catch (error) {
        if (!(error as Error).message.includes('NOSCRIPT')) throw error
      }
    }
    const fresh = await this.redis.scriptLoad(source)
    this.shas.set(source, fresh)
    return this.redis.evalSha(fresh, { keys, arguments: args })
  }

  /** The `HDEL` is the proof the win reached Kafka. It runs only after the send returns. */
  private async deliver(buyerId: string, seq: number): Promise<void> {
    await this.producer.send({
      // The key is the buyer, so one buyer keeps one partition and keeps order.
      topic: this.topic,
      messages: [{ key: buyerId, value: JSON.stringify({ buyerId, seq }) }],
    })
    await this.redis.hDel(this.outboxKey, buyerId)
  }

  /**
   * A row still in `sale:outbox` is a unit the sale sold and Kafka never saw.
   * A second send is safe, because the producer is idempotent and `orders` holds
   * `UNIQUE (user_id)`.
   */
  private async sweepOutbox(): Promise<void> {
    if (this.sweeping) return
    this.sweeping = true
    try {
      const stranded = await this.redis.hGetAll(this.outboxKey)
      for (const [buyerId, seq] of Object.entries(stranded)) await this.deliver(buyerId, Number(seq))
    } catch (error) {
      console.error(`outbox sweep failed: ${(error as Error).message}`)
    } finally {
      this.sweeping = false
    }
  }

  /**
   * No counter in Redis means Redis was lost. Rebuild only then.
   * A live Redis runs ahead of Postgres by what the queue holds.
   */
  private async restoreIfEmpty(): Promise<void> {
    if ((await this.redis.exists(this.soldKey)) === 1) return
    const restored = await this.rehydrate()
    if (restored.buyers > 0) {
      console.warn(`Redis held no sale, so it was rebuilt from ${restored.buyers} order rows in ${restored.ms} ms.`)
    }
  }

  /**
   * `autoCommit` is off because a timer commits an offset the database never wrote.
   * The commit below runs after the Postgres transaction, never before it. It runs
   * once every `COMMIT_EVERY` records, and a commit that never happened replays.
   *
   * There is no `seek`. A replay from any earlier offset meets the fence in
   * `Gate.record`, so the Kafka offset is a resume hint and costs only time.
   */
  private async startWorker(): Promise<void> {
    const consumer = this.kafka.consumer({
      groupId: this.group,
      readUncommitted: false,
      sessionTimeout: 10_000,
    })
    this.consumers.push(consumer)
    await consumer.connect()
    await consumer.subscribe({ topic: this.topic, fromBeginning: true })

    await consumer.run({
      autoCommit: false,
      eachMessage: async ({ topic, partition, message }) => {
        this.counts.consumed += 1
        const win = JSON.parse(message.value?.toString() ?? '{}') as { buyerId?: string; seq?: number }
        if (win.buyerId === undefined) return
        const done = await this.gate.record({
          buyerId: win.buyerId,
          seq: Number(win.seq ?? 0),
          topic,
          partition,
          offset: Number(message.offset),
        })
        if (done === 'written') this.counts.written += 1
        else if (done === 'replayed') this.counts.replayed += 1
        else if (done === 'duplicate-buyer') this.counts.duplicateBuyers += 1
        else this.counts.refusedByDatabase += 1
        // kafkajs runs one partition in order, so every record up to this one is
        // already in Postgres. A commit every 25 records is the same claim as a
        // commit every record, and it costs 1 round trip instead of 25.
        const seen = (this.sinceCommit.get(partition) ?? 0) + 1
        if (seen < COMMIT_EVERY) {
          this.sinceCommit.set(partition, seen)
          return
        }
        this.sinceCommit.set(partition, 0)
        await consumer.commitOffsets([
          { topic, partition, offset: String(Number(message.offset) + 1) },
        ])
      },
    })
  }
}
