import { Kafka, Partitioners, type Admin, type Consumer, type Producer } from 'kafkajs'
import { createClient, type RedisClientType } from 'redis'
import type { Gate, SaleNumbers } from '../gate/gate.ts'
import { saleState, type Outcome } from '../gate/status.ts'

export const TOPIC = 'sale.wins'
export const PARTITIONS = 4
const GROUP = 'sale-writers'

// The only two keys in Redis.
export const SOLD_KEY = 'sale:sold'
export const BUYERS_KEY = 'sale:buyers'

export type PipelineOptions = {
  readonly redisUrl: string
  readonly kafkaBrokers: readonly string[]
  readonly workers: number
  readonly sale: SaleNumbers
  readonly gate: Gate
  /**
   * A suffix on the Redis keys, the topic and the group. A test sets it, so two
   * test files that share one Redis never read each other's sale. A second
   * campaign would use the same field. See `docs/design-experiments.md`.
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
 * The decision path. Redis answers the buyer, Kafka carries the win, and
 * `Gate.record` writes it down. The three stores never write each other, so
 * each one fails on its own.
 *
 * Kafka keeps order inside one partition only, and several workers write at
 * once. So the number `INCR` returns travels in the record into `orders.seq`,
 * and `ORDER BY seq` reads the arrival order whatever order the rows landed in.
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
      // The broker drops a record it already holds, so a retried send after a
      // timeout writes one record and not two.
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
    await pipeline.restoreIfEmpty()
    for (let id = 0; id < Math.max(1, options.workers); id += 1) await pipeline.startWorker()
    return pipeline
  }

  /**
   * Answers one buyer, and never touches Postgres.
   *
   * The `GET` is a fast path and never the decision. `sale:sold` only goes up,
   * so a buyer refused there writes nothing. `INCR` past the stock is what
   * refuses a buyer, so a stale read costs one call and never a wrong answer.
   */
  async reserve(buyerId: string, nowMs: number = Date.now()): Promise<Outcome> {
    const state = saleState(nowMs, 1, this.window)
    if (state === 'pending') return 'not-open'
    if (state === 'closed') return 'over'

    const sold = Number((await this.redis.get(this.soldKey)) ?? 0)
    if (sold >= this.stock) {
      this.counts.refusedByFastPath += 1
      return 'sold-out'
    }

    const fresh = await this.redis.sAdd(this.buyersKey, buyerId)
    if (fresh === 0) return 'already-bought'

    const seq = await this.redis.incr(this.soldKey)
    if (seq > this.stock) {
      // Left in the set, this buyer would read `already-bought` on a retry for
      // a unit they never won, and the set would grow with the traffic.
      await this.redis.sRem(this.buyersKey, buyerId)
      this.counts.losersRemoved += 1
      return 'sold-out'
    }

    await this.producer.send({
      // The key is the buyer, so one buyer keeps one partition and keeps order.
      topic: this.topic,
      messages: [{ key: buyerId, value: JSON.stringify({ buyerId, seq }) }],
    })
    this.counts.produced += 1
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
      // A replay and a duplicate buyer are one record twice, so neither counts.
      // Counted, this returned true at 49 of 50 rows on a 4-worker run.
      const done = this.counts.written + this.counts.refusedByDatabase
      if (done >= this.counts.produced) return true
      await new Promise((ready) => setTimeout(ready, 25))
    }
    return false
  }

  /**
   * Rebuilds the hot state from the database. The counter comes from
   * `max(seq)`, never the row count, because a count would hand the next buyer
   * a place an earlier buyer holds. A win still in Kafka has no order row, so
   * the rebuild is exact only after the queue drains.
   */
  async rehydrate(): Promise<{ buyers: number; highestSeq: number; ms: number }> {
    const startedAt = Date.now()
    const winners = await this.gate.winners()
    const highest = winners.reduce((top, one) => Math.max(top, one.seq), 0)
    await this.redis.del(this.buyersKey)
    if (winners.length > 0) await this.redis.sAdd(this.buyersKey, winners.map((one) => one.buyerId))
    await this.redis.set(this.soldKey, String(highest))
    return { buyers: winners.length, highestSeq: highest, ms: Date.now() - startedAt }
  }

  async close(): Promise<void> {
    for (const consumer of this.consumers) await consumer.disconnect().catch(() => {})
    await this.producer.disconnect().catch(() => {})
    await this.admin.disconnect().catch(() => {})
    await this.redis.quit().catch(() => {})
  }

  /**
   * No counter in Redis means Redis was lost, so rebuild. A counter is left
   * alone, because a live Redis runs ahead of Postgres by what the queue holds.
   */
  private async restoreIfEmpty(): Promise<void> {
    if ((await this.redis.exists(this.soldKey)) === 1) return
    const restored = await this.rehydrate()
    if (restored.buyers > 0) {
      console.warn(`Redis held no sale, so it was rebuilt from ${restored.buyers} order rows in ${restored.ms} ms.`)
    }
  }

  /**
   * One consumer, with Postgres as the only offset store. `autoCommit` is off:
   * Kafka commits on a timer, so a dead worker can leave an offset past a row
   * it never wrote. Measured on this design, 201 of 1,000 rows never landed.
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

    // A group join can arrive before `run` returns, and `seek` refuses one
    // then. So each seek waits for this promise.
    let started: () => void = () => {}
    const running = new Promise<void>((ready) => {
      started = ready
    })

    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      const mine = payload.memberAssignment[this.topic] ?? []
      void (async () => {
        try {
          await running
          for (const partition of mine) {
            const next = await this.gate.offsetOf(this.topic, partition)
            consumer.seek({ topic: this.topic, partition, offset: String(next) })
          }
        } catch (error) {
          // A failed seek on a closing consumer costs nothing. The next owner
          // seeks to the same row.
          console.error(`the worker could not seek: ${(error as Error).message}`)
        }
      })()
    })

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
      },
    })
    started()
  }
}
