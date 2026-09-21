import { Kafka, Partitioners, type Admin, type Consumer, type Producer } from 'kafkajs'
import { createClient, type RedisClientType } from 'redis'
import type { Gate, SaleNumbers } from '../gate/gate.ts'
import { saleState, type Outcome } from '../gate/status.ts'
import { REHYDRATE, RESERVE } from './scripts.ts'

export const TOPIC = 'sale.wins'
export const PARTITIONS = 4
const GROUP = 'sale-writers'

// The only five keys in Redis.
export const SOLD_KEY = 'sale:sold'
export const BUYERS_KEY = 'sale:buyers'
export const OUTBOX_KEY = 'sale:outbox'

/**
 * Every place the script issued that Postgres does not hold yet. The outbox empties
 * when Kafka takes the win, and this hash empties when the order row is committed.
 * So a rebuild that reads Postgres and this hash sees every place, Kafka included.
 */
export const ISSUED_KEY = 'sale:issued'

/** Present means Redis still holds the sale it was given. Absent means a rebuild is due. */
export const LIVE_KEY = 'sale:live'

/** A win that never reached Kafka waits here. The sweep is the only thing that finds it. */
const SWEEP_MS = 250

/** How often a worker saves its place in the queue. Anything past the last save replays. */
const COMMIT_MS = 1_000

export type PipelineOptions = {
  readonly redisUrl: string
  readonly kafkaBrokers: readonly string[]
  readonly workers: number
  readonly sale: SaleNumbers
  readonly gate: Gate
  /**
   * A test sets it, so two test files that share one Redis never read each other's sale.
   * A second campaign would use the same field.
   */
  readonly namespace?: string
}

export type PipelineCounts = {
  produced: number
  consumed: number
  written: number
  replayed: number
  alreadyRecorded: number
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
  private stock: number
  /** The sweep refreshes it, so `npm run sale:window` takes effect with no restart. */
  private window: { startMs: number; endMs: number }
  readonly topic: string
  private readonly group: string
  private readonly soldKey: string
  private readonly buyersKey: string
  private readonly outboxKey: string
  private readonly liveKey: string
  private readonly issuedKey: string
  private readonly shas = new Map<string, string>()
  private sweep: NodeJS.Timeout | undefined
  private sweeping = false
  private restoring: Promise<void> | undefined
  private readonly commits: { timer: NodeJS.Timeout; flush: () => Promise<void> }[] = []
  readonly counts: PipelineCounts = {
    produced: 0,
    consumed: 0,
    written: 0,
    replayed: 0,
    alreadyRecorded: 0,
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
    this.liveKey = `${LIVE_KEY}${tail}`
    this.issuedKey = `${ISSUED_KEY}${tail}`
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
    // `createTopics` makes the broker log an error when the topic is already
    // there, so every restart printed one. Reading the list first keeps it quiet.
    const topics = await pipeline.admin.listTopics()
    if (!topics.includes(pipeline.topic)) {
      await pipeline.admin.createTopics({
        topics: [{ topic: pipeline.topic, numPartitions: PARTITIONS, replicationFactor: 1 }],
      })
    }
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

    let [outcome, seq, why] = await this.tryReserve(buyerId)

    // Redis lost the sale between two requests. Rebuild from the order rows, then ask
    // once more. A second `lost` means the rebuild failed, and a buyer gets no guess.
    if (outcome === 'lost') {
      await this.restore()
      ;[outcome, seq, why] = await this.tryReserve(buyerId)
      if (outcome === 'lost') throw new Error('Redis lost the sale, and the rebuild did not take.')
    }

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

  private async tryReserve(buyerId: string): Promise<[string, string, string]> {
    return (await this.runScript(
      RESERVE,
      [this.buyersKey, this.soldKey, this.outboxKey, this.liveKey, this.issuedKey],
      [buyerId, String(this.stock)],
    )) as [string, string, string]
  }

  /**
   * One rebuild at a time. Every caller that finds the sale gone waits on the same
   * promise, so a burst of requests never starts a second `rehydrate`.
   */
  private async restore(): Promise<void> {
    this.restoring ??= this.rehydrate()
      .then((done) => {
        console.warn(`Redis lost the sale. It was rebuilt from ${done.buyers} order rows in ${done.ms} ms.`)
      })
      .finally(() => {
        this.restoring = undefined
      })
    await this.restoring
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
   * someone already holds.
   *
   * A win still in Kafka has no order row, so Postgres alone reads behind the sale. The script
   * raises the floor with the places left in `sale:outbox`, and it never lowers `sale:sold`.
   */
  async rehydrate(): Promise<{ buyers: number; highestSeq: number; ms: number }> {
    const startedAt = Date.now()
    const winners = await this.gate.winners()
    const highest = winners.reduce((top, one) => Math.max(top, one.seq), 0)
    await this.runScript(
      REHYDRATE,
      [this.buyersKey, this.soldKey, this.outboxKey, this.liveKey, this.issuedKey],
      [String(highest), ...winners.map((one) => one.buyerId)],
    )
    return { buyers: winners.length, highestSeq: highest, ms: Date.now() - startedAt }
  }

  async close(): Promise<void> {
    if (this.sweep !== undefined) clearInterval(this.sweep)
    // The last flush costs one round trip and keeps the lag metric honest.
    for (const one of this.commits) {
      clearInterval(one.timer)
      await one.flush().catch(() => {})
    }
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
   * A second send is safe. The producer never writes the same record twice, and
   * `orders` refuses a repeated `user_id`.
   */
  private async sweepOutbox(): Promise<void> {
    if (this.sweeping) return
    this.sweeping = true
    try {
      await this.refreshCampaign()
      await this.rebuildIfBehind()
      const stranded = await this.redis.hGetAll(this.outboxKey)
      for (const [buyerId, seq] of Object.entries(stranded)) await this.deliver(buyerId, Number(seq))
    } catch (error) {
      console.error(`outbox sweep failed: ${(error as Error).message}`)
    } finally {
      this.sweeping = false
    }
  }

  /**
   * The sale window and the unit count are one row in Postgres, so an operator changes them
   * with `npm run sale:window` and no restart. `reserve` still reads an in-memory copy, and
   * this refresh is what keeps that copy at most one sweep old.
   */
  private async refreshCampaign(): Promise<void> {
    const sale = await this.gate.campaign()
    this.stock = sale.stock
    this.window = { startMs: sale.startMs, endMs: sale.endMs }
  }

  /**
   * Redis issues the place, and Postgres records it later. So `sale:sold` is never below
   * the highest place on disk while Redis is whole. Below it, Redis lost the counter, and
   * the next buyer would take a place that Postgres already holds.
   *
   * `RESERVE` refuses on the first request where either `sale:live` or `sale:sold` is gone,
   * so a deleted key never reaches a buyer. The check below catches the one case a deletion
   * cannot produce: a counter rewritten to a lower number. It costs one indexed
   * `MAX(seq)` read every 250 ms.
   */
  private async rebuildIfBehind(): Promise<void> {
    const sold = Number((await this.redis.get(this.soldKey)) ?? 0)
    if (sold >= (await this.gate.highestSeq())) return
    await this.restore()
  }

  /**
   * No `sale:live` flag means Redis never held this sale, or it lost it. Rebuild then,
   * and write the flag. A live Redis runs ahead of Postgres by what the queue holds.
   */
  private async restoreIfEmpty(): Promise<void> {
    if ((await this.redis.exists(this.liveKey)) === 1) return
    const restored = await this.rehydrate()
    if (restored.buyers > 0) {
      console.warn(`Redis held no sale, so it was rebuilt from ${restored.buyers} order rows in ${restored.ms} ms.`)
    }
  }

  /**
   * `autoCommit` is off because a timer commits an offset the database never wrote.
   * A timer of my own commits instead, and it commits only an offset that Postgres
   * already wrote. So it can never run ahead. An inline commit was correct too, and
   * it cost 4.9 s of drain time on 1,000 records.
   *
   * There is no `seek`. A replay from any earlier offset meets the resume-point
   * check in `Gate.record`, so the Kafka offset is a hint and costs only time.
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

    // One worker owns its own partitions, so each worker commits only its own.
    const applied = new Map<number, number>()

    /**
     * Only an offset that Postgres already wrote reaches this map. So a commit here
     * can never run ahead of Postgres, and a commit that never happens only replays.
     */
    const flush = async (): Promise<void> => {
      if (applied.size === 0) return
      const due = [...applied.entries()].map(([partition, offset]) => ({
        topic: this.topic,
        partition,
        offset: String(offset),
      }))
      applied.clear()
      await consumer
        .commitOffsets(due)
        .catch((error: Error) => console.error(`offset commit failed, the record replays: ${error.message}`))
    }

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
        else if (done === 'already-recorded') this.counts.alreadyRecorded += 1
        else this.counts.refusedByDatabase += 1
        // Postgres holds this record now. The timer below commits the number,
        // and no commit runs inside the handler.
        applied.set(partition, Number(message.offset) + 1)
        // The place is settled, so a rebuild no longer has to count it.
        await this.redis.hDel(this.issuedKey, win.buyerId).catch(() => {})
      },
    })

    const timer = setInterval(() => void flush(), COMMIT_MS)
    timer.unref()
    this.commits.push({ timer, flush })
  }
}
