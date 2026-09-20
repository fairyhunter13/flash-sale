import type { Pool } from 'pg'
import { poolFor, writeCampaign } from './setup/db.ts'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, inject, it } from 'vitest'
import type { Config } from '../src/config.ts'
import { buildApp, type App } from '../src/server.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2036-06-01T00:00:00Z')

function config(): Config {
  return Object.freeze({
    databaseUrl: 'postgres://unused/unused',
    dbPoolMax: 4,
    redisUrl: inject('redisUrl'),
    kafkaBrokers: [inject('kafkaBroker')],
    queueWorkers: 1,
    port: 0,
    host: '127.0.0.1',
  })
}

/** Reads one `event: sale` block at a time out of the raw stream. */
async function* saleEvents(body: ReadableStream<Uint8Array>): AsyncGenerator<SaleEvent> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffered = ''
  while (true) {
    const { done, value } = await reader.read()
    if (done) return
    buffered += decoder.decode(value, { stream: true })
    let cut = buffered.indexOf('\n\n')
    while (cut !== -1) {
      const block = buffered.slice(0, cut)
      buffered = buffered.slice(cut + 2)
      const line = block.split('\n').find((it) => it.startsWith('data: '))
      if (line !== undefined) yield JSON.parse(line.slice(6)) as SaleEvent
      cut = buffered.indexOf('\n\n')
    }
  }
}

type SaleEvent = { state: string; stockLeft: number }

let pool: Pool
let app: App
let base: string

beforeAll(async () => {
  pool = await poolFor(inject('databaseUrl'), 't_stream')
})

afterAll(async () => {
  await pool.end()
})

let run = 0

beforeEach(async () => {
  await pool.query('DELETE FROM orders')
  await pool.query('DELETE FROM queue_offsets')
  await writeCampaign(pool, { stock: 5, startMs: START, endMs: END })
  // A 20 ms tick keeps the test short. The server runs at the 250 ms default.
  run += 1
  app = await buildApp(config(), pool, 20, `t_stream_${run}`)
  base = await app.fastify.listen({ port: 0, host: '127.0.0.1' })
})

afterEach(async () => {
  await app.fastify.close()
})

describe('the stream', () => {
  it('the stream pushes a change and closes cleanly', async () => {
    const stop = new AbortController()
    const answer = await fetch(`${base}/api/sale/stream`, { signal: stop.signal })

    expect(answer.status).toBe(200)
    expect(answer.headers.get('content-type')).toBe('text/event-stream')
    expect(answer.body).not.toBeNull()

    const events = saleEvents(answer.body as ReadableStream<Uint8Array>)

    const first = await events.next()
    expect(first.value).toMatchObject({ state: 'open', stockLeft: 5 })

    await app.pipeline.reserve('buyer-a', START + 60_000)

    // A tick can fire between the first read and the purchase, so the next
    // block is not always the changed one. The test reads until the count
    // moves, and it fails on the limit rather than on the first block.
    const second = await eventWhere(events, (one) => one.stockLeft === 4)
    expect(second).toMatchObject({ state: 'open', stockLeft: 4 })

    stop.abort()
    await waitFor(() => app.ticker.openConnections === 0)
    expect(app.ticker.openConnections).toBe(0)
  })

  it('a second page gets the state at once, and one tick serves both', async () => {
    const stop = new AbortController()
    const [one, two] = await Promise.all([
      fetch(`${base}/api/sale/stream`, { signal: stop.signal }),
      fetch(`${base}/api/sale/stream`, { signal: stop.signal }),
    ])

    const firstOfOne = await saleEvents(one.body as ReadableStream<Uint8Array>).next()
    const firstOfTwo = await saleEvents(two.body as ReadableStream<Uint8Array>).next()

    expect(firstOfOne.value).toMatchObject({ stockLeft: 5 })
    expect(firstOfTwo.value).toMatchObject({ stockLeft: 5 })
    expect(app.ticker.openConnections).toBe(2)

    stop.abort()
    await waitFor(() => app.ticker.openConnections === 0)
  })
})

/** The first event that answers the question, out of the next 20. */
async function eventWhere(
  events: AsyncGenerator<SaleEvent>,
  wanted: (one: SaleEvent) => boolean,
): Promise<SaleEvent> {
  for (let read = 0; read < 20; read += 1) {
    const next = await events.next()
    if (next.done) break
    if (wanted(next.value)) return next.value
  }
  throw new Error('the stream never sent the event this test waits for')
}

async function waitFor(ready: () => boolean, limitMs = 2000): Promise<void> {
  const until = Date.now() + limitMs
  while (!ready()) {
    if (Date.now() > until) throw new Error('the condition did not hold in time')
    await new Promise((done) => setTimeout(done, 10))
  }
}
