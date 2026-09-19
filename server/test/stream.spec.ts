import { Redis } from 'ioredis'
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, inject, it } from 'vitest'
import type { Config } from '../src/config.ts'
import { buildApp, type App } from '../src/server.ts'

const START = Date.parse('2026-06-01T00:00:00Z')
const END = Date.parse('2036-06-01T00:00:00Z')

function config(redisUrl: string): Config {
  return Object.freeze({
    stock: 5,
    startMs: START,
    endMs: END,
    redisUrl,
    databaseUrl: 'postgres://unused/unused',
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

let redis: Redis
let app: App
let base: string

beforeAll(() => {
  redis = new Redis(inject('redisUrl'), { db: 3 })
})

afterAll(async () => {
  await redis.quit()
})

beforeEach(async () => {
  await redis.flushdb()
  // A 20 ms tick keeps the test short. The server runs at the 250 ms default.
  app = await buildApp(config(inject('redisUrl')), redis, 20)
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

    await app.gate.reserve('buyer-a', START + 60_000)

    const second = await events.next()
    expect(second.value).toMatchObject({ state: 'open', stockLeft: 4 })

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

async function waitFor(ready: () => boolean, limitMs = 2000): Promise<void> {
  const until = Date.now() + limitMs
  while (!ready()) {
    if (Date.now() > until) throw new Error('the condition did not hold in time')
    await new Promise((done) => setTimeout(done, 10))
  }
}
