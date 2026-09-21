import type { FastifyInstance, FastifyReply } from 'fastify'
import type { Gate } from '../gate/gate.ts'
import type { Pipeline } from '../queue/pipeline.ts'
import { readSale } from './sale.ts'

export const TICK_MS = 250

/**
 * I run one timer for the process, never one per page. 5,000 open pages then cost the same 4
 * reads a second as one. A tick writes only when the text changed.
 */
export class SaleTicker {
  private readonly clients = new Set<FastifyReply>()
  private timer: NodeJS.Timeout | undefined
  private last = ''

  private readonly gate: Gate
  private readonly pipeline: Pipeline
  private readonly tickMs: number

  constructor(gate: Gate, pipeline: Pipeline, tickMs: number = TICK_MS) {
    this.gate = gate
    this.pipeline = pipeline
    this.tickMs = tickMs
  }

  add(reply: FastifyReply): void {
    this.clients.add(reply)
    if (this.timer === undefined) {
      this.timer = setInterval(() => void this.tick(), this.tickMs)
      this.timer.unref()
    }
  }

  remove(reply: FastifyReply): void {
    this.clients.delete(reply)
    if (this.clients.size === 0) this.stop()
  }

  get openConnections(): number {
    return this.clients.size
  }

  /** Ends every open stream. A shutdown must not wait on a page that never leaves. */
  closeAll(): void {
    for (const reply of [...this.clients]) reply.raw.end()
    this.clients.clear()
    this.stop()
  }

  stop(): void {
    if (this.timer === undefined) return
    clearInterval(this.timer)
    this.timer = undefined
    // I forget the last body here. The next page to connect then gets the state at once.
    this.last = ''
  }

  /** Sends the current state to one page, whether it changed or not. */
  async sendNow(reply: FastifyReply): Promise<void> {
    write(reply, JSON.stringify(await readSale(this.gate, this.pipeline)))
  }

  private async tick(): Promise<void> {
    let body: string
    try {
      body = JSON.stringify(await readSale(this.gate, this.pipeline))
    } catch {
      // A store can stop answering. An open stream would read a stale count as
      // a live one. So I close every page.
      this.closeAll()
      return
    }
    if (body === this.last) return
    this.last = body
    for (const reply of this.clients) write(reply, body)
  }
}

function write(reply: FastifyReply, body: string): void {
  reply.raw.write(`event: sale\ndata: ${body}\n\n`)
}

export function registerStream(app: FastifyInstance, ticker: SaleTicker): void {
  app.get('/api/sale/stream', async (request, reply) => {
    // hijack hands the socket to this handler. Fastify never ends the
    // response, so the connection stays open for the life of the page.
    reply.hijack()
    reply.raw.writeHead(200, {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-transform',
      Connection: 'keep-alive',
    })
    ticker.add(reply)
    request.raw.on('close', () => ticker.remove(reply))
    try {
      await ticker.sendNow(reply)
    } catch {
      ticker.remove(reply)
      reply.raw.end()
    }
  })
}
