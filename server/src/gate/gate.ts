import { readFileSync } from 'node:fs'
import type { ClientContext, Redis, Result } from 'ioredis'

export const KEY = {
  stock: 'sale:stock',
  buyers: 'sale:buyers',
  wins: 'sale:wins',
  window: 'sale:window',
} as const

export type Outcome = 'won' | 'already-bought' | 'sold-out' | 'not-open' | 'over'

export type Reservation = {
  readonly outcome: Outcome
  /** The stream entry id when the buyer won, and the Lua reason otherwise. */
  readonly detail: string
}

const OUTCOME: Record<number, Outcome> = {
  1: 'won',
  0: 'already-bought',
  [-1]: 'sold-out',
  [-2]: 'not-open',
  [-3]: 'over',
}

const SCRIPT = readFileSync(new URL('./reserve.lua', import.meta.url), 'utf8')

// The generic must match the one ioredis declares, defaults included, or the
// merge is rejected and every built-in command disappears from the type.
declare module 'ioredis' {
  interface RedisCommander<Context extends ClientContext = { type: 'default' }> {
    reserve(
      stock: string,
      buyers: string,
      wins: string,
      saleWindow: string,
      buyerId: string,
      nowMs: string,
    ): Result<[number, string], Context>
  }
}

export type SaleNumbers = {
  readonly stock: number
  readonly startMs: number
  readonly endMs: number
}

export class Gate {
  constructor(private readonly redis: Redis) {
    // ioredis retries EVALSHA on NOSCRIPT by itself, so no SCRIPT LOAD is needed.
    redis.defineCommand('reserve', { numberOfKeys: 4, lua: SCRIPT })
  }

  /**
   * Writes the sale only when it is absent. A restart in the middle of a live
   * sale then keeps the real count, so units already sold stay sold.
   */
  async seed(sale: SaleNumbers): Promise<void> {
    await this.redis
      .multi()
      .set(KEY.stock, String(sale.stock), 'NX')
      .hsetnx(KEY.window, 'start_ms', String(sale.startMs))
      .hsetnx(KEY.window, 'end_ms', String(sale.endMs))
      .exec()
  }

  async reserve(buyerId: string, nowMs: number = Date.now()): Promise<Reservation> {
    const [code, detail] = await this.redis.reserve(
      KEY.stock,
      KEY.buyers,
      KEY.wins,
      KEY.window,
      buyerId,
      String(nowMs),
    )
    const outcome = OUTCOME[code]
    if (outcome === undefined) throw new Error(`reserve.lua returned an unknown code ${code}`)
    return { outcome, detail }
  }

  async stockLeft(): Promise<number> {
    const value = await this.redis.get(KEY.stock)
    return value === null ? 0 : Number(value)
  }
}
