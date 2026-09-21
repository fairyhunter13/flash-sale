export type SaleState = 'pending' | 'open' | 'sold-out' | 'closed'

/** The answer one buyer receives. */
export type Outcome = 'won' | 'already-bought' | 'sold-out' | 'not-open' | 'over'

export type SaleWindow = {
  readonly startMs: number
  readonly endMs: number
}

// One request has one "now", and a test needs no fake timer.
export function saleState(nowMs: number, left: number, window: SaleWindow): SaleState {
  if (nowMs < window.startMs) return 'pending'
  if (nowMs > window.endMs) return 'closed'
  // I check the end time before the count. An ended sale is closed even with units left.
  return left <= 0 ? 'sold-out' : 'open'
}
