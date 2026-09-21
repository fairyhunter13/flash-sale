export type SaleState = 'pending' | 'open' | 'sold-out' | 'closed'

/** The answer one buyer receives. */
export type Outcome = 'won' | 'already-bought' | 'sold-out' | 'not-open' | 'over'

export type SaleWindow = {
  readonly startMs: number
  readonly endMs: number
}

// The clock arrives as an argument. One request has one "now",
// and a test needs no fake timer.
export function saleState(nowMs: number, left: number, window: SaleWindow): SaleState {
  if (nowMs < window.startMs) return 'pending'
  if (nowMs > window.endMs) return 'closed'
  // A sale that ended is closed even with units left. So the end time
  // outranks the count. Inside the window, no unit left is sold out.
  return left <= 0 ? 'sold-out' : 'open'
}
