export type SaleState = 'pending' | 'open' | 'sold-out' | 'closed'

export type SaleWindow = {
  readonly startMs: number
  readonly endMs: number
}

// The clock arrives as an argument, exactly as it does in reserve.lua, so one
// request has one "now" and a test needs no fake timer.
export function saleState(nowMs: number, left: number, window: SaleWindow): SaleState {
  if (nowMs < window.startMs) return 'pending'
  if (nowMs > window.endMs) return 'closed'
  // The end time outranks the count, because a sale that ended is closed even
  // with units left. Inside the window, no unit left is sold out.
  return left <= 0 ? 'sold-out' : 'open'
}
