import { describe, expect, it } from 'vitest'
import { saleState } from '../../src/gate/status.ts'

const WINDOW = {
  startMs: Date.parse('2026-06-01T00:00:00Z'),
  endMs: Date.parse('2026-06-02T00:00:00Z'),
}
const DURING = WINDOW.startMs + 60_000

describe('the sale state', () => {
  it('the state follows the clock and the count', () => {
    expect(saleState(WINDOW.startMs - 1, 1000, WINDOW)).toBe('pending')
    expect(saleState(DURING, 1000, WINDOW)).toBe('open')
    expect(saleState(DURING, 0, WINDOW)).toBe('sold-out')
    expect(saleState(WINDOW.endMs + 1, 1000, WINDOW)).toBe('closed')
  })

  it('the start instant itself is open', () => {
    expect(saleState(WINDOW.startMs, 1000, WINDOW)).toBe('open')
  })

  it('the end instant itself is still open', () => {
    expect(saleState(WINDOW.endMs, 1000, WINDOW)).toBe('open')
  })

  it('a sale that ended with units left is closed and not open', () => {
    expect(saleState(WINDOW.endMs + 1, 500, WINDOW)).toBe('closed')
  })

  it('a sale that ended with no units left is closed and not sold-out', () => {
    expect(saleState(WINDOW.endMs + 1, 0, WINDOW)).toBe('closed')
  })

  it('a sold-out sale before the start is still pending', () => {
    expect(saleState(WINDOW.startMs - 1, 0, WINDOW)).toBe('pending')
  })
})
