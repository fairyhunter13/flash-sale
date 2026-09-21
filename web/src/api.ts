export type SaleState = 'pending' | 'open' | 'sold-out' | 'closed'

export type SaleView = {
  readonly state: SaleState
  readonly stockLeft: number
  readonly startsAt: string
  readonly endsAt: string
}

export type Outcome = 'won' | 'already-bought' | 'sold-out' | 'not-open' | 'over'

/** `unknown` is the answer when the record cannot be read. It is never `false`. */
export type Held = { readonly held: true; readonly at: string } | { readonly held: false } | 'unknown'

/**
 * Everything the page needs from the server. App takes it as a prop. A test
 * passes a plain object and never patches `fetch` onto the global scope.
 */
export type Api = {
  /** Pushes each sale state. Returns the function that closes the stream. */
  watchSale(onSale: (sale: SaleView) => void, onFault: () => void): () => void
  purchase(userId: string): Promise<Outcome>
  readHeld(userId: string): Promise<Held>
}

export const http: Api = {
  watchSale(onSale, onFault) {
    // Same origin, so the dev proxy and the built page both work with no base URL.
    const source = new EventSource('/api/sale/stream')
    source.addEventListener('sale', (event) => onSale(JSON.parse(event.data) as SaleView))
    source.addEventListener('error', () => onFault())
    return () => source.close()
  },

  async purchase(userId) {
    const answer = await fetch('/api/purchase', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ userId }),
    })
    if (!answer.ok) throw new Error(`the server answered ${answer.status}`)
    return ((await answer.json()) as { outcome: Outcome }).outcome
  },

  async readHeld(userId) {
    const answer = await fetch(`/api/purchase/${encodeURIComponent(userId)}`)
    // 503 means the record cannot be read. Telling the buyer they hold nothing
    // would be a different answer, and a wrong one.
    if (!answer.ok) return 'unknown'
    return (await answer.json()) as Held
  },
}
