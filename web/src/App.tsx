import { useEffect, useState } from 'react'
import type { Api, Outcome, SaleView } from './api.ts'

const BUYER_KEY = 'flash-sale.buyerId'

const SALE_TEXT: Record<SaleView['state'], string> = {
  pending: 'The sale has not opened yet.',
  open: 'The sale is open.',
  'sold-out': 'Every unit is gone.',
  closed: 'The sale is closed.',
}

/** One sentence for each of the 5 outcomes the server can answer. */
const OUTCOME_TEXT: Record<Outcome, string> = {
  won: 'You got one. The unit is yours.',
  'already-bought': 'You already hold a unit. Nobody takes two.',
  'sold-out': 'Every unit is gone. You did not get one.',
  'not-open': 'The sale has not opened yet.',
  over: 'The sale is over.',
}

function readStoredBuyerId(): string {
  try {
    return localStorage.getItem(BUYER_KEY) ?? ''
  } catch {
    // A browser that refuses storage still runs the page.
    return ''
  }
}

function storeBuyerId(userId: string): void {
  try {
    localStorage.setItem(BUYER_KEY, userId)
  } catch {
    /* nothing to do */
  }
}

export function App({ api }: { api: Api }): React.ReactElement {
  const [sale, setSale] = useState<SaleView | undefined>(undefined)
  const [saleFault, setSaleFault] = useState(false)
  const [buyerId, setBuyerId] = useState(readStoredBuyerId)
  const [message, setMessage] = useState('')
  const [busy, setBusy] = useState(false)

  useEffect(() => {
    return api.watchSale(
      (next) => {
        setSale(next)
        setSaleFault(false)
      },
      () => setSaleFault(true),
    )
  }, [api])

  // A refresh must not lose the result of an attempt that already succeeded.
  useEffect(() => {
    const stored = readStoredBuyerId()
    if (stored === '') return
    let live = true
    void api.readHeld(stored).then((held) => {
      if (!live) return
      if (held === 'unknown') setMessage('The record cannot be reached, so what you hold is unknown.')
      else if (held.held) setMessage('You hold a unit.')
    })
    return () => {
      live = false
    }
  }, [api])

  const trimmed = buyerId.trim()
  const canBuy = trimmed !== '' && sale?.state === 'open' && !busy

  async function buy(): Promise<void> {
    setBusy(true)
    storeBuyerId(trimmed)
    try {
      setMessage(OUTCOME_TEXT[await api.purchase(trimmed)])
    } catch {
      setMessage('The server did not answer. Nothing was taken.')
    } finally {
      setBusy(false)
    }
  }

  return (
    <main>
      <h1>Flash sale</h1>

      <section aria-label="sale state">
        {saleFault && <p role="status">The sale cannot be read right now.</p>}
        {sale === undefined && !saleFault && <p role="status">Reading the sale...</p>}
        {sale !== undefined && (
          <p role="status">
            {SALE_TEXT[sale.state]} <strong>{sale.stockLeft}</strong> left.
          </p>
        )}
      </section>

      <form
        onSubmit={(event) => {
          event.preventDefault()
          if (canBuy) void buy()
        }}
      >
        <label htmlFor="buyerId">Your name or email</label>
        <input
          id="buyerId"
          value={buyerId}
          autoComplete="off"
          onChange={(event) => setBuyerId(event.target.value)}
        />
        <button type="submit" disabled={!canBuy}>
          Buy Now
        </button>
      </form>

      {message !== '' && <p role="alert">{message}</p>}
    </main>
  )
}
