import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { App } from '../src/App.tsx'
import type { Api, Held, Outcome, SaleView } from '../src/api.ts'

const OPEN: SaleView = {
  state: 'open',
  stockLeft: 42,
  startsAt: '2026-01-01T00:00:00.000Z',
  endsAt: '2036-01-01T00:00:00.000Z',
}

/**
 * App takes the whole server surface as a prop, so a test builds a plain
 * object here. Nothing is patched onto the global scope.
 */
function fakeApi(over: Partial<Api> = {}, sale: SaleView | undefined = OPEN): Api {
  return {
    watchSale(onSale) {
      if (sale !== undefined) onSale(sale)
      return () => {}
    },
    purchase: async () => 'won' as Outcome,
    readHeld: async () => ({ held: false }) as Held,
    ...over,
  }
}

beforeEach(() => localStorage.clear())
afterEach(() => localStorage.clear())

describe('the page', () => {
  it('the page shows the sale state and the stock', async () => {
    render(<App api={fakeApi()} />)

    const line = await screen.findByRole('status')
    expect(line).toHaveTextContent('The sale is open.')
    expect(line).toHaveTextContent('42 left.')
  })

  it('the page names each outcome', async () => {
    const person = userEvent.setup()
    const outcomes: Outcome[] = ['won', 'already-bought', 'sold-out', 'not-open', 'over']
    const seen: string[] = []

    for (const outcome of outcomes) {
      const view = render(<App api={fakeApi({ purchase: async () => outcome })} />)
      await person.type(screen.getByLabelText('Your name or email'), 'hafiz')
      await person.click(screen.getByRole('button', { name: 'Buy Now' }))

      const said = (await screen.findByRole('alert')).textContent ?? ''
      expect(said).not.toBe('')
      seen.push(said)
      view.unmount()
      localStorage.clear()
    }

    // 5 outcomes and 5 different sentences. A shared sentence would hide one
    // answer behind another.
    expect(new Set(seen).size).toBe(5)
  })

  it('an empty identifier cannot be sent', async () => {
    const person = userEvent.setup()
    const purchase = vi.fn(async () => 'won' as Outcome)
    render(<App api={fakeApi({ purchase })} />)

    const button = screen.getByRole('button', { name: 'Buy Now' })
    expect(button).toBeDisabled()

    // Spaces alone are not an identifier.
    await person.type(screen.getByLabelText('Your name or email'), '   ')
    expect(button).toBeDisabled()
    expect(purchase).not.toHaveBeenCalled()
  })

  it('the button is refused while the sale is not open', async () => {
    const person = userEvent.setup()
    render(<App api={fakeApi({}, { ...OPEN, state: 'pending', stockLeft: 1000 })} />)

    await person.type(screen.getByLabelText('Your name or email'), 'hafiz')
    expect(screen.getByRole('button', { name: 'Buy Now' })).toBeDisabled()
    expect(await screen.findByRole('status')).toHaveTextContent('The sale has not opened yet.')
  })

  it('a reload tells the buyer what they already hold', async () => {
    localStorage.setItem('flash-sale.buyerId', 'hafiz')
    render(<App api={fakeApi({ readHeld: async () => ({ held: true, at: OPEN.startsAt }) })} />)

    expect(await screen.findByRole('alert')).toHaveTextContent('You hold a unit.')
  })

  it('a record that cannot be read never says the buyer holds nothing', async () => {
    localStorage.setItem('flash-sale.buyerId', 'hafiz')
    render(<App api={fakeApi({ readHeld: async () => 'unknown' })} />)

    const said = await screen.findByRole('alert')
    expect(said).toHaveTextContent('unknown')
    expect(said.textContent).not.toContain('hold nothing')
  })

  it('a stream that fails says the sale cannot be read', async () => {
    render(
      <App
        api={fakeApi({
          watchSale(_onSale, onFault) {
            onFault()
            return () => {}
          },
        })}
      />,
    )

    await waitFor(() =>
      expect(screen.getByRole('status')).toHaveTextContent('The sale cannot be read right now.'),
    )
  })
})
