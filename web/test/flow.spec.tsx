import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { App } from '../src/App.tsx'
import { http } from '../src/api.ts'

/**
 * `App.spec.tsx` passes a plain object in, so it never reaches `api.ts`. This
 * file renders the page with the real `http` client and stubs the two browser
 * APIs it calls. The route names, the request body and the 503 path are covered
 * here and nowhere else.
 */
class FakeSource {
  static last: FakeSource | undefined
  readonly url: string
  closed = false
  private readonly listeners = new Map<string, (event: { data: string }) => void>()

  constructor(url: string) {
    this.url = url
    FakeSource.last = this
  }

  addEventListener(type: string, handler: (event: { data: string }) => void): void {
    this.listeners.set(type, handler)
  }

  close(): void {
    this.closed = true
  }

  emit(type: string, data = ''): void {
    act(() => this.listeners.get(type)?.({ data }))
  }
}

const OPEN = {
  state: 'open',
  stockLeft: 42,
  startsAt: '2026-01-01T00:00:00.000Z',
  endsAt: '2036-01-01T00:00:00.000Z',
}

function answer(body: unknown, ok = true, status = 200): Promise<Response> {
  return Promise.resolve({ ok, status, json: async () => body } as Response)
}

let fetchMock: ReturnType<typeof vi.fn>

beforeEach(() => {
  localStorage.clear()
  FakeSource.last = undefined
  fetchMock = vi.fn(() => answer({ held: false }))
  vi.stubGlobal('fetch', fetchMock)
  vi.stubGlobal('EventSource', FakeSource)
})

afterEach(() => {
  vi.unstubAllGlobals()
  localStorage.clear()
})

describe('the page over the real client', () => {
  it('subscribes to the stream route and renders the state it pushes', async () => {
    render(<App api={http} />)

    expect(FakeSource.last?.url).toBe('/api/sale/stream')

    FakeSource.last?.emit('sale', JSON.stringify(OPEN))

    expect(await screen.findByRole('status')).toHaveTextContent('The sale is open. 42 left.')
  })

  it('a stream error says the sale cannot be read', async () => {
    render(<App api={http} />)

    FakeSource.last?.emit('error')

    expect(await screen.findByRole('status')).toHaveTextContent('The sale cannot be read right now.')
  })

  it('Buy Now posts the identifier to the purchase route', async () => {
    render(<App api={http} />)
    FakeSource.last?.emit('sale', JSON.stringify(OPEN))

    fetchMock.mockImplementation(() => answer({ outcome: 'won' }))
    await userEvent.type(screen.getByLabelText('Your name or email'), 'buyer-a')
    await userEvent.click(screen.getByRole('button', { name: 'Buy Now' }))

    expect(fetchMock).toHaveBeenCalledWith('/api/purchase', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ userId: 'buyer-a' }),
    })
    expect(await screen.findByRole('alert')).toHaveTextContent('You got one. The unit is yours.')
  })

  it('a 503 from the record route says unknown, and never that the buyer holds nothing', async () => {
    localStorage.setItem('flash-sale.buyerId', 'buyer-a')
    fetchMock.mockImplementation(() => answer({}, false, 503))

    render(<App api={http} />)

    expect(fetchMock).toHaveBeenCalledWith('/api/purchase/buyer-a')
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'The record cannot be reached, so what you hold is unknown.',
    )
  })

  it('a record that reads held tells the buyer on a reload', async () => {
    localStorage.setItem('flash-sale.buyerId', 'buyer-a')
    fetchMock.mockImplementation(() => answer({ held: true, at: '2026-01-01T00:00:00.000Z' }))

    render(<App api={http} />)

    expect(await screen.findByRole('alert')).toHaveTextContent('You hold a unit.')
  })

  it('leaving the page closes the stream', async () => {
    const view = render(<App api={http} />)
    const source = FakeSource.last

    view.unmount()

    await waitFor(() => expect(source?.closed).toBe(true))
  })
})
