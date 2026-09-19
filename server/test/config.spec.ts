import { describe, expect, it } from 'vitest'
import { ConfigError, readConfig } from '../src/config.ts'

const complete = {
  SALE_STOCK: '1000',
  SALE_START: '2026-01-01T00:00:00Z',
  SALE_END: '2036-01-01T00:00:00Z',
  REDIS_URL: 'redis://localhost:6379',
  DATABASE_URL: 'postgres://flash:flash@localhost:5432/flash',
  PORT: '3000',
  HOST: '0.0.0.0',
}

describe('the configuration', () => {
  it('a missing SALE_STOCK stops the boot', () => {
    const { SALE_STOCK: _dropped, ...without } = complete
    expect(() => readConfig(without)).toThrow(ConfigError)
    try {
      readConfig(without)
    } catch (error) {
      expect((error as ConfigError).problems).toEqual(['SALE_STOCK is not set. It must be a whole number above 0, for example 1000.'])
    }
  })

  it('a SALE_END before SALE_START stops the boot', () => {
    const backwards = { ...complete, SALE_END: '2020-01-01T00:00:00Z' }
    expect(() => readConfig(backwards)).toThrow(/It must be after SALE_START/)
  })

  it('a full environment reads every value', () => {
    const config = readConfig(complete)
    expect(config.stock).toBe(1000)
    expect(config.startMs).toBe(Date.parse('2026-01-01T00:00:00Z'))
    expect(config.endMs).toBe(Date.parse('2036-01-01T00:00:00Z'))
    expect(config.redisUrl).toBe('redis://localhost:6379')
    expect(config.port).toBe(3000)
    expect(config.host).toBe('0.0.0.0')
  })

  it('every problem is reported at once', () => {
    try {
      readConfig({})
      expect.unreachable('an empty environment must stop the boot')
    } catch (error) {
      expect((error as ConfigError).problems).toHaveLength(6)
    }
  })

  it('a stock of zero is refused, because a sale of nothing is not a sale', () => {
    expect(() => readConfig({ ...complete, SALE_STOCK: '0' })).toThrow(/SALE_STOCK is 0/)
  })

  it('a REDIS_URL that is not a redis address is refused', () => {
    expect(() => readConfig({ ...complete, REDIS_URL: 'http://localhost:6379' })).toThrow(/It must start with redis/)
  })
})
