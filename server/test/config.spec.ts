import { describe, expect, it } from 'vitest'
import { ConfigError, readConfig } from '../src/config.ts'

const complete = {
  DATABASE_URL: 'postgres://flash:flash@localhost:5432/flash',
  DB_POOL_MAX: '20',
  REDIS_URL: 'redis://localhost:6379',
  KAFKA_BROKERS: 'localhost:9092',
  QUEUE_WORKERS: '4',
  PORT: '3000',
  HOST: '0.0.0.0',
}

describe('the configuration', () => {
  it('a missing DATABASE_URL stops the boot', () => {
    const { DATABASE_URL: _dropped, ...without } = complete
    expect(() => readConfig(without)).toThrow(ConfigError)
    try {
      readConfig(without)
    } catch (error) {
      expect((error as ConfigError).problems).toEqual([
        'DATABASE_URL is not set. It must be an address, for example postgres://flash:flash@localhost:5432/flash.',
      ])
    }
  })

  it('the sale is not read here, because the migrations own it', () => {
    expect(readConfig(complete)).not.toHaveProperty('stock')
  })

  it('a full environment reads every value', () => {
    const config = readConfig(complete)
    expect(config.databaseUrl).toBe('postgres://flash:flash@localhost:5432/flash')
    expect(config.dbPoolMax).toBe(20)
    expect(config.redisUrl).toBe('redis://localhost:6379')
    expect(config.kafkaBrokers).toEqual(['localhost:9092'])
    expect(config.queueWorkers).toBe(4)
    expect(config.port).toBe(3000)
    expect(config.host).toBe('0.0.0.0')
  })

  it('KAFKA_BROKERS reads a list, and drops an empty entry', () => {
    const many = { ...complete, KAFKA_BROKERS: 'one:9092, two:9092 ,,three:9092' }
    expect(readConfig(many).kafkaBrokers).toEqual(['one:9092', 'two:9092', 'three:9092'])
  })

  it('a KAFKA_BROKERS of commas alone is refused', () => {
    expect(() => readConfig({ ...complete, KAFKA_BROKERS: ' , , ' })).toThrow(/one or more host:port/)
  })

  it('a REDIS_URL that is not a redis address is refused', () => {
    expect(() => readConfig({ ...complete, REDIS_URL: 'postgres://localhost:5432/flash' })).toThrow(
      /It must start with redis/,
    )
  })

  it('every problem is reported at once', () => {
    try {
      readConfig({})
      expect.unreachable('an empty environment must stop the boot')
    } catch (error) {
      expect((error as ConfigError).problems).toHaveLength(6)
    }
  })

  it('a DATABASE_URL that is not a postgres address is refused', () => {
    expect(() => readConfig({ ...complete, DATABASE_URL: 'mysql://localhost:3306/flash' })).toThrow(
      /It must start with postgres/,
    )
  })

  it('a DB_POOL_MAX of zero is refused, because the pool would never open', () => {
    expect(() => readConfig({ ...complete, DB_POOL_MAX: '0' })).toThrow(/DB_POOL_MAX is 0/)
  })
})
