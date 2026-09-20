/**
 * The addresses and the sizes, and never the sale itself. The unit count and
 * the window are rows, written by `server/sql/migrations/0002_campaign.sql`,
 * so one fact lives in one place.
 */
export type Config = {
  readonly databaseUrl: string
  /** The most Postgres connections this process ever opens. */
  readonly dbPoolMax: number
  readonly redisUrl: string
  /** One or more `host:port`, separated by commas. */
  readonly kafkaBrokers: readonly string[]
  /** Consumers in the group on this process. Kafka spreads the partitions over them. */
  readonly queueWorkers: number
  readonly port: number
  readonly host: string
}

export class ConfigError extends Error {
  readonly problems: readonly string[]

  constructor(problems: readonly string[]) {
    super(`The environment is not usable.\n  ${problems.join('\n  ')}`)
    this.name = 'ConfigError'
    this.problems = problems
  }
}

type Env = Record<string, string | undefined>

// Every problem is collected, because a boot that reports one missing variable
// at a time costs the reader one restart for each one.
class Reader {
  readonly problems: string[] = []
  private readonly env: Env

  // A field and an assignment, and never a constructor parameter property.
  // `node --experimental-strip-types` deletes types and rewrites nothing, so a
  // parameter property is a SyntaxError there. The same shape is used in every
  // class in this package.
  constructor(env: Env) {
    this.env = env
  }

  private raw(name: string): string | undefined {
    const value = this.env[name]?.trim()
    return value === '' ? undefined : value
  }

  wholeNumber(name: string, example: string): number {
    const value = this.raw(name)
    if (value === undefined) {
      this.problems.push(`${name} is not set. It must be a whole number above 0, for example ${example}.`)
      return 0
    }
    const parsed = Number(value)
    if (!Number.isSafeInteger(parsed) || parsed <= 0) {
      this.problems.push(`${name} is ${value}. It must be a whole number above 0, for example ${example}.`)
      return 0
    }
    return parsed
  }

  url(name: string, scheme: string, example: string): string {
    const value = this.raw(name)
    if (value === undefined) {
      this.problems.push(`${name} is not set. It must be an address, for example ${example}.`)
      return ''
    }
    let parsed: URL
    try {
      parsed = new URL(value)
    } catch {
      this.problems.push(`${name} is ${value}. It must be an address, for example ${example}.`)
      return ''
    }
    if (parsed.protocol !== `${scheme}:`) {
      this.problems.push(`${name} starts with ${parsed.protocol} It must start with ${scheme}, for example ${example}.`)
      return ''
    }
    return value
  }

  text(name: string, fallback: string): string {
    return this.raw(name) ?? fallback
  }

  /** A comma-separated list, with every empty entry dropped. */
  hosts(name: string, example: string): readonly string[] {
    const value = this.raw(name)
    if (value === undefined) {
      this.problems.push(`${name} is not set. It must be one or more host:port, for example ${example}.`)
      return []
    }
    const parts = value.split(',').map((one) => one.trim()).filter((one) => one !== '')
    if (parts.length === 0) {
      this.problems.push(`${name} is ${value}. It must be one or more host:port, for example ${example}.`)
      return []
    }
    return Object.freeze(parts)
  }
}

export function readConfig(env: Env = process.env): Config {
  const read = new Reader(env)

  const databaseUrl = read.url('DATABASE_URL', 'postgres', 'postgres://flash:flash@localhost:5432/flash')
  const dbPoolMax = read.wholeNumber('DB_POOL_MAX', '20')
  const redisUrl = read.url('REDIS_URL', 'redis', 'redis://localhost:6379')
  const kafkaBrokers = read.hosts('KAFKA_BROKERS', 'localhost:9092')
  const queueWorkers = read.wholeNumber('QUEUE_WORKERS', '4')
  const port = read.wholeNumber('PORT', '3000')
  const host = read.text('HOST', '0.0.0.0')

  if (read.problems.length > 0) throw new ConfigError(read.problems)

  return Object.freeze({
    databaseUrl,
    dbPoolMax,
    redisUrl,
    kafkaBrokers,
    queueWorkers,
    port,
    host,
  })
}
