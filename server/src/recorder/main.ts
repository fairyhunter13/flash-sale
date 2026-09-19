import { Redis } from 'ioredis'
import { Pool } from 'pg'
import { readConfig } from '../config.ts'
import { Recorder } from './consumer.ts'

const config = readConfig()
const redis = new Redis(config.redisUrl)
const pool = new Pool({ connectionString: config.databaseUrl })

const recorder = new Recorder(redis, pool)
await recorder.ensureGroup()
recorder.start()

for (const signal of ['SIGINT', 'SIGTERM'] as const) {
  process.once(signal, () => {
    // The loop finishes its batch first, so no acknowledged win is lost.
    void recorder
      .stop()
      .then(() => Promise.all([redis.quit(), pool.end()]))
      .then(() => process.exit(0))
  })
}
