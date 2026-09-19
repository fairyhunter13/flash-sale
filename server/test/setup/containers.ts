import { readFileSync } from 'node:fs'
import { PostgreSqlContainer } from '@testcontainers/postgresql'
import { RedisContainer } from '@testcontainers/redis'
import type { TestProject } from 'vitest/node'

declare module 'vitest' {
  interface ProvidedContext {
    redisUrl: string
    databaseUrl: string
  }
}

/**
 * One Redis and one Postgres for the whole run, on a port the host picks. So
 * `npm test` is the whole command, and it never collides with an engine the
 * box already runs.
 */
export default async function setup(project: TestProject) {
  const schema = readFileSync(new URL('../../sql/schema.sql', import.meta.url), 'utf8')

  const [redis, postgres] = await Promise.all([
    new RedisContainer('redis:7-alpine').start(),
    new PostgreSqlContainer('postgres:16-alpine')
      .withDatabase('flash')
      .withUsername('flash')
      .withPassword('flash')
      .start(),
  ])

  await postgres.exec(['psql', '-U', 'flash', '-d', 'flash', '-c', schema])

  project.provide('redisUrl', redis.getConnectionUrl())
  project.provide('databaseUrl', postgres.getConnectionUri())

  return async () => {
    await Promise.all([redis.stop(), postgres.stop()])
  }
}
