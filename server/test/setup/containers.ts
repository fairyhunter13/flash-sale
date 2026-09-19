import { readFileSync } from 'node:fs'
import { PostgreSqlContainer } from '@testcontainers/postgresql'
import type { TestProject } from 'vitest/node'

declare module 'vitest' {
  interface ProvidedContext {
    databaseUrl: string
  }
}

/**
 * One Postgres for the whole run, on a port the host picks. So `npm test` is
 * the whole command, and it never collides with a database the box already
 * runs.
 */
export default async function setup(project: TestProject) {
  const schema = readFileSync(new URL('../../sql/schema.sql', import.meta.url), 'utf8')

  const postgres = await new PostgreSqlContainer('postgres:16-alpine')
    .withDatabase('flash')
    .withUsername('flash')
    .withPassword('flash')
    .start()

  await postgres.exec(['psql', '-U', 'flash', '-d', 'flash', '-c', schema])

  project.provide('databaseUrl', postgres.getConnectionUri())

  return async () => {
    await postgres.stop()
  }
}
