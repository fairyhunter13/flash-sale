import { PostgreSqlContainer } from '@testcontainers/postgresql'
import { RedisContainer } from '@testcontainers/redis'
import { GenericContainer, Wait } from 'testcontainers'
import type { TestProject } from 'vitest/node'

declare module 'vitest' {
  interface ProvidedContext {
    databaseUrl: string
    redisUrl: string
    kafkaBroker: string
  }
}

/**
 * The broker tells a client its own address, from `advertised.listeners`. That
 * address must be right at start. The port cannot be a random one.
 */
const KAFKA_HOST_PORT = 19_092

/**
 * One Postgres, one Redis and one Kafka for the whole run.
 * `npm test` is the whole command. The three start together. Kafka takes about 20 seconds.
 */
export default async function setup(project: TestProject) {
  const [postgres, redis, kafka] = await Promise.all([
    new PostgreSqlContainer('postgres:16-alpine')
      .withDatabase('flash')
      .withUsername('flash')
      .withPassword('flash')
      .start(),
    new RedisContainer('redis:7-alpine').start(),
    // The same image and the same KRaft settings as docker-compose.yml.
    // Tests and the running server meet one broker version.
    new GenericContainer('apache/kafka:4.0.0')
      .withExposedPorts({ container: 9092, host: KAFKA_HOST_PORT })
      .withEnvironment({
        KAFKA_NODE_ID: '1',
        KAFKA_PROCESS_ROLES: 'broker,controller',
        KAFKA_LISTENERS: 'PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093',
        KAFKA_ADVERTISED_LISTENERS: `PLAINTEXT://127.0.0.1:${KAFKA_HOST_PORT}`,
        KAFKA_CONTROLLER_QUORUM_VOTERS: '1@localhost:9093',
        KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER',
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT',
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: '1',
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: '1',
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: '1',
        KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: '0',
        CLUSTER_ID: 'flash-sale-test-cluster',
      })
      .withWaitStrategy(Wait.forLogMessage(/Kafka Server started/))
      .withStartupTimeout(180_000)
      .start(),
  ])

  // No schema here. Each test file migrates its own Postgres schema in `poolFor`.
  project.provide('databaseUrl', postgres.getConnectionUri())
  project.provide('redisUrl', redis.getConnectionUrl())
  project.provide('kafkaBroker', `127.0.0.1:${KAFKA_HOST_PORT}`)

  return async () => {
    await Promise.all([postgres.stop(), redis.stop(), kafka.stop()])
  }
}
