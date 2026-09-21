# Architecture

Why the system has this shape, and what each choice costs.

![Buyers reach Fastify, which decides in Redis, carries the win over Kafka, and records the order in Postgres](../diagrams/architecture.svg)

## Redis decides the winner, and Postgres keeps the record

The sale asks two things of a store, and the two pull in opposite directions.

- The decision must be fast and atomic. Nothing runs between the read and the write.
- The record must survive a crash. The row must reach disk before the store acknowledges the write.

One store fits neither job well. So the design splits them. Redis decides the winner, and Postgres only records the result.

`Pipeline.reserve` in `server/src/queue/pipeline.ts` holds the decision. It runs one Redis script, `RESERVE` in `server/src/queue/scripts.ts`, then sends the win to Kafka. `Gate.record` in `server/src/gate/gate.ts` reads that topic and writes the order row.

A buyer who wins hears so at once. The order row reaches Postgres a few milliseconds later. Over 1,000 wins that gap ran from 566 ms to 3,285 ms. `GET /api/purchase/:userId` reads Postgres, so it lags by the same amount.

**What the split buys.** Each of the three stores fails on its own. I injected 2 seconds of delay into the Postgres link, and the sale kept answering buyers at 1,740 decisions a second with all 1,000 wins intact. A design that writes the order inside the request instead committed 0 wins when Postgres stopped, and the buyer never heard an answer.

**What the split costs.**

1. Two more containers are two more ways to fail. Where Redis is unreachable, the sale refuses every buyer.
2. Two stores disagree for a short window, named above.
3. Inside that window, durability rests on one Redis setting. Redis writes each change to an append-only file, and `appendfsync everysec` flushes that file once a second. So a power cut can lose up to 1 second of acknowledged wins. A `kill -9` of the container lost 0 of 15,619 acknowledged writes, because `kill -9` ends the process and the file survives it.
4. A gap between two stores needs a repair path. The worker writes its resume point in the same transaction as the order row.

A faster design exists. A single writer that holds the count in one process runs 2.5 times faster. It cannot survive a second process, and a fence that makes it safe costs a third of that speed. It still lost 477 acknowledged wins when I killed the process. Speed that loses an acknowledged win is not a trade this sale can take.

## The record store is Postgres and not SQLite

The sale writes 1,000 rows through 1 writer, and SQLite handles that with no container.

I use Postgres anyway, because a real sale runs on Postgres and the design must hold when the sale grows. SQLite forces a redesign at the first second writer.

The cost is Docker. `docker compose up -d` starts both containers, and `npm test` starts its own through testcontainers.

## One Redis script replaces a lock

A lock is the usual answer to a sequence of steps that must not interleave. This design uses none, because Redis runs one script as one command, and a script that runs alone needs no lock.

A lock fails in two opposite ways. The holder can die with the lock still held, which is why a lock needs a timeout. A timeout that is too short then releases the lock while the work still runs. A script has neither failure, because Redis holds the server for the length of the script.

Each command inside the script is safe on its own.

| The decision | The command | Why it is safe alone |
| --- | --- | --- |
| Who gets unit number *n* | `INCR sale:sold` | It returns a different number to every caller. A buyer wins only where that number is at most the stock |
| Whether a buyer already holds a unit | `SADD sale:buyers` | It returns 1 to one caller and 0 to every other |

The script opens with a `GET` as a fast path, and that read never makes the decision. A stale read refuses a buyer the sale could still serve, so the wrong `GET` costs one refused buyer and never a wrong count.

**The faults were in the gaps, not in the commands.** I shipped 4 plain commands first, and two faults lived between them.

1. **A buyer was told `already-bought` for a unit nobody won.** A buyer who loses at `INCR` leaves the set again through `SREM`. A second request from that buyer, arriving between the `SADD` and the `SREM`, read a set member and answered `already-bought`.
2. **A crash between `INCR` and the Kafka send burned a unit.** The counter moved, no record left, and the sale sold 999 of 1,000.

The script closes the first fault, because no other client runs inside it. It closes the second with `sale:outbox`. The script writes the buyer and the place into that hash in the same command that issues them. `Pipeline.deliver` runs `HDEL` only after the Kafka send returns, so a row left in the hash is a win that Kafka never saw. `Pipeline.sweepOutbox` reads the hash every 250 ms and sends each row again. A second send is safe three times over: the producer is idempotent, the Kafka key is the buyer, and `orders` holds `UNIQUE (user_id)`.

**Why no transaction.** A `MULTI`/`EXEC` block queues every command and answers them all at the end, so `reserve` cannot read what `SADD` returned before it decides whether to call `INCR`. A block has no rollback either: where one command fails inside `EXEC`, Redis still applies the others. `WATCH` with a retry loop closes the first fault, and it swaps a lock-free path for one that retries under load.

The script costs one load, plus the `EVALSHA` recovery after a Redis restart. `Pipeline.runScript` catches `NOSCRIPT` and loads the script again. The gain is round trips: 4 commands take 4, and the script takes 1.

## The unique index on the buyer is a second guard

The Redis set is the fast guard in memory. `UNIQUE (user_id)` on `orders` is the slow guard on disk. The two fail independently, and that independence is the reason both stay. Erase Redis, or replay a record, and the write still meets the index.

The insert uses `ON CONFLICT DO NOTHING`, so a replay is silent and needs no error handling. I measured that form at 73% cheaper than catching the `23505` error.

**The clause names no constraint on purpose.** `orders` also holds `UNIQUE (seq)`, and `seq` is the place Redis issued. Named `(user_id)`, a repeat place raised `23505`, `eachMessage` threw, and kafkajs crashed the consumer and met the same record again. One record then stopped a partition for good. A bare `ON CONFLICT` covers every unique index on the table, so Postgres drops the repeat and the worker moves on.

## What the queue worker guarantees

Exactly-once has three levels, and the code reaches two of them.

- **Delivery.** Two systems cannot agree on one commit, so no code reaches it.
- **Processing.** The consumption bookmark commits inside the same transaction as the effect. The code reaches this level.
- **Effect.** The sink refuses a repeat on its own. The code reaches this level too.

Kafka transactions cover what Kafka writes, and a Postgres row sits outside them. Redpanda states the same limit for its own broker: exactly-once holds "only when the consumer's output is sent to a Kafka topic itself and not to other remote syncs". KIP-939 was designed to let a Kafka producer join an external transaction, and its public APIs were reverted from Kafka 4.1, 4.2, 4.3 and 4.4. So a broker swap does not move the boundary.

**That boundary is why `queue_offsets` exists.** The table holds the bookmark, and `Gate.record` writes it in the same transaction as the order row. A record whose offset sits below the bookmark is already applied, so `Gate.record` returns `replayed` before it reaches the insert. Delete the table and the design drops to at-least-once delivery, with the exactly-once effect resting on `UNIQUE (user_id)` alone.

The consumer runs with `autoCommit: false`. A per-worker timer commits to Kafka only offsets that Postgres already wrote, so the Kafka bookmark can never run ahead of the record. There is no `seek`. Kafka says where to resume, and a wrong answer there costs time. Postgres says what was applied, and only Postgres is a correctness claim.

A crash between the two commits replays the record, and the guard refuses it. A total loss of `__consumer_offsets` replays the partition from offset 0, and the guard refuses every record already applied.

The manual commit also keeps the standard lag metric honest. `kafka-consumer-groups --describe --group sale-writers` reads the offset the worker wrote after its Postgres `COMMIT`, so the lag it prints is work Postgres has not taken yet.

**Order survives 4 workers.** Each worker carries the number `INCR` issued, all the way into `orders.seq`. The 4 workers write in whatever order they finish, so rows arrive out of order. By arrival time I counted 19, 487 and 460 inversions across three runs. `ORDER BY seq` showed 0 inversions in every run.

## Losing a store, and getting it back

Redis holds the count, and the count is the sale. So the design has to answer what happens when Redis loses it.

Two detectors find the loss, and both end in one rebuild from the order rows.

1. **The `sale:live` flag.** The `RESERVE` script refuses before it counts where the flag is gone, and it answers `lost`. `Pipeline.reserve` then rebuilds and asks once more. `REHYDRATE` writes the flag last, so a script that dies half way leaves the sale refused rather than wrong.
2. **The counter check in the sweep.** Redis issues the place and Postgres records it later, so `sale:sold` is never below `MAX(orders.seq)` while Redis is whole. Below it, Redis lost the counter. The sweep reads one indexed `MAX(seq)` every 250 ms, and a single erased key cannot hide behind a surviving flag.

The rebuild reads every winner from Postgres and writes the set and the counter again. It never lowers `sale:sold`, so a rebuild can undersell and can never issue a place twice.

## Scaling

Each bottleneck below carries the measurement that found it and the change that moves it.

### Database connections

A million buyers do not open a million connections, and no request waits on one.

Postgres starts one OS process per connection, each about 5 MB, with a default `max_connections` of 100. A browser socket is not a database connection. `server/src/server.ts` builds one `pg.Pool` with `max: DB_POOL_MAX`, and that number caps what the process opens.

| Open sockets | `DB_POOL_MAX` | Peak Postgres backends | Requests a second |
| --- | --- | --- | --- |
| 500 | 20 | 4 | 6,800 to 8,380 |

At 500 open sockets only 4 connections are ever in use. Redis answers the buyer path, so that path never touches the pool. Only the queue workers and the page reads open a connection, and they arrive at the rate the queue drains. An earlier design opened a transaction per purchase and held all 20 at the peak.

A bounded pool turns a database failure into a wait inside the application, and that wait has a limit you can see. An arrival spike lands in Kafka instead, where the workers drain it at whatever rate the pool allows.

When one process is not enough, PgBouncer goes in front of Postgres in transaction mode, where it multiplexes thousands of client connections onto tens of server connections. Size `pool_size` from the core count, never from the client count. PostgreSQL 18 added asynchronous I/O and still ships no built-in pooler.

### The one stock row

Every queue worker takes its unit with an `UPDATE` on row `id = 1`, so those writes run one at a time. Redis already answered, and the buyer waits for none of it. At 1,000 units the serialized writes finish fast. At 1,000,000 units the drain is the wall.

The change is `N` stock rows of `stock / N`, with each buyer hashed to one row. It gives up a perfect sell-out, because one shard can empty while another still holds units. It pays off only at a large unit count, where the imbalance stays small.

### Accepting the load before it reaches the API

One Node process cannot absorb a million requests in one second, whatever the database does. The changes, in the order they pay off:

1. **More API processes.** Fastify holds no state, so `N` processes behind one load balancer answer `N` times the requests. One Redis still holds the count, and no process decides alone.
2. **A waiting room.** Admit a bounded number of buyers per second to the purchase route, and hand everyone else a queue position. The sale sells out at the same moment either way, and a fast refusal replaces a timeout.
3. **More queue workers and more partitions.** The write is already off the request path. `QUEUE_WORKERS` sets how many consumers the process runs, and each consumer owns whole partitions. A number above the partition count leaves consumers idle.

### The page, and the open sockets

10,000 open stream sockets on one Node process reach a memory limit, and one ticker serves all of them, so CPU stays cheap. The static files belong on a content delivery network. The stream stays on the API.

### What breaks first, in order

1. The single Node process, when it runs out of open sockets.
2. Redis, on one CPU core. Its commands run on one thread, so the whole decision path sits on that core. The ceiling is still high.
3. The queue drain, once wins arrive faster than the workers retire them. A buyer never feels it. The lag on `GET /api/purchase/:userId` grows instead.
4. The single stock row, far above 1,000 units.

### The shape at a million buyers

The picture below draws every change above. I built none of it, and each box names the measurement that would call for it.

![The target: a content delivery network and a waiting room at the edge, N Fastify processes that all decide in one Redis, Kafka with more partitions and more workers, and PgBouncer in front of a Postgres primary with a read replica](../diagrams/architecture-scale.svg)

## What this design does not promise

The place number restores the arrival order for a reader, and the rows do not *land* in that order. A reader who sorts by insert order still sees the wrong list. Only a single writer fixes that, and the cost of a single writer is in the first section.

One Redis key is one point of failure for the sale it serves, so a busy launch needs a key per sale. A hash tag lands both keys of a sale on one node, and the counter stays correct after that change, because correctness here is a per-key property. The design implies it. I never measured it.
