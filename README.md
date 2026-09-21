# Flash sale

One product has 1,000 units, plus a start time and an end time. Thousands of buyers land at the same moment, and each one may take a single unit. Each unit sells once.

I wrote it in TypeScript on Node 24. Fastify runs the API, Redis 7 makes the decision, and Kafka 4 carries the win. Postgres 16 keeps the permanent record, and React 19 builds the page.

## Run it

You need Node 24 or newer and Docker.

```sh
cp .env.example .env     # ports and addresses only
npm install
npm run db:up            # Postgres, Redis and Kafka in Docker
npm run build            # type checks 3 workspaces, and builds the page
npm start                # migrates the database, then serves on :3000
```

Open `http://127.0.0.1:3000`. One server serves both the page and the API, and there is no CORS.

One row in the database is the whole sale. `server/sql/migrations/0002_campaign.sql` writes 1,000 units, a start in 2026 and an end in 2036. The sale is open the moment you start.

To move the window, run `npm run sale:window`. The server picks the change up within one sweep, which is 250 ms, and it needs no restart.

```sh
npm run sale:window -- --start 2026-10-01T09:00:00Z --end 2026-10-01T10:00:00Z
npm run sale:window -- --start 2026-10-01T09:00:00Z --end 2026-10-01T10:00:00Z --units 500
```

Without `--units` the unit count does not move, so a running sale keeps what it already sold. With `--units` the count is set again, and Redis must be empty before the sale opens.

Run `npm run db:migrate`. The command `npm start` also runs the migrations, and a fresh clone needs no extra step.

`.env` holds addresses and sizes only. Postgres, Redis or Kafka may already run on your machine. If one already runs, change `POSTGRES_PORT`, `REDIS_PORT` or `KAFKA_PORT`, and change the URL beside it.

`npm test` runs all 77 tests. The 23 unit tests each check one module, and none of them touch a container. Run them on their own with `npm run test:unit`, and they finish in about a second, even with Docker stopped.

The 54 integration tests use real Postgres, Redis and Kafka. Testcontainers starts each one in Docker for the run and throws it away after. I mocked nothing. Docker must be running when you start them with `npm run test:integration`.

`npm run dev` runs the server and Vite together. Vite serves the page on `http://127.0.0.1:5173`, and it also proxies `/api` requests to the server.

`npm run db:down` removes the container and its data. Stop the server first. When a server outlives its broker, it holds a producer sequence the new broker never issued. Every send then fails with `out of order sequence number`, and the failures continue until you restart the server.

## Run the stress test

```sh
npm start                # in one terminal
npm run stress           # in another
```

The script empties the sale and orders tables, then drives **10,000 buyers over 500 connections**. It deletes rows, so run it only against localhost.

The run fails on any other outcome:

```
queue drained in 718 ms
ok  won           1000  (want 1000)
ok  sold-out      9000  (want 9000)
ok  other            0  (want 0)
ok  units left       0  (want 0)
ok  pg orders     1000  (want 1000)

10000 buyers over 500 connections in 1.24 s
8077 purchase requests a second
5 Postgres backends at the peak, for 500 open sockets

PASS
```

`queue drained in 718 ms` is the gap between the last buyer getting an answer and the last order row landing in Postgres. Redis answers the buyer first, and a worker writes the row to Postgres later.

That number grows with the records the topic already holds, so a repeat run reads higher than the first one. Against a new `sale.wins` topic I measured 770 ms and 773 ms. The same build read 1,387 ms, 2,411 ms and 3,078 ms on the third, fourth and fifth run. `npm run db:down` and `npm run db:up` drop the topic and return the number to the first reading.

Postgres runs one operating system process per open connection, and that process is a backend. I measured 4 to 5 `Postgres backends` at peak against 500 open sockets, over 23 runs, with `DB_POOL_MAX` set to 20. Only workers and page reads touch the pool, and the buyer path skips it. See [Scaling](#scaling).

`npm run bench` measures throughput with autocannon, but it skips count checks. Autocannon reads its `amount` as a per-connection quota.

## How it works

![The system today: a React page, a Fastify process that decides in Redis and produces to Kafka, 4 queue workers, and Postgres 16 holding the permanent state](diagrams/architecture.svg)

The picture above shows the path a purchase takes, and `GET /api/sale` also reads data. It asks `Pipeline` for the units left and asks the pool for the sale window. `Pipeline` is Redis, and the pool is Postgres. The flowchart below shows those 2 reads and names the call on each arrow.

<details>
<summary>Mermaid source of the same system, with the read paths and the call on each arrow</summary>

```mermaid
flowchart LR
    B["Buyer<br/>React page"]
    F["Fastify<br/>4 routes + SSE"]
    PL["Pipeline<br/>decides the winner"]
    R[("Redis 7<br/>sale:sold, sale:buyers, sale:outbox, sale:issued")]
    K[["Kafka 4<br/>sale.wins, 4 partitions"]]
    W["4 queue workers"]
    P[("Postgres 16<br/>stock, orders, queue_offsets")]

    B -- "POST /api/purchase" --> F
    B -- "GET /api/sale/stream (SSE)" --> F
    B -- "GET /api/purchase/:userId" --> F
    F --> PL
    PL -- "EVALSHA, one script" --> R
    PL -- "send, keyed by the buyer" --> K
    K -- "read_committed, no auto commit" --> W
    W -- "BEGIN / order row + unit + resume point / COMMIT" --> P
    F -- "the units left" --> R
    F -- "the sale window, cached 250 ms<br/>and a buyer's order row" --> P
```

</details>

`mermaid-cli` renders `diagrams/architecture.svg` from `diagrams/architecture.mmd`, and it uses the Iconify `logos` and `mdi` packs. GitHub does not register those packs when it renders Mermaid `architecture-beta` blocks, and the icons then show up as broken placeholders there. I commit the rendered SVG for that reason. The Mermaid source sits beside it so a reader can diff or change the diagram as text.

I split the state across three places. Each one has a single job.

| Place | Holds | Why it is there |
| --- | --- | --- |
| Redis 7 | `sale:sold`, `sale:buyers`, `sale:outbox` and `sale:issued` | It answers the buyer. Redis runs one command at a time, so `INCR` never hands two buyers the same place. |
| Kafka 4 | `sale.wins`, 4 partitions | It carries each win, so the buyer waits for no database write. |
| Postgres 16 | `stock`, `orders` and `queue_offsets` | It is the permanent record, and the only store that must survive a restart. |

Redis answers the buyer in one round trip. `Pipeline.reserve` in `server/src/queue/pipeline.ts` runs the `RESERVE` script from `server/src/queue/scripts.ts`, and Redis runs that whole script as one command. The script does up to 6 steps, and it stops at the first refusal. Before step 1 it checks that `sale:live` and `sale:sold` both exist. Where either key is gone, Redis lost the sale. The script answers `lost` and counts nothing.

1. `GET sale:sold`. If the count already reached the stock, the buyer reads `sold-out`. Nothing is written anywhere.
2. `SADD sale:buyers`. If the member was already in the set, the buyer holds a unit and the answer is `already-bought`.
3. `INCR sale:sold`. The number it returns is the buyer's place in the queue. Because Redis runs one command at a time, two buyers never get the same number.
4. `SREM sale:buyers`, only if that place passed the stock. The loser holds nothing, and the set has no reason to remember them.
5. `HSET sale:outbox`, the buyer to their place. The Kafka send clears the entry, so a row left behind is a win Kafka never received.
6. `HSET sale:issued`, the buyer to the same place. The worker clears the entry only after Postgres commits the order row. So a row left here is a place Postgres does not hold yet, and a rebuild has to count it.

Kafka gets one record for a winner's place, and the buyer is the key. A reconciler sweeps `sale:outbox` every 250 ms and sends each leftover row again. A rebuild after a Redis loss reads the order rows and `sale:issued` together. A place in flight sits in neither the order table nor a cleared outbox.

Steps 2 to 4 are safe together only because one script holds them. Run them as 3 separate commands. A second request from a buyer who lost at step 3 then reads the set between step 2 and step 4. It then answers `already-bought` for a unit nobody won. Inside the script no other client runs at all, and the gap closes.

`SADD` in step 2 is the only "one unit for each buyer" check on the fast path.

Without `SADD`, a repeat buyer reaches `INCR`, and the counter then drops a unit for someone who already holds one. Postgres still refuses the second order row at `UNIQUE (user_id)`. Nobody gets two units. But the count loses that unit, and the sale reads sold out with fewer than 1,000 order rows.

Step 4 is why the set grows with the stock and not with the crowd.

I ran 30,000 buyers against 1,000 units, and Redis held 1,000 members and 47,504 bytes. Drop step 4 and the same run held 30,000 members and 1,461,456 bytes. The run without step 4 used 30.8 times more memory.

In a flash sale, the crowd size is the number nobody can predict.

`Gate.record` in `server/src/gate/gate.ts` writes the permanent state: it writes the order row, takes the unit and moves the queue resume point. I wrap all three in one Postgres transaction. The `no-unit-left` path is the one exception: it rolls the transaction back, then moves the resume point on its own.

One route lags behind the purchase answer. `GET /api/purchase/:userId` reads Postgres only, and a buyer told `won` can briefly read `held: false` until a worker writes their row. I measured 12 runs of 1,000 wins each. The last row landed 566 ms to 3,285 ms after the last buyer got an answer. The page does not need that route for the purchase result, and `POST /api/purchase` already carries the outcome.

The server has 4 routes.

| Route | Answers |
| --- | --- |
| `GET /api/sale` | `pending`, `open`, `sold-out` or `closed`, and the units left |
| `GET /api/sale/stream` | the same, pushed over server-sent events whenever it changes |
| `POST /api/purchase` | one of `won`, `already-bought`, `sold-out`, `not-open`, `over` |
| `GET /api/purchase/:userId` | whether that buyer holds a unit, and when they got it |

One ticker serves every open page. It reads the state once per tick, then writes to each open socket. So 1,000 pages cost one read.

The state read costs no database round trip. `GET /api/sale` takes the units left from Redis, and the sale window from a copy of `stock` that I let `Gate` hold for 250 ms. The window never moves while the sale runs. A stale copy cannot be wrong.

## Why no unit is oversold

Three places refuse, and each one refuses a different thing.

Redis decides the winner. `INCR sale:sold` returns a unique number on each call: 1, then 2, and so on. Redis runs one command at a time, so 10,000 buyers who arrive together get 10,000 different numbers. A buyer whose number passes the stock loses, and I drop them from the set. I used no lock and no transaction.

Kafka carries each win once, and in order, for one buyer. Kafka splits a topic into partitions, and each partition keeps its own order. I key each record by the buyer, so one buyer always lands on one partition. The producer runs idempotent, which means a record sent twice lands once. kafkajs then forces `acks: -1`, and a send waits for every Kafka copy that is caught up. A retried send writes one record. The consumer reads `read_committed`, so it never sees a record from a transaction that did not commit.

Postgres refuses the oversell a second time. `Gate.record` runs one transaction that stops at the first statement to refuse.

1. An offset is a record's position in its partition, and `queue_offsets` remembers the next one.
   `INSERT INTO queue_offsets ... ON CONFLICT DO NOTHING`, then
   `SELECT next_offset ... FOR UPDATE`. Every path grabs this lock first. Two workers on one partition then run one after the other. When the lock came second, it deadlocked against the stock row. I first saw that deadlock at 100 parallel records. A record whose offset is below `next_offset` stops here and answers `replayed`, because the database already applied it.
2. `INSERT INTO orders (user_id, seq) ... ON CONFLICT DO NOTHING RETURNING user_id`. If no row comes back, the table already holds that buyer or that place. Neither one takes a second unit from the count. The clause names no constraint, so it covers `orders_seq_key` as well as `orders_user_id_key`. Named, a repeat place raised `23505` and stopped the partition for good.
3. `UPDATE stock SET units_left = units_left - 1 WHERE id = 1 AND units_left > 0 RETURNING
   units_left`. If no row comes back, the database holds fewer units than the queue holds wins. That is the oversell I refuse here.

A commit writes the order row, the decrement, and the new resume point together. A rollback writes none of them.

[`docs/architecture.md`](docs/architecture.md#what-the-queue-worker-guarantees) covers repeats, crashes and arrival order, and it says why `queue_offsets` cannot be dropped.

Four guards protect the count.

| Guard | Where | What it refuses |
| --- | --- | --- |
| `INCR` past the stock | `Pipeline.reserve` | the oversell, before the queue |
| `AND units_left > 0` | the `UPDATE` above | the oversell, a second time |
| `UNIQUE (user_id)` | `orders_user_id_key` | a second unit for one buyer, and a record read twice |
| `CHECK (units_left >= 0)` | `stock_never_negative` | a future defect, as a failed transaction |

The last two guards live in `server/sql/migrations/0001_tables.sql`. Each one has a test in `server/test/integration/schema.spec.ts` that tries to break it. When the worker fails, Postgres rolls the transaction back, and the stock stays intact.

## Why the Redis decision is one script and not a transaction

`Pipeline.reserve` uses no `MULTI` and no `WATCH`. It runs one Lua script, and Redis runs that script as one command. A `MULTI` block answers every command at the end, so it cannot branch on what `SADD` returned. [`docs/architecture.md`](docs/architecture.md#one-redis-script-replaces-a-lock) has the full argument.

## What happens when something breaks

Each row is a fault I injected and measured.

| What breaks | What the buyer gets | What the record shows |
| --- | --- | --- |
| Postgres stops | The buyer still wins in Redis, and the win waits in Kafka | the order rows held all 1,000 wins after the restart. `server/test/integration/routes.spec.ts` asserts the 500 while it is down |
| The Postgres link adds 2 s of delay | The answer is unchanged, and the drain takes longer | 1,000 order rows, at 1,740 decisions a second |
| The server is killed mid-sale | No unit comes back | the count is rebuilt from the order rows |
| Redis is lost | The counter is rebuilt from `max(seq)` | 1,000 buyers, rebuilt exactly, in 4 ms |
| 1 of 4 workers is killed while the queue drains | No row is lost, and no row is written twice | 0 rows lost, 0 rows doubled |

## Measured

I ran this on a box with an Intel Core Ultra 9 275HX, 24 cores, 62 GB RAM and Node 24.11.1. Postgres 16, Redis 7 and Kafka 4 run in Docker. Every number below names the command that produced it.

`p50` is the middle response time, and half the requests come back faster than it. `p99` is the time that only the slowest 1 in 100 requests exceed.

| Measure | Number | Command |
| --- | --- | --- |
| Buyers, and the units they took | 10,000 buyers, exactly 1,000 won | `npm run stress` |
| Time for all 10,000 | 1.19 s to 1.48 s over 23 runs, so 6,750 to 8,380 a second | `npm run stress` |
| Postgres backends at the peak | 4 to 5, for 500 open sockets | `npm run stress` |
| The queue drain | every one of the 1,000 order rows landed, 566 ms to 3,285 ms after the last buyer was answered | `npm run stress` |
| `GET /api/sale` throughput | 30,167 to 32,309 a second over 3 runs, p50 13 to 14 ms, p99 54 to 58 ms | `npm run bench` |
| `POST /api/purchase` throughput | 28,358 to 29,331 a second over 3 runs, p50 16 ms, p99 26 to 32 ms | `npm run bench` |
| Errors and non-2xx under load | 0 and 0 | `npm run bench` |
| Tests | 77 over 11 files: 23 unit, 54 integration against real Postgres, Redis and Kafka | `npm test` |

Redis answers both routes, and the purchase route runs within 12% of the read route. An earlier version opened a Postgres transaction on every purchase and ran at 11,529 a second. Redis raised the refusal path by 2.5 times.

`npm run bench` drives one repeat buyer against a sold-out sale. It measures the refusal path only. `npm run stress` measures the winning path at 6,750 to 8,380 a second, and that last number includes the Kafka send.

The load generator and the server share the same 24 cores, and the server is one Node process with no cluster and no tuning. Every number here is a floor.

## Scaling

Four things break first, in this order.

1. **The single Node process**, when it runs out of open sockets. More processes behind one load balancer answer more requests.
2. **Redis, on one CPU core.** Every command runs on that one thread. The ceiling is still high.
3. **The queue drain**, once wins arrive faster than the workers retire them. A buyer never feels it. The lag on `GET /api/purchase/:userId` grows instead, and a higher `QUEUE_WORKERS` pulls it back.
4. **The single stock row**, far above 1,000 units. Every worker updates row `id = 1`, one write at a time. The change is `N` stock rows of `stock / N`, and it gives up a perfect sell-out.

Database connections are the bottleneck people expect. They are not one here. At 500 open sockets with `DB_POOL_MAX` set to 20, Postgres held 4 to 5 backends at the peak.

[`docs/architecture.md`](docs/architecture.md#scaling) gives each measurement, the change that moves each limit, and the target picture at a million buyers.

## Why Redis, a queue and a database

Three stores are more than one flash sale needs. The split is what makes each store fail on its own.

One process can own the count instead, with every buyer going through that one process and nothing else writing it. That is a single writer, and it runs 2.5 times faster. A second process breaks it. Making it safe costs a third of that speed, and it still lost 477 acknowledged wins when I killed the process. With the split I injected 2 seconds of delay into the Postgres link. The sale kept answering at 1,740 decisions a second, with all 1,000 wins intact. The same fault against a design that writes the order inside the request committed 0 wins, and the buyer never heard an answer.

[`docs/architecture.md`](docs/architecture.md#redis-decides-the-winner-and-postgres-keeps-the-record) gives the full argument and the four costs.

## Trade-offs

The split above costs two more containers, and it leaves a window where Redis holds a win that Postgres does not have yet. I measured both.

One Lua script makes the decision, and I use no transaction. It costs a second language that no type checker reads. It buys two things that four plain commands miss. A second request from a buyer who lost at `INCR` no longer reads `already-bought` for a unit nobody won. A crash right after `INCR` no longer burns a unit. [Why the Redis decision is one script and not a transaction](#why-the-redis-decision-is-one-script-and-not-a-transaction) gives the argument, and `server/test/integration/pipeline.spec.ts` gives the proof.

SQLite handles 1,000 rows from one process with no container. I use Postgres anyway. A real sale grows past that, and I did not want the design to change when it does. The cost is one container.

Nothing is mocked. The database is real Postgres. A run starts it in Docker, and a test starts it through testcontainers. The code does not change when the database moves to a managed instance.

The page receives server-sent events. One open connection carries every message, and the page never asks for one. I need one direction only, and a WebSocket adds nothing here. Server-sent events reconnect on their own, and they are plain HTTP.

Two npm workspaces hold the code in one repository. One `npm ci` and one `npm test` cover `server/` and `web/` together. The root `package.json` holds scripts that start other scripts, so a reader has to open that file to see what `npm test` runs. That is the cost I took.

Some things are missing on purpose. There is no authentication, because a username is the whole identity a flash sale needs. No payment, no deployment, no rate limit. A real sale needs a rate limit. This one does not claim to be one.

## Layout

```
server/   Fastify, the Redis pipeline, the Postgres gate, the migrations, 58 tests
web/      React 19 on Vite, 13 tests
stress/   the correctness run, and the throughput bench
```

| File at the root | What it is |
| --- | --- |
| `diagrams/architecture.mmd`, `diagrams/architecture.svg` | the system today, as source and as image |
| `diagrams/architecture-scale.mmd`, `diagrams/architecture-scale.svg` | the target shape at a much larger load |
| `docker-compose.yml` | Postgres 16, Redis 7 with `appendfsync everysec`, and Kafka 4 in KRaft mode, where Kafka keeps its own metadata and needs no ZooKeeper |
| `.env.example` | every address and size the server reads, with no constant hidden in source |
| `server/sql/migrations/` | the tables, then the campaign row. `npm start` and `npm run db:migrate` both apply them, once each |
| `docs/architecture.md` | why the system has this shape, what each choice costs, and how it scales |

## What the sale does, and where

| Capability | Where it lives |
| --- | --- |
| A configurable start and end time, and purchases only inside it | `start_at` and `end_at` in the `stock` row, written by `server/sql/migrations/0002_campaign.sql`. The server checks them before it reads any store. `server/test/integration/pipeline.spec.ts` |
| One product with a fixed quantity | one row in `stock`, with `CHECK (units_left >= 0)` and a single-row constraint |
| One unit per user | `SADD sale:buyers` refuses the second attempt, and `UNIQUE (user_id)` on `orders` refuses it again. `server/test/integration/schema.spec.ts` |
| An endpoint for the sale state | `GET /api/sale`, and `GET /api/sale/stream` for the live form. A buyer sees 3 states: upcoming (`pending`), active (`open`) and ended (`sold-out` or `closed`). The sale splits "ended" in two so the buyer knows whether the units ran out or the clock did |
| An endpoint to attempt a purchase | `POST /api/purchase` |
| An endpoint for what a buyer holds | `GET /api/purchase/:userId` reads Postgres, so a fresh win lags by the drain time |
| A simple frontend with a state line, an identifier field, a Buy Now button and feedback | `web/`, React 19. The page names all 5 outcomes and updates with no reload |
| A clear system diagram | `diagrams/architecture.svg` at the top of How it works, with its source beside it |
| High throughput, and a design that scales | the Measured and Scaling sections. Each number names its command. The write is already off the request path |
| Robustness and fault tolerance | a slow database costs drain time and no answers. A rolled-back transaction writes no order. A lost Redis is rebuilt from the order rows, and `max(seq)` gives the next place, never the row count |
| Concurrency control, with no overselling | `INCR` in Redis, then the row lock in the `UPDATE`, proved by the 10,000-buyer run and by 21 injected faults |
| Unit and integration tests | 23 unit tests in `server/test/unit/` and `web/test/unit/`, run by `npm run test:unit`. 54 integration tests in `server/test/integration/` and `web/test/integration/`, run by `npm run test:integration` against real Postgres, Redis and Kafka through testcontainers |
| Stress tests, and an explanation of the results | `npm run stress` for the counts, `npm run bench` for the speed, and the Measured section for the reading |
| TypeScript, Node with Fastify, React | all three, type checked by `npm run build` |
| Ready for managed services | every store is a managed product, and nothing is mocked. The Scaling section holds the target picture |
