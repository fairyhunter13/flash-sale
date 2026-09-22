# Test plan

## How to read a row

Every row carries an identifier. I number a test case `T-01`, a scenario `S-01`, and a user journey `J-01`. The numbers only go up. A dropped row keeps the number it was born with.

**Tier** says how much of the real system the test uses.

- `unit` runs one module. It starts no container.
- `route` sends a request through the real route.
- `engine` runs against a real Redis, Kafka or Postgres.

**Covers** names a development task, from `D-01` to `D-16`. The task table is in [`docs/development-plan.md`](development-plan.md#task-table).

**Status** is one of planned, in-progress, done, blocked or dropped. A blocked row names the behavior I saw, and a dropped row gets one line to say why.

**Risk** is the likelihood times the impact. Each one runs from 1 to 5, so risk runs from 1 to 25. A score of 1 to 6 is low, 7 to 14 is medium, and 15 to 25 is high. A high risk earns a user journey as well as a test case.

## Scope

Every tier except `unit` talks to real services. Testcontainers starts a real Redis 7, a real Kafka 4 and a real Postgres 16 inside the test run, and vitest is the runner.

I tested the Redis pipeline, the four routes, the Kafka workers, the page and the stress run.

The tier decides the folder. A `unit` case lives in `server/test/unit/` and `web/test/unit/`, and `npm run test:unit` runs those 23 while Docker is stopped.

A `route` case and an `engine` case live in `server/test/integration/` and `web/test/integration/`, and `npm run test:integration` runs those 58. `npm test` runs all 81.

I left four things out.

- Nothing is deployed, and I wrote no deployment test.
- The user types a buyer identifier and I trust it. The README names that trust as a hole.
- The sale records a win but takes no money. I built no payment path, and there is nothing to test yet.
- The tests use the React testing library, but I ran no browser matrix.

## Scenarios

S-01 Two buyers reach for the last unit.
Precondition: the sale is open and 1 unit is left.
Action: two different buyers send a purchase at the same time.
Expected result: one answer is `won`. The other is `sold-out`.
Postcondition: the count is 0 and never drops below it. Exactly 1 new record lands on the topic.

S-02 The same buyer buys twice.
Precondition: the sale is open, units are left, and buyer A holds a unit.
Action: buyer A sends a second purchase.
Expected result: I return `already-bought`. A buyer who already won is not competing for a leftover unit, and the server still does not return `sold-out` for a repeat buyer.
Postcondition: the count does not change, and no new record lands on the topic.

S-03 An attempt before the sale opens.
Precondition: the clock is before the start time.
Action: a buyer sends a purchase.
Expected result: the answer is `not-open`.
Postcondition: the count does not change. No buyer is recorded.

S-04 An attempt after the sale ends.
Precondition: the clock is after the end time, and units are left.
Action: a buyer sends a purchase.
Expected result: the answer is `over`.
Postcondition: the count does not change.

S-05 Ten thousand buyers race for one thousand units.
Precondition: the sale is open with 1,000 units, and a worker runs.
Action: 10,000 different buyers each send one purchase over 500 connections.
Expected result: exactly 1,000 answers are `won`, and exactly 9,000 are `sold-out`. The server answers every request.
Postcondition: the order table holds exactly 1,000 rows. Each `user_id` appears once.

S-06 A buyer asks again after the answer is lost.
Precondition: buyer A won a unit, and the worker wrote the row.
Action: buyer A reads their own purchase state.
Expected result: `held` is true.
Postcondition: the count stays the same. No second unit is taken.

S-07 Read the sale state.
Precondition: the sale is open with units left.
Action: a caller reads the sale.
Expected result: `state` is `open`, and `stockLeft` is the real count.
Postcondition: a read takes no unit.

S-08 A win becomes an order row.
Precondition: the `sale.wins` topic holds one record, and a worker runs.
Action: the worker reads the record.
Expected result: the worker inserts one row for that buyer.
Postcondition: I write the order row, the unit and the offset in one transaction.

S-09 A worker restarts with wins unread.
Precondition: 50 records are on the topic, and the worker is stopped.
Action: the worker starts.
Expected result: the worker reads all 50 from the topic.
Postcondition: the order table holds 50 rows.

S-10 The database refuses a second row for one buyer.
Precondition: an order row exists for buyer A.
Action: a worker reads a second record for buyer A.
Expected result: the unique index refuses the insert. The worker does not stop.
Postcondition: the order table holds exactly 1 row for buyer A.

S-19 A record below the resume point never reaches the stock.
Precondition: `queue_offsets` holds a resume point past the record's offset.
Action: a worker reads that record again.
Expected result: `Gate.record` answers `replayed` before the insert runs.
Postcondition: the order table and the unit count do not move.

S-20 A win that never reached Kafka is delivered by the sweep.
Precondition: `sale:outbox` holds a buyer and a place, and no order row names that buyer.
Action: the reconciler runs.
Expected result: the win reaches Kafka, and a worker writes the order row.
Postcondition: `sale:outbox` is empty, and the unit count is down by 1.

S-21 A buyer is never told they hold a unit they did not win.
Precondition: the sale has 1 unit, and 3 buyers each send 8 parallel requests.
Action: 2 of the 3 buyers lose at the counter.
Expected result: every buyer told `already-bought` holds an order row.
Postcondition: exactly 1 order row exists.

S-22 A rebuild never lowers the counter.
Precondition: `sale:sold` reads 3, and the order table holds 2 rows.
Action: `Pipeline.rehydrate` runs.
Expected result: `sale:sold` stays at 3.
Postcondition: the next buyer gets place 4, and no place is sold twice.

S-23 A place the table already holds never stops the worker.
Precondition: one order row holds place 1, and the queue holds a second buyer on place 1.
Action: a worker reads that record.
Expected result: `Gate.record` answers `already-recorded`, and it raises no error.
Postcondition: the order table and the unit count do not move, and the resume point moves by 1.

S-24 A Redis that loses the whole sale mid-run never issues a place twice.
Precondition: 3 buyers hold a unit, and every Redis key of the sale is erased.
Action: a fourth buyer sends a purchase.
Expected result: `Pipeline.reserve` rebuilds from the order rows, then answers `won`.
Postcondition: the places read 1, 2, 3 and 4, and no place is issued twice.

S-25 A new sale window takes effect with no restart.
Precondition: the sale is open, and the process is running.
Action: `npm run sale:window` writes a window that already closed.
Expected result: the next purchase reads `over` within one sweep, which is 250 ms.
Postcondition: the unit count does not move.

S-26 One erased key puts Redis behind Postgres, and the sweep repairs it.
Precondition: `sale:sold` reads 0, the order table holds 3 rows, and `sale:live` survives.
Action: the sweep runs.
Expected result: `sale:sold` returns to 3.
Postcondition: the units left read 7, and no place is issued twice.

S-27 An erased counter alone never issues a place twice.
Precondition: 3 buyers hold a unit, and `sale:sold` is deleted while `sale:live` survives.
Action: a fourth buyer sends a purchase.
Expected result: `RESERVE` answers `lost`, and the rebuild runs before the buyer is answered.
Postcondition: the places read 1, 2, 3 and 4, and every buyer told `won` holds a row.

S-28 A rebuild keeps a win that Kafka never confirmed.
Precondition: 3 buyers hold a unit, and one more place sits in `sale:outbox` and in `sale:issued`.
Action: the rebuild runs.
Expected result: the stranded buyer reaches the order table.
Postcondition: the units left read 6, and the stranded place is never issued again.

S-29 Every Redis key the sale writes has no expiry.
Precondition: a sale is open, and one buyer holds a unit.
Action: read every key that carries this sale's suffix.
Expected result: each key answers `-1` to `TTL`, which means it never expires.
Postcondition: no key outside the 5 the design names is present.

S-30 The sale gives its Redis keys back when the window closes.
Precondition: a sale sold 3 of 10 units, and Postgres holds all 3 order rows.
Action: move the end time into the past, then wait for the sweep.
Expected result: no key with this sale's suffix is left, and the unit count still reads 7.
Postcondition: the sweep starts no rebuild, so the keys stay gone.

S-31 A sale that has not opened holds no Redis key.
Precondition: the window starts in one hour, and no buyer has asked yet.
Action: let the sweep run three times, then send one purchase.
Expected result: the buyer reads `not-open`, and no key with this sale's suffix is present.
Postcondition: the unit count comes from Postgres, because no counter exists.

S-32 A server that starts after the sale ended holds no Redis key.
Precondition: the sale sold 3 of 10 units, the window is closed, and the keys are gone.
Action: stop the process, then start a second one against the same sale.
Expected result: no rebuild runs, no key with this sale's suffix comes back, and a late buyer reads `over`.
Postcondition: the count comes from Postgres, and the replayed wins add no order row.

S-11 A store does not answer.
Precondition: the database is unreachable.
Action: a buyer sends a purchase.
Expected result: 500 with an `error` field. The response has no `outcome` field.
Postcondition: nothing is recorded.

S-12 The page names each outcome.
Precondition: the page is open, and the server answers a known outcome.
Action: the person types a buyer identifier and presses the button.
Expected result: the page shows one sentence for that outcome. The five sentences differ from each other.
Postcondition: the page holds that outcome until the person tries again.

S-13 The page shows the state of the sale.
Precondition: the sale is not open yet, and the page is loaded.
Action: the person reads the page before any attempt.
Expected result: the page names the state and the units left. It refuses the button while the state is not `open`.
Postcondition: a page load takes no unit.

S-14 Postgres does not answer.
Precondition: Postgres is unreachable. Redis answers normally.
Action: a buyer reads their own purchase state.
Expected result: 503 with an `error` field. The response carries no `held` field.
Postcondition: the page says something went wrong. It does not tell the buyer they hold nothing.

S-15 The sale opens while the page is open.
Precondition: the page holds an open stream, and the sale state is `pending`.
Action: the sale start time passes.
Expected result: the page shows `open` with no reload.
Postcondition: the server holds no connection for the page, and the page closes its stream when the component unmounts.

S-16 The server restarts in the middle of a live sale.
Precondition: the sale is open, and some units are already sold.
Action: the server boots again.
Expected result: Redis keeps the sold-unit count. The boot rebuilds nothing.
Postcondition: a unit that was already sold stays sold.

S-17 Redis is lost while Postgres survives.
Precondition: Postgres holds the order rows, and Redis holds no counter. Redis comes back empty after a restart with no saved data. A promoted replica comes back empty the same way.
Action: the server boots.
Expected result: the server rebuilds the counter and the buyer set from the order rows.
Postcondition: the next buyer gets the next place. No unit goes to two buyers.

S-18 The reviewer starts the server with no build step.
Precondition: a fresh clone, and `npm install` ran.
Action: the reviewer runs `npm start`.
Expected result: the server listens and answers. `node --experimental-strip-types` strips the type annotations and touches nothing else. So a constructor parameter property, an enum or a namespace is a SyntaxError.
Postcondition: no source file holds syntax that strip-only mode refuses.

## Cases

| ID | Title | Scenario | Covers | Status | Test node ID | Risk | Tier |
| --- | --- | --- | --- | --- | --- | --- | --- |
| T-01 | Every workspace builds and the types check | — | D-01, D-10 | done | `npm run build` | 6 | unit |
| T-02 | The schema creates the table and the unique index | — | D-02 | done | server/test/integration/schema.spec.ts > the order table > the order table refuses a duplicate user | 12 | engine |
| T-56 | A closed sale drops all 5 Redis keys, and Postgres answers the count | S-30 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > the sale drops every Redis key once the window closes, and Postgres answers from then on | 13 | engine |
| T-57 | A pending sale writes no Redis key, and the count comes from Postgres | S-31 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > a sale that has not opened writes no Redis key at all | 13 | engine |
| T-58 | A restart after the sale writes no Redis key, and a Kafka replay adds no row | S-32 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > a server that starts after the sale ended takes no Redis key on | 13 | engine |
| T-03 | The boot stops when a store address is missing, and the sale is not read here | — | D-03 | done | server/test/unit/config.spec.ts > the configuration > a missing DATABASE_URL stops the boot | 8 | unit |
| T-04 | Two buyers race for one unit, and one wins | S-01 | D-04, D-05 | done | server/test/integration/pipeline.spec.ts > the pipeline > a buyer who lost is told sold-out again, and holds no place in the set | 25 | engine |
| T-05 | A repeat buyer is refused with the right reason | S-02 | D-04, D-05 | done | server/test/integration/pipeline.spec.ts > the pipeline > a winner who asks again is told already-bought | 20 | engine |
| T-06 | A purchase before the start time is refused | S-03 | D-04 | done | server/test/integration/pipeline.spec.ts > the pipeline > the window is answered before any store is read | 12 | engine |
| T-07 | A purchase after the end time is refused | S-04 | D-04 | done | server/test/unit/status.spec.ts > the sale state > a sale that ended with units left is closed and not open | 12 | unit |
| T-08 | Parallel calls take exactly the stock, and no more | S-01 | D-05 | done | server/test/integration/pipeline.spec.ts > the pipeline > the set holds the stock, whatever the traffic is | 25 | engine |
| T-09 | The sale state follows the clock and the count | S-07 | D-06 | done | server/test/unit/status.spec.ts > the sale state > the state follows the clock and the count | 9 | engine |
| T-10 | GET /api/sale answers the state and the count | S-07 | D-07 | done | server/test/integration/routes.spec.ts > the routes > GET /api/sale answers the state | 9 | route |
| T-11 | POST /api/purchase answers exactly one outcome | S-01, S-02 | D-07 | done | server/test/integration/routes.spec.ts > the routes > POST /api/purchase answers one outcome | 16 | route |
| T-12 | A store that does not answer gives 500 and no outcome | S-11 | D-07 | done | server/test/integration/routes.spec.ts > the routes > a dead database gives 500 and no outcome | 15 | route |
| T-13 | A win on the queue becomes one order row | S-08 | D-08 | done | server/test/integration/gate.spec.ts > the gate > one win writes one order row and takes one unit | 16 | engine |
| T-14 | A restart does not give back a unit already sold | S-09 | D-08 | done | server/test/integration/gate.spec.ts > the gate > a restart does not give back a unit already sold | 20 | engine |
| T-15 | GET /api/purchase/:userId reads the record | S-06 | D-09 | done | server/test/integration/orders.spec.ts > the purchase state > the purchase state reads the record | 9 | route |
| T-16 | The page shows each of the five outcomes | S-12 | D-10 | done | web/test/unit/App.spec.tsx > the page > the page names each outcome | 9 | unit |
| T-17 | The button is refused while the identifier is empty | S-12 | D-10 | done | web/test/unit/App.spec.tsx > the page > an empty identifier cannot be sent | 6 | unit |
| T-18 | The stress run reports 1,000 wins and 9,000 refusals | S-05 | D-11 | done | `npm run stress` > PASS, all 6 counts match | 25 | engine |
| T-19 | Every command the README names exists in a package | — | D-12 | dropped | `readme.spec.ts` was deleted. A test that reads prose broke on every edit | 6 | unit |
| T-20 | Every requirement the map states is realized by a part | — | D-13 | dropped | `concepts.spec.ts` was deleted with the concept map | 12 | engine |
| T-21 | The unique index refuses a second row for one buyer | S-10 | D-08 | done | server/test/integration/schema.spec.ts > the order table > the order table refuses a duplicate user | 20 | engine |
| T-22 | The page names the state and the units left | S-13 | D-10 | done | web/test/unit/App.spec.tsx > the page > the page shows the sale state and the stock | 12 | unit |
| T-23 | The README holds a diagram and a scaling section | — | D-12, D-14 | dropped | `readme.spec.ts` was deleted. A test that reads prose broke on every edit | 9 | unit |
| T-24 | A Postgres that does not answer gives 503 and no held field | S-14 | D-09 | done | server/test/integration/orders.spec.ts > the purchase state > a dead Postgres gives 503 and no held field | 15 | route |
| T-25 | Every number the README claims comes from a run | — | D-12 | dropped | `readme.spec.ts` was deleted. A test that reads prose broke on every edit | 9 | unit |
| T-26 | A fresh clone installs, builds and tests with no extra step | — | D-15 | done | Run by hand against a clone of HEAD in an empty directory | 16 | engine |
| T-27 | Every Decision row has a section in the decision log | — | D-16 | dropped | `decisions.spec.ts` was deleted with the concept map it read | 9 | unit |
| T-42 | Every decision entry states the question and what it gives up | — | D-16 | dropped | `decisions.spec.ts` was deleted with the concept map it read | 7 | unit |
| T-43 | Every case and every failure the map states is covered by a check | — | D-13 | dropped | `concepts.spec.ts` was deleted with the concept map | 8 | engine |
| T-44 | Every failure the map states is handled by a part | — | D-13 | dropped | `concepts.spec.ts` was deleted with the concept map | 6 | engine |
| T-45 | The concept file reads back as rows, and every row has a name | — | D-13 | dropped | `concepts.spec.ts` was deleted with the concept map | 5 | engine |
| T-28 | The stream pushes a state change, and a closed page releases it | S-15 | D-07 | done | server/test/integration/stream.spec.ts > the stream > the stream pushes a change and closes cleanly | 16 | route |
| T-29 | A restart does not give back a unit already sold | S-16 | D-05 | done | server/test/integration/gate.spec.ts > the gate > a restart does not give back a unit already sold | 20 | engine |
| T-31 | Each boundary instant lands on the right state | S-07 | D-06 | done | server/test/unit/status.spec.ts > the sale state > a sale that ended with units left is closed and not open | 12 | engine |
| T-32 | An empty userId is refused before a store is touched | S-12 | D-07 | done | server/test/integration/routes.spec.ts > the routes > an empty userId is refused before the database is touched | 8 | route |
| T-33 | Two open pages share one ticker, and each gets the state at once | S-15 | D-07 | done | server/test/integration/stream.spec.ts > the stream > a second page gets the state at once, and one tick serves both | 9 | route |
| T-34 | A record read twice writes no second row | S-10 | D-08 | done | server/test/integration/gate.spec.ts > the gate > the same record read twice writes one row | 16 | engine |
| T-35 | The workers drain every win the buyers produced | S-08 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > Redis answers the buyer, and the database holds the same winners | 12 | engine |
| T-30 | An insert that names the constraint writes no second row | S-10 | D-02, D-08 | done | server/test/integration/schema.spec.ts > the order table > the second insert writes no row when `ON CONFLICT` names the constraint | 12 | engine |
| T-40 | A record below the resume point never reaches the stock | S-19 | D-08 | done | server/test/integration/gate.spec.ts > the gate > a record below the resume point never reaches the stock | 15 | engine |
| T-41 | A worker that replays a whole partition writes no second row | S-19 | D-08 | done | server/test/integration/gate.spec.ts > the gate > a worker that replays a whole partition from offset 0 writes no second row | 14 | engine |
| T-46 | A win left in the outbox reaches the database after one sweep | S-20 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > a win left in the outbox reaches the database after one sweep | 13 | engine |
| T-47 | No buyer is told already-bought for a unit they never won | S-21 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > no buyer is told already-bought for a unit they never won | 12 | engine |
| T-48 | A rebuild never lowers the counter | S-22 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > a rebuild never lowers the counter | 7 | engine |
| T-49 | A second buyer on a place already taken never stops the worker | S-23 | D-08 | done | server/test/integration/gate.spec.ts > the gate > a second buyer on a place already taken is refused, and the worker moves on | 15 | engine |
| T-50 | A Redis that loses the whole sale mid-run never issues a place twice | S-24 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > a Redis that loses the whole sale mid-run never issues a place twice | 14 | engine |
| T-51 | A new sale window takes effect with no restart | S-25 | D-05 | done | server/test/integration/pipeline.spec.ts > the pipeline > a new sale window takes effect with no restart | 10 | engine |
| T-52 | The sweep rebuilds the counter when one erased key puts Redis behind Postgres | S-26 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > the sweep rebuilds the counter when one erased key puts Redis behind Postgres | 16 | engine |
| T-53 | An erased counter alone never issues a place twice | S-27 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > an erased counter alone never issues a place twice | 14 | engine |
| T-54 | A rebuild keeps a win that Kafka never confirmed | S-28 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > a rebuild keeps a win that Kafka never confirmed | 15 | engine |
| T-55 | No Redis key expires, and only 5 keys exist | S-29 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > every Redis key the sale writes has no expiry, and there are only five of them | 12 | engine |
| T-36 | Every server source file runs under strip-only mode | S-18 | D-01 | done | server/test/unit/strip.spec.ts > the source runs under node > every server source file strips cleanly | 10 | route |
| T-37 | A lost Redis is rebuilt from the order rows | S-17 | D-08 | done | server/test/integration/pipeline.spec.ts > the pipeline > a lost Redis is rebuilt from the order rows, and the next place is right | 14 | engine |
| T-38 | The Buy Now button is refused while the sale is not open | S-03 | D-10 | done | web/test/unit/App.spec.tsx > the page > the button is refused while the sale is not open | 12 | unit |
| T-39 | A reload tells the buyer what they already hold | S-06 | D-10 | done | web/test/unit/App.spec.tsx > the page > a reload tells the buyer what they already hold | 14 | unit |
| T-40 | A record that cannot be read never says the buyer holds nothing | S-14 | D-10 | done | web/test/unit/App.spec.tsx > the page > a record that cannot be read never says the buyer holds nothing | 16 | unit |
| T-41 | A stream that fails says the sale cannot be read | S-11 | D-10 | done | web/test/unit/App.spec.tsx > the page > a stream that fails says the sale cannot be read | 13 | unit |

## User journeys

J-01, a buyer wins. Before the sale, a buyer opens the page and it shows `pending`. The sale opens, and the page flips to `open` with no reload. The buyer types an identifier and presses the button, and the page confirms the unit is theirs.

Acceptance: the order table holds that buyer. Reload it and you get the same answer, straight from the record.

J-02: A buyer loses a sale. Stock is 1, another buyer already took it, and now this buyer tries to buy and lands on a sold-out page. The trouble is the page cannot say whether the buyer lost the race or already won it. Acceptance: the page tells "sold out" apart from "already bought", and it shows the buyer which one applies.

J-03, the stress run: `docker compose up -d`, then `npm run stress`. I count it as passing when the run prints the three counts and the command that produced them.

J-04, a reviewer clones the repository, runs `npm install` and `npm test`, and reads the README.

Acceptance: I ship it when three things hold. Every command is in the README, and the test run starts its own Redis and Postgres. The diagram renders on the GitHub page.

## Experience bar

- A refusal and a fault look different, and I kept them apart on purpose. `sold-out` means the sale ran out. A Redis failure is a 500, and the page tells the buyer something broke.
- Every error names the valid set. Send a bad `userId` and the answer tells you what a `userId` must be.
- No answer is silently empty. `GET /api/purchase/:userId` for a buyer who never bought answers `held: false`. A Postgres read failure answers 503.

## Fixtures

`redis:7` and `postgres:16` are real containers, and Testcontainers starts them in `server/test/setup/containers.ts` for every `engine` run, each on its own port. The same two images run under `docker-compose.yml` for the app and the stress run. `stress/run.ts` generates the 10,000 buyer identifiers, and they are unique by construction. I never mock Redis or Postgres in an `engine` row. T-12 and T-24 each simulate a dead engine when they close the real connection.

## Traceability

Two checks, each one command.

1. Run `npx vitest list --json=/tmp/nodes.json`. The output has to contain every node ID this file claims. `vitest list` ignores `--reporter`, and only the `--json=<path>` form writes anything to disk. A row that names a test the runner never collects proves nothing.
2. Every `T-nn` names at least one `D-nn`. Every `D-nn` gets named by at least one `T-nn`. I check both directions with an `awk` pass over the two documents.
