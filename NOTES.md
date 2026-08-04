# kafkaos — Kafka Learning Sidebook

This is the running journal for the kafkaos learning project. Every stage gets an
entry here: what we built, the commands we ran, the concept it was meant to teach,
and what we actually observed. Read top to bottom to retrace the whole journey.

**Project shape**: an e-commerce order flow (orders → payments → inventory →
shipping), built incrementally with Node.js + TypeScript (`kafkajs`), against a
local single-broker Kafka cluster running in Docker.

---

## Roadmap

Stages are built one at a time, on request — nothing below is built ahead of time.

- [x] **Stage 0** — Environment setup
- [x] **Stage 1** — Topics & partitions
- [x] **Stage 2** — Basic producer
- [x] **Stage 3** — Basic consumer
- [x] **Stage 4** — Consumer groups & rebalancing
- [x] **Stage 5** — Delivery semantics (at-most/at-least/exactly-once, acks, idempotence)
- [x] **Stage 6** — Multi-service event flow (payments, inventory, shipping topics)
- [x] **Stage 7** — Schema Registry (Avro/JSON Schema, schema evolution)
- [x] **Stage 8** — Kafka Connect (sink + source connectors)
- [x] **Stage 9** — Stream processing with ksqlDB (windowed aggregations, joins)
- [x] **Stage 10** — Transactions / exactly-once semantics
- [x] **Stage 11** — Monitoring & operations (lag, multi-broker, leader election, ISR)
- [x] **Stage 12** — Failure testing (kill brokers/consumers, verify guarantees hold) — end of Part 1

## Roadmap — Part 2: Scale & Performance

Part 1 (Stages 0–12) was about *breadth* — the features and failure modes of
Kafka. Part 2 is about *scale* — how the same mechanisms behave, and what
changes, when the volume of data goes from "a few test messages" to "millions
of messages," and how the right approach depends on the shape of the data
itself (key cardinality, statefulness, message size), not just the volume.

Revised after research into current (2026) real-world practice — one real
correction to the original plan (Stage 14) and one new centerpiece stage
added (Stage 15), specifically aimed at this project's actual goal: the
professional ability to receive and analyze millions of messages, not just
survive them.

- [x] **Stage 13** — Load testing & producer/consumer tuning: real throughput
  measurement on our own cluster (messages/sec, MB/sec), then tune
  `batch.size`, `linger.ms` (~10–100ms is the usual sweet spot),
  `compression.type` (`zstd` is the modern default — better ratio than `lz4`
  at acceptable CPU cost on Kafka 3.0+), and `acks`, measuring the actual
  before/after difference, not the theoretical one.
- [x] **Stage 14** — Partitioning strategy under skewed data: deliberately
  create a hot-key scenario and prove the counter-intuitive fact directly —
  **adding more partitions does not fix a single hot key**, since that key
  always hashes to the same partition regardless of partition count. Fix it
  with key salting instead, and measure the improvement. Also apply the
  practical methodology found in research: measure real key distribution,
  simulate partition assignment before committing to a key, keep any single
  partition under ~20% of total traffic, and overprovision partition count to
  ~2–3x the planned consumer instance count.
- [x] **Stage 15** — Kafka → ClickHouse: a real analytics pipeline for
  millions of events. The centerpiece of "receive and analyze millions of
  messages" as an actual professional capability, not just an ingestion
  exercise — directly extends Stage 8's Postgres/Connect work with an
  OLAP-oriented store built for exactly this. Stand up ClickHouse, wire it to
  a Kafka topic via the canonical pattern (Kafka Engine table → Materialized
  View → `MergeTree` storage table), ingest millions of synthetic events, and
  run real analytical aggregation queries against them — compare against the
  alternative of a Kafka Connect ClickHouse sink connector.
- [x] **Stage 16** — Scaling stateful processing: extend the Stage 9 ksqlDB
  (or raw Kafka Streams) work to see how state (backed by RocksDB locally)
  gets distributed/rebalanced as processing instances are added or removed
  under real load — co-partitioning requirements become sharper at scale,
  not easier.
- [x] **Stage 17** — Redpanda: same code, different broker. Redpanda speaks
  the exact same Kafka wire protocol — our existing `kafkajs` code should run
  against it with zero code changes, just a different Docker image in
  `docker-compose.yml`. The point isn't "switch platforms," it's a cheap,
  concrete way to see which parts of what we've learned are *Kafka-the-API*
  versus *Kafka-the-implementation*: rerun Stage 13's load tests unmodified
  against Redpanda and compare real numbers and operational feel (C++/
  thread-per-core vs. JVM, single binary, no separate controller quorum to
  reason about). Worth noting going in: Kafka 4.0's KRaft mode already closed
  much of the operational-simplicity gap Redpanda was originally built to
  exploit — part of this stage is checking whether that's still true.
- [x] **Stage 18** — Distributed tracing with OpenTelemetry: extend the
  Stage 6 pipeline (`orders → payments → inventory → shipping`) with real
  distributed tracing, not just the lag/replication monitoring from Stage 11.
  Kafka has no built-in equivalent of an HTTP request header for this —
  context has to be propagated explicitly through message headers
  (`traceparent`/`tracestate`), injected by the producer and extracted by the
  next consumer, so a single order can be traced end-to-end across all four
  services as one connected trace instead of four disconnected logs.
- [x] **Stage 19** — Prometheus + Grafana monitoring: the missing piece from
  Stage 11's operations work — that stage used CLI/Kafka UI to check lag
  and ISR by hand, on demand. This stage builds the thing you'd actually run
  continuously in production: scrape broker JMX metrics (and consumer lag)
  with Prometheus, and build real Grafana dashboards for lag, throughput, and
  ISR health — the concrete, demonstrable answer to "do you know how to
  operate this in production," distinct from OpenTelemetry (Stage 18, which
  traces one message's journey) — this is fleet-level visibility, not
  single-request tracing.
- [x] **Stage 20** — Large payloads & the claim-check pattern (bonus): prove
  why huge messages hurt throughput/latency, then implement
  store-a-reference-not-the-blob (payload in S3/blob storage, Kafka carries
  just a pointer).

### Considered, deliberately not added: Apache Pulsar

Pulsar separates storage (Apache BookKeeper) from compute (stateless
brokers) and offers genuine multi-tenancy and geo-replication strengths Kafka
doesn't match as directly. But it's an architecturally different system, not
API-compatible with Kafka (unlike Redpanda) — adding it properly would mean
a parallel implementation, not a cheap extension. Research is consistent that
**Kafka remains the stronger fit specifically for high-throughput event
streaming with predictable scaling needs — financial systems and real-time
pipelines**, which is this project's own domain (and this project owner's
actual professional background: insurance/fintech backend work). Pulsar's
strengths — multi-tenancy, geo-replication, diverse cloud-native messaging
patterns — matter more for a different kind of platform team problem.
Revisit if a concrete reason to need those specific strengths ever comes up.

### Sources consulted for this revision

- [Kafka Performance Tuning Guide — Conduktor](https://www.conduktor.io/glossary/kafka-performance-tuning-guide)
- [How to Tune Kafka for Million Messages Per Second — OneUptime](https://oneuptime.com/blog/post/2026-01-25-tune-kafka-million-messages-per-second/view)
- [Maximizing Kafka Throughput — RisingWave](https://risingwave.com/blog/maximizing-kafka-throughput-a-comprehensive-guide/)
- [Kafka Partition Strategy: How to Design for Scale — Zeliot](https://www.zeliot.in/blog/kafka-partition-strategy-how-to-design-for-scale)
- [Apache Kafka Partition Strategy — Confluent](https://www.confluent.io/learn/kafka-partition-strategy/)
- [Handling Hot Partitions in Kafka — Medium](https://medium.com/@natesh.somanna/handling-hot-partitions-in-kafka-c7b41b36c929)
- [Kafka to ClickHouse: 3 Ingestion Methods Compared — Glassflow](https://www.glassflow.dev/blog/kafka-to-clickhouse)
- [How Braze rebuilt its real-time analytics pipeline with ClickHouse Cloud](https://clickhouse.com/blog/how-braze-rebuilt-real-time-analytics-pipeline-with-clickHouse-cloud)
- [Real-Time User Behavior Analytics at Scale with Kafka and ClickHouse — Medium](https://medium.com/@alireza.mousavizade/real-time-user-behavior-analytics-at-scale-with-kafka-and-clickhouse-cf3107a30728)
- [Scaling Kafka Streams for High-Volume Data Processing — Confluent](https://www.confluent.io/blog/scaling-kafka-streams/)
- [How Real-Time Stream Processing Safely Scales with ksqlDB — Confluent](https://www.confluent.io/blog/how-real-time-stream-processing-safely-scales-with-ksqldb/)
- [Apache Kafka vs. Apache Pulsar: Differences & Comparison — AutoMQ](https://www.automq.com/blog/apache-kafka-vs-apache-pulsar-differences-comparison)
- [Kafka vs Pulsar: Architecture Compared — Conduktor](https://www.conduktor.io/glossary/kafka-vs-pulsar)
- [Apache Kafka® vs. Apache Pulsar™ Comparison — Instaclustr](https://www.instaclustr.com/blog/kafka-versus-pulsar/)
- [Redpanda vs Kafka: Architecture, Trade-offs — Conduktor](https://www.conduktor.io/glossary/redpanda-vs-kafka)
- [Redpanda vs Kafka 2026: Real Latency & Cost Analysis — AutoMQ](https://www.automq.com/blog/redpanda-vs-kafka-benchmark-cost-analysis)
- [Kafka vs Redpanda: Real Benchmarks on Identical Hardware — ComputingForGeeks](https://computingforgeeks.com/kafka-vs-redpanda-benchmarks/)
- [Kafka with OpenTelemetry: Distributed Tracing Guide — Last9](https://last9.io/blog/kafka-with-opentelemetry/)
- [Distributed Tracing for Kafka Applications — Conduktor](https://www.conduktor.io/glossary/distributed-tracing-for-kafka-applications)

## Roadmap — Part 3: Distributed Data Patterns

Part 1 was breadth, Part 2 is scale. Part 3 is *correctness patterns* for
distributed systems built on Kafka — the standard, named answers to problems
this project has already run into concretely: duplicate delivery (Stage 5),
the dual-write problem (implicit whenever a service needs to update its own
database *and* publish an event), and multi-service business processes with
no single distributed transaction to rely on (Stage 6's whole pipeline).

- [x] **Stage 21** — Idempotent consumers & deduplication: redo Stage 5's
  at-least-once crash experiment, but this time with a dedup layer (a
  processed-IDs table with a unique constraint, checked and updated in the
  same transaction as the side effect). Prove the exact same duplicate
  delivery this time produces **no duplicate effect** — only a harmlessly
  skipped reprocessing attempt.
- [x] **Stage 22** — Retry policies & DLQ with alerting: extend Stage 6's
  services with a real application-level resilience pattern for a downstream
  failure (e.g. a database write failing) — retry with backoff a bounded
  number of times, and only after exhausting retries, route the message to a
  dead-letter topic **and fire an alert** (not just log it, like Stage 8's
  Connect-level DLQ did for malformed messages). Deliberately placed right
  after Stage 21: safe retries depend on the consumer already being
  idempotent, otherwise a retry is just a slower way to create the Stage 5
  duplicate-effect problem again.
- [x] **Stage 23** — Transactional Outbox Pattern: solve the dual-write
  problem for a service that needs to update its own database *and*
  reliably publish a Kafka event as one atomic unit. Write the business
  change and an outbox row in the same local DB transaction, then use
  Debezium (a Kafka Connect source connector purpose-built for CDC) to
  reliably publish outbox rows to Kafka. Prove it survives a crash between
  the DB commit and the publish — nothing lost, unlike a naive
  "write-DB-then-call-Kafka" implementation.
- [x] **Stage 24** — Saga Pattern: extend Stage 6's `orders → payments →
  inventory → shipping` chain — which is already a choreography-style saga
  — with the compensating path it's currently missing. Right now,
  `inventory-service` just skips a failed payment; it never *reverses* a
  payment that succeeded when inventory reservation fails afterward. Add
  that compensating `refund-payment` flow, trace both the happy path and the
  failure/compensation path through the topics, and compare against an
  orchestration-style version (one service explicitly sequencing every step
  and every compensation, instead of each service reacting independently).

## Roadmap — Part 4: The Capstone

Parts 1–3 are 24 individually-learned stages. Part 4 is not a new technical
topic — it's **assembly**: taking the pieces that are actually meant to work
*together* and combining them into one coherent, named, production-shaped
reference system, instead of 24 separate exercises that happen to share a
repo. This is the thing to actually walk through in an interview or point to
from a CV — not "I did 24 Kafka exercises," but "here's a distributed system
I built, and here's exactly how it handles duplicate messages, downstream
failures, and partial rollbacks, and here's the dashboard that shows it
working."

- [x] **Stage 25 — The Capstone**: assemble a single, named, cohesively
  documented system out of the pieces already built for exactly this
  purpose:
  - **Business backbone**: Stage 6's four services (`order` / `payment` /
    `inventory` / `shipping`), unchanged in intent
  - **Reliable event publishing**: Stage 23's Transactional Outbox, so each
    service's own DB write and its published event are never inconsistent
  - **Consumer resilience**: Stage 21's idempotent consumers + Stage 22's
    retry/DLQ/alerting, so failures degrade gracefully instead of silently
    corrupting or duplicating state
  - **Correctness under partial failure**: Stage 24's Saga compensation path
    (a failed inventory reservation actually refunds the payment, not just
    logs a skip)
  - **Observability**: Stage 18's OpenTelemetry tracing (follow one order
    end-to-end across all four services as a single trace) + Stage 19's
    Prometheus/Grafana dashboards (fleet-level lag/throughput/ISR health)
  - **Scale-informed configuration**: producer/consumer settings and
    partitioning choices that reflect what Stages 13–17 actually measured,
    not defaults
  - **A real deliverable, not just code**: a dedicated top-level
    `ARCHITECTURE.md` (or a rewritten `README.md`) describing this as one
    product with a name, an architecture diagram, and a single
    `docker compose up` path to seeing it work end-to-end — written for
    someone who has never read `NOTES.md`, unlike everything else in this
    repo, which assumes the reader is following the whole journey.

  Naming and the exact architecture diagram are still open — worth deciding
  once Stages 13–24 are actually built and we know precisely what's real
  enough to assemble, rather than designing the capstone before the parts
  that compose it exist.

---

## Stage 0 — Environment setup

**Date**: 2026-07-30

**Goal**: Get a local Kafka cluster running that we can see into, and a Node/TS
project wired up to talk to it — no Kafka-specific code yet, just the foundation.

### What we set up

**`docker-compose.yml`** — two containers:

- `kafka` (`apache/kafka:3.8.0`), running in **KRaft mode**: a single node acting as
  both broker and controller. Modern Kafka (3.x+) no longer requires ZooKeeper —
  KRaft mode uses Kafka's own Raft-based consensus for cluster metadata, which is
  simpler to run for learning purposes and is the direction Kafka is moving overall.
  - Three listeners are configured: `PLAINTEXT` (port 19092, internal — used by other
    containers like Kafka UI), `CONTROLLER` (port 9093, KRaft consensus traffic), and
    `PLAINTEXT_HOST` (port 9092, exposed to the host machine — this is what our
    Node.js code and CLI tools will connect to from outside Docker).
  - Replication factor is set to 1 everywhere (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR`,
    `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR`) because we only have one broker.
    This will change in Stage 11 when we add more brokers to see replication/leader
    election in action.
- `kafka-ui` (`provectuslabs/kafka-ui`), a web UI on `localhost:8080` for browsing
  topics, partitions, consumer groups, and individual messages without needing the
  CLI for everything. It talks to Kafka over the internal `PLAINTEXT` listener
  (`kafka:19092`), while we'll use `localhost:9092` from the host.

**`package.json` / `tsconfig.json`** — minimal Node.js + TypeScript scaffold:

- `kafkajs` — the Kafka client library we'll use for all producer/consumer code
  from Stage 2 onward. Pure JS/TS, no native bindings, good docs, widely used.
- `ts-node` / `typescript` — so we can run `.ts` files directly during development.
- `src/index.ts` — a placeholder file (just logs a string) whose only job right now
  is to prove the TypeScript build pipeline works end to end. Real Kafka code starts
  in Stage 1.

### Commands used

```bash
docker compose up -d          # start kafka + kafka-ui
docker compose ps             # confirm both containers are "healthy"/"Up"
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080   # 200 = Kafka UI reachable
npm install                   # install kafkajs, typescript, ts-node
npx tsc --noEmit               # confirm the TS scaffold compiles cleanly
```

### What we observed

- `docker compose ps` showed `kafka` as `healthy` (via its healthcheck, which runs
  `kafka-broker-api-versions.sh` against the broker) and `kafka-ui` as `Up`.
- `http://localhost:8080` returned `200` — Kafka UI is reachable and (at this point)
  shows the cluster `kafkaos-local` with zero topics, since nothing has produced to
  it yet.
- `npx tsc --noEmit` compiled `src/index.ts` with no errors.

### Key concepts introduced

- **KRaft vs ZooKeeper**: KRaft is Kafka's newer self-managed metadata/consensus
  mechanism, replacing the ZooKeeper dependency older Kafka versions required.
- **Broker vs controller role**: in KRaft mode a node can play both roles at once
  (as ours does); in bigger clusters these are often split across dedicated nodes.
- **Listeners**: Kafka distinguishes the address a client connects to from the
  address a broker is reachable at internally — this is why we have separate
  `PLAINTEXT` (Docker-internal) and `PLAINTEXT_HOST` (host-facing) listeners. This
  trips people up constantly in real deployments, so worth internalizing early.

### Next up

Stage 1 — create the `orders` topic, look at partition count and replication
factor, and understand what a partition actually is under the hood (a directory of
append-only segment files on disk, if you want to go one level down).

---

## Stage 1 — Topics & partitions

**Date**: 2026-07-30

**Goal**: Create our first real topic and understand what a "partition" concretely
is — both at the metadata level (what the cluster reports) and at the storage level
(what actually exists on disk inside the broker container).

### What we did

Created the `orders` topic with 3 partitions and replication factor 1:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic orders \
  --partitions 3 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
```

Described it to see per-partition metadata:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

```
Topic: orders   TopicId: GU2DvhOQSrmi2vnG62Epog   PartitionCount: 3   ReplicationFactor: 1
    Partition: 0   Leader: 1   Replicas: 1   Isr: 1
    Partition: 1   Leader: 1   Replicas: 1   Isr: 1
    Partition: 2   Leader: 1   Replicas: 1   Isr: 1
```

Listed all topics — only `orders` shows up, no `__consumer_offsets` or other
internal topics yet:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Looked at what a partition actually *is* on disk. Kafka's default `log.dirs` on
this image is `/tmp/kafka-logs`; each partition is literally its own directory
named `<topic>-<partitionNumber>`:

```bash
docker exec kafka ls /tmp/kafka-logs | grep orders
# orders-0  orders-1  orders-2

docker exec kafka ls -la /tmp/kafka-logs/orders-0
# 00000000000000000000.index          <- maps offsets -> byte position in the .log file
# 00000000000000000000.log            <- the actual append-only message segment (0 bytes, empty so far)
# 00000000000000000000.timeindex      <- maps timestamps -> offsets
# leader-epoch-checkpoint             <- tracks leader changes, used for consistency after failover
# partition.metadata                  <- partition/topic id metadata
```

The `00000000000000000000` prefix is the **base offset of the segment** — a
partition's log is split into segments as it grows, and this is the first (and so
far only) one, starting at offset 0.

Then deliberately tried to break replication, to make the constraint concrete
rather than abstract:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create \
  --topic test-rf-demo --partitions 1 --replication-factor 2 \
  --bootstrap-server localhost:9092
```

```
Error: Unable to replicate the partition 2 time(s): The target replication factor
of 2 cannot be reached because only 1 broker(s) are registered.
```

The command failed cleanly and no topic was created (confirmed via `--list`
afterward) — replication factor is a hard ceiling on broker count, not a request
Kafka can partially satisfy.

### What we observed

- A topic is metadata (name, partition count, replication factor, configs) plus,
  for every partition, an actual directory of segment files on the broker(s) that
  host it.
- With `replication-factor 1`, `Leader` and `Replicas` are just `[1]` (our single
  broker) — there's no redundancy yet. We'll revisit this in Stage 11 with a
  multi-broker cluster, where `Replicas` will list multiple brokers and `Isr`
  (in-sync replicas) becomes meaningful to watch.
- Internal topics like `__consumer_offsets` don't exist yet — Kafka creates them
  lazily the first time they're needed (first consumer group), which we'll trigger
  in Stage 3/4.

### Key concepts introduced

- **Partition**: the unit of parallelism and ordering in Kafka. Order is only
  guaranteed *within* a partition, never across partitions of a topic.
- **Segments**: a partition's log isn't one giant file — it's split into segment
  files (`.log` + `.index` + `.timeindex`) that roll over based on size/time
  config, which is what makes log retention/deletion efficient (drop whole old
  segments instead of rewriting a file).
- **Replication factor vs broker count**: RF can never exceed the number of
  brokers in the cluster — Kafka enforces this at topic-creation time.
- **Leader / Replicas / Isr**: every partition has one leader broker (handles all
  reads/writes for it) and a set of replicas; Isr is the subset of replicas that
  are currently caught up enough to be eligible for leader election.

### Next up

Stage 2 — write a basic producer in TypeScript that sends order events (JSON) to
the `orders` topic, keyed by order id, and watch how keys map to specific
partitions.

---

## Stage 2 — Basic producer

**Date**: 2026-07-30

**Goal**: Write a real producer in TypeScript, send keyed JSON order events to
`orders`, and directly observe the key → partition mapping (and that it's
*consistent* for a given key, not random per send).

### What we built

- **`src/shared/kafka.ts`** — a shared `Kafka` client instance (`clientId: "kafkaos"`,
  `brokers: ["localhost:9092"]`) that later stages (consumer, etc.) will import
  too, instead of each file constructing its own client.
- **`src/shared/types.ts`** — the `Order` shape (`orderId`, `customerId`, `items`, `total`,
  `status`) shared across producer/consumer code.
- **`src/stage02-producer/producer.ts`** — connects a `kafkajs` producer, sends 6 distinct orders
  (`order-1`..`order-6`) each keyed by its `orderId`, then **resends** `order-1`
  and `order-2` with an updated status to check whether the same key maps to the
  same partition on a second, independent send.

One deliberate config choice: `kafka.producer({ createPartitioner:
Partitioners.DefaultPartitioner })`. kafkajs changed its default partitioner in
v2.0.0 to match the Java client's murmur2-based hashing, and emits a startup
warning unless you opt in explicitly — so we set it explicitly rather than
silently depend on a default that could differ across library versions.

### Commands used

```bash
npm run produce   # runs `ts-node src/stage02-producer/producer.ts`

# independent verification, reading straight off the broker rather than
# trusting the producer's own reported partitions:
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --timeout-ms 5000
```

### What we observed

Producer's own output:

```
sent order-1 -> partition 1
sent order-2 -> partition 0
sent order-3 -> partition 0
sent order-4 -> partition 2
sent order-5 -> partition 2
sent order-6 -> partition 0
resent order-1 -> partition 1
resent order-2 -> partition 0
```

Independently read back from the broker via the console consumer — partitions
and keys matched exactly, and messages within each partition came back in the
exact order they were sent (e.g. `order-2` "created" then `order-2` "updated",
both in partition 0, in that order):

```
Partition:0  order-2  {...status:"created"}
Partition:0  order-3  {...}
Partition:0  order-6  {...}
Partition:0  order-2  {...status:"updated"}
Partition:1  order-1  {...status:"created"}
Partition:1  order-1  {...status:"updated"}
Partition:2  order-4  {...}
Partition:2  order-5  {...}
```

- `order-1` landed on partition 1 **both times**; `order-2` landed on partition 0
  **both times**. Same key → same partition, deterministically, because kafkajs
  hashes the key (murmur2) and mods by partition count — no randomness, no
  round-robin when a key is present.
- Keys were spread unevenly across the 3 partitions with only 6 distinct keys
  (partition 0 got 3, partition 1 got 1, partition 2 got 2) — expected with a
  small sample size and hash-based distribution; it evens out statistically over
  many distinct keys, not over few.

### Key concepts introduced

- **Key-based partitioning**: providing a message `key` routes it deterministically
  to one partition (via a hash of the key), which is how Kafka gives you ordering
  *per key* even though a topic overall has no global order across partitions.
- **No key = round robin (sticky batches)**: if you omit the key, kafkajs/Kafka
  spread messages across partitions instead — no per-entity ordering guarantee at
  all in that case. We didn't demonstrate this yet; worth trying explicitly later
  by comparing keyed vs unkeyed sends.
- **Producer acks are implicit here**: `producer.send()` resolved successfully for
  every message, meaning the broker acknowledged each write — we haven't yet
  configured `acks` explicitly or looked at what happens when we don't wait for
  acknowledgment. That's the subject of Stage 5 (delivery semantics).

### Next up

Stage 3 — write a basic consumer that reads from `orders`, and look at offsets:
auto-commit vs manual commit, and what "reading from the beginning" vs "reading
new messages only" actually means under the hood.

---

## Stage 3 — Basic consumer

**Date**: 2026-07-30

**Goal**: Write a consumer, understand what an **offset** actually controls, and
see the difference between auto-commit and manual commit directly — not just read
about it.

### What we built

**`src/stage03-04-consumer-groups/consumer.ts`** — a `kafkajs` consumer, configurable via env vars so we could
run the same code under different conditions without duplicating files:

- `GROUP_ID` (default `order-processor`) — which consumer group to join
- `AUTO_COMMIT` (default `true`) — whether kafkajs auto-commits offsets
  periodically, or we commit manually after each message via
  `consumer.commitOffsets()`
- `RUN_MS` (default `5000`) — how long to stay connected before disconnecting
  (a real consumer runs forever; we cap it so each run is a bounded experiment)

Subscribes with `fromBeginning: true` — but as the experiments below show, that
flag only matters the *first* time a given group ever reads the topic.

### Experiments run

**1. Fresh group, default auto-commit** (`npm run consume`, group
`order-processor`) — read all 8 backlog messages from Stage 2, one line per
message showing partition/offset/key/value, then disconnected after 5s.

**2. Same group, run again immediately** — `consumed 0 message(s) this run`.
Nothing to read: the group's offsets were already committed at the end of the
log, so it resumed from there — `fromBeginning: true` had no effect this time.
Confirmed via CLI:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order-processor
```
```
GROUP            TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-processor  orders  0          4               4               0
order-processor  orders  1          2               2               0
order-processor  orders  2          2               2               0
```

**3. Produced a second batch** (`npm run produce` again — same 8 logical
messages, but new offsets since it's an append-only log) — then checked lag
*before* consuming:

```
GROUP            TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
order-processor  orders  0          4               8               4
order-processor  orders  1          2               4               2
order-processor  orders  2          2               4               2
```

`LAG` = `LOG-END-OFFSET - CURRENT-OFFSET`: how many messages this group hasn't
consumed yet, per partition. This is the number you'd alert on in production.

**4. Fresh group, manual commit** (`GROUP_ID=order-processor-manual
AUTO_COMMIT=false npm run consume`) — being a brand-new group, it read from the
beginning: all 16 messages (both batches). Every single message was followed by
an explicit `commitOffsets()` call, logged individually (e.g. `-> manually
committed partition=2 offset=1`).

**5. Compared final state of both groups**:

```
order-processor          (auto)    0:4/8 lag4   1:2/4 lag2   2:2/4 lag2
order-processor-manual   (manual)  0:8/8 lag0   1:4/4 lag0   2:4/4 lag0
```

`order-processor` still shows lag because we never re-ran *it* after the second
`produce` — each consumer group's progress is tracked completely independently.
`order-processor-manual` caught all the way up because it read everything from
scratch and committed after every message.

**6. `__consumer_offsets` now exists** — back in Stage 1 we noted it didn't exist
yet because no consumer group had ever run. Listing topics now shows it:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
# __consumer_offsets
# orders
```

This is where Kafka itself stores every group's committed offsets — the CLI
`--describe --group` command above is really just reading this topic.

### Key concepts introduced

- **Offset**: a per-partition, strictly increasing integer identifying a message's
  position in the log. Offsets are meaningless across partitions — partition 0
  offset 4 and partition 1 offset 4 are unrelated messages.
- **Committed offset**: the position a *consumer group* has recorded as "processed
  up to here" for a partition. This is what determines where a group resumes,
  not `fromBeginning` — that flag is only consulted when the group has *no*
  prior committed offset at all.
- **Auto-commit vs manual commit**: auto-commit (kafkajs default) periodically
  commits progress in the background — simpler, but you can lose track of
  exactly what's "safely processed" if your app crashes between commits.
  Manual commit gives precise control over exactly when an offset is considered
  done, at the cost of writing that logic yourself. This is the foundation for
  Stage 5 (delivery semantics) — auto-commit-before-processing risks
  at-most-once loss, commit-after-processing risks at-least-once duplicates.
- **Consumer lag**: `LOG-END-OFFSET - CURRENT-OFFSET` per partition, the standard
  production health metric for "is this consumer keeping up."
- **`__consumer_offsets`**: offsets aren't stored in some external system — they
  live in Kafka itself, in an internal topic, created lazily on first use.

### Next up

Stage 4 — consumer groups & rebalancing: run multiple consumer instances in the
same group at once, and watch how Kafka splits the 3 `orders` partitions across
them — then kill one and watch the rebalance happen live.

---

## Stage 4 — Consumer groups & rebalancing

**Date**: 2026-07-30

**Goal**: Run several consumer instances in the same group at once, watch Kafka
split the 3 `orders` partitions across them as membership changes, and see what
actually happens — not what's assumed to happen — when a member dies.

### What we changed

Extended `src/stage03-04-consumer-groups/consumer.ts` with:

- `INSTANCE_ID` env var — just a log tag, so multiple instances running at once
  are distinguishable in their (separate) log files.
- `SESSION_TIMEOUT_MS` env var (default `10000`) — passed as kafkajs's
  `sessionTimeout`. Default kafkajs/Kafka session timeout is 30s; lowered here so
  failure detection would be fast enough to observe within this exercise.
- A listener on `consumer.events.GROUP_JOIN`, logging `payload.memberAssignment`
  every time this instance (re)joins the group — this is what let us see
  partition assignment change in real time, from inside each process.

Each instance runs as its own OS process (`npx ts-node src/stage03-04-consumer-groups/consumer.ts`),
launched in the background with output redirected to its own log file — this is
what actually simulates independent service instances, as opposed to threads in
one process.

### Experiment 1 — scaling up (join-triggered rebalances)

Fresh group `order-processor-rebalance` (renamed after a first pass at
`order-processor-scaled`, kept below).

1. **Instance A alone** joins → assigned **all 3 partitions**: `{"orders":[0,1,2]}`.
2. **Instance B joins** → immediate rebalance → **A**: `[0,1]`, **B**: `[2]`.
3. **Instance C joins** → another rebalance → **A**: `[1]`, **B**: `[0]`,
   **C**: `[2]` — one partition per instance.

Cross-checked step 3 independently via the broker itself, matching our
consumer-ID-to-partition mapping exactly:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group order-processor-scaled
```
```
PARTITION  CONSUMER-ID
2          kafkaos-1dd445ec-...df56   (= instance C)
0          kafkaos-ac8297f3-...8edd   (= instance B)
1          kafkaos-13561cbe-...5584   (= instance A)
```

Every join triggered a full **stop-the-world rebalance** (`groupProtocol:
"RoundRobinAssigner"` in the logs) — all members pause, re-negotiate, and get a
fresh assignment, even though only one instance actually changed. This is the
"eager" rebalance protocol; Kafka has since added cooperative/incremental
rebalancing to avoid pausing members that don't need to move, which we may
revisit later.

### Experiment 2 — killing a member (the actual surprise)

Fresh group `order-processor-rebalance` (second run), `SESSION_TIMEOUT_MS=10000`
on both instances:

1. **A2** joins alone → `[0,1,2]`.
2. **B2** joins → rebalance → **A2**: `[0,1]`, **B2**: `[2]`.
3. `kill -9` on B2's process at **11:53:14** — simulating a hard crash (no
   `LeaveGroup` request sent, unlike a graceful shutdown).

**Expectation**: within `sessionTimeoutMs` (10s) plus a small margin, the broker
notices B2 stopped heartbeating, evicts it, and rebalances partition `2` onto A2.

**What actually happened**: it didn't, for over four minutes. Polling
`kafka-consumer-groups.sh --describe` repeatedly showed partition `2` **still
"owned" by B2's dead member ID** the entire time, and A2 kept running normally on
just `[0,1]`, completely unaware anything was wrong — it never needed to interact
with B2's group state, so nothing prompted the broker to act on the missed
heartbeats. The full broker-log timeline confirms it:

```
11:52:49  A2 joins alone                              → generation 1
11:53:00  B2 joins                                     → generation 2 (A2:[0,1], B2:[2])
11:53:14  (B2 killed — kill -9, no LeaveGroup sent)
  ...     [nothing happens for 4.5 minutes — B2 still listed as owner of partition 2]
11:57:53  A2's own 300s run timer fires, calls consumer.disconnect()
          → "Removing member A2 on LeaveGroup" → generation 3, 1 member (B2!)
          → B2 (dead) is now the sole member and group "leader" — but obviously
            can never respond with the SyncGroup its own dead process owes
11:57:58  generation 3 stabilizes, waiting on B2's (impossible) SyncGroup
11:58:03  B2 finally reaped → "Removing member B2 on LeaveGroup" → group empty
```

So B2's stale session was only actually cleaned up as a **side effect of A2's
unrelated, graceful shutdown** five minutes later — not by the session-timeout
watchdog firing independently, which is what the `sessionTimeoutMs=10000` config
would lead you to expect. The dead member's session was only ever "noticed" once
something else forced the group to renegotiate.

### Key concepts introduced

- **Partition assignment is per-group, recomputed on every membership change**:
  any join or leave triggers a full rebalance for that group (with this eager
  protocol), even if most members' assignments end up unchanged.
- **Graceful shutdown ≠ crash, and Kafka treats them very differently.** A
  process that calls `consumer.disconnect()` sends an explicit `LeaveGroup`
  request, and the broker reacts in seconds. A process that's killed (`SIGKILL`,
  crash, OOM) sends nothing — the broker can only find out via the session
  timeout mechanism, and as we saw, that mechanism can be **far less prompt in
  practice than the configured value suggests**, especially if no other group
  activity happens to surface the stale state.
- **A "dead" partition owner can silently stall processing.** If new messages
  had arrived on partition 2 during those 4.5 minutes, nobody would have been
  consuming them — Kafka still considered it B2's, and A2 had no reason to
  suspect otherwise. This is a real operational risk, not just a lab curiosity.
- **Practical implication**: production consumers should handle `SIGTERM`
  explicitly (call `consumer.disconnect()` before exiting) so planned restarts
  and deploys trigger the fast, graceful path — rather than relying on session
  timeout to catch every shutdown. We didn't add that handler here; worth doing
  before Stage 12 if we want to compare the two paths side by side deliberately
  rather than stumbling into it.
- **`sessionTimeout` is necessary but not provably sufficient on its own** to
  guarantee prompt failure detection — this is exactly the kind of thing worth
  re-testing more rigorously and skeptically in Stage 12 (Failure Testing),
  ideally with more controlled tooling than wall-clock polling.

### Next up

Stage 5 — delivery semantics: at-most-once vs at-least-once vs exactly-once,
`acks`, and the idempotent producer — building directly on what we now know
about commits (Stage 3) and how unreliable failure detection can be in practice
(this stage).

---

## Stage 5 — Delivery semantics

**Date**: 2026-07-30

**Goal**: Turn "at-most-once / at-least-once / exactly-once" from vocabulary into
something we've actually watched happen — a real lost message, a real duplicate,
a real `acks` latency difference, and the real boundary of what an idempotent
producer protects against.

### What we built

- **`src/stage05-delivery-semantics/producer-acks.ts`** — sends N messages at a configurable `acks` level
  (`0`, `1`, or `-1`), timing the batch.
- **`src/stage05-delivery-semantics/producer-idempotence.ts`** — a producer with `idempotent: true`, sending
  the *same* logical order via two independent `send()` calls.
- **`src/stage05-delivery-semantics/consumer-semantics.ts`** — the main event. A consumer that, depending on
  `MODE`, either commits-then-works or works-then-commits, and can be told to
  crash (`process.exit(1)`) partway through via `CRASH_AFTER` — with every "unit
  of work" recorded as an appended line in a side-effect log file (`LOG_FILE`),
  standing in for something real like a DB write or a downstream API call.
- New topics: `orders-semantics` (general scratch topic) and `orders-crash-demo`
  (1 partition, so message order is simple and the crash point is deterministic)
  — both seeded via the existing `producer.ts`, now parameterized by a `TOPIC`
  env var instead of hardcoding `"orders"`.

### Experiment A — `acks`

Sent 20 messages at each level to `orders-semantics`:

```
acks=0:  20 messages in 17ms  (0.85ms/msg)
acks=1:  20 messages in 25ms  (1.25ms/msg)
acks=-1: 20 messages in 48ms  (2.40ms/msg)
```

All 60 messages landed (`kafka-get-offsets.sh` confirmed `orders-semantics:0:60`
afterward) — nothing was actually lost here. That's an important caveat on its
own: **`acks=0` not waiting for a broker response doesn't mean loss will happen,
it means you get zero guarantee and zero visibility either way.** In a stable
local test, nothing goes wrong; the risk shows up under real broker failure or
network trouble, which we're deferring to Stage 12.

The other caveat: with **replication factor 1** (our single broker), `acks=1`
(leader only) and `acks=-1`/`all` (all in-sync replicas) are durability-wise
*identical* — there's only one replica to wait for either way. The timing gap
above is most likely noise, not a real difference in work done. The actual
distinction between `acks=1` and `acks=-1` only becomes meaningful with RF>1,
which is Stage 11 territory (multi-broker cluster).

### Experiment B — idempotent producer: what it does *not* cover

Sent the same logical order (`order-idem-1`) via **two separate, independent
`send()` calls**, from a producer configured with `idempotent: true`:

```
send #1 -> partition 0 offset 60
send #2 (application-level "retry") -> partition 0 offset 61
```

Two distinct offsets — both landed as separate messages. **Idempotence did not
dedupe this.** That's expected and important to internalize: kafkajs/Kafka's
idempotent producer assigns a `(producerId, epoch, sequenceNumber)` to each
batch and lets the broker discard a *retry of that exact batch* (e.g. the
producer's own internal retry after an ack was lost on the wire, so the client
doesn't know if the broker actually got it). It has no way to know that two
separate `send()` calls represent "the same" business event — that's an
application-level concern, solved differently (e.g. a dedup key downstream, or
by not blindly resending at the app layer at all).

### Experiment C — at-most-once vs at-least-once, by actually crashing

Seeded `orders-crash-demo` with 8 messages (`order-1`..`order-6`, then
`order-1`/`order-2` resent) — offsets 0 through 7, one partition, so the crash
point is unambiguous.

**At-most-once** (`MODE=at-most-once`): commit the offset, *then* do the work.
Configured to crash right after committing offset 3 (for `order-3`, offset 2):

```
processed order-1 (offset 0)
processed order-2 (offset 1)
[CRASH] committed offset=3 for key=order-3, exiting BEFORE doing the work
```

Side-effect log after the crash: only `order-1` and `order-2` — `order-3`'s work
never ran. Restarted the **same consumer group** (no code changes, no
`CRASH_AFTER` this time):

```
processed order-4 (offset 3)   <- resumed here, NOT at offset 2
processed order-5 (offset 4)
processed order-6 (offset 5)
processed order-1 (offset 6)
processed order-2 (offset 7)
```

Final log: 7 entries, offsets `0,1,3,4,5,6,7` — **offset 2 (`order-3`) is gone
forever.** The offset was already committed before the crash, so Kafka has no
idea anything was missed. This is at-most-once, concretely: the work either
happens once, or not at all — never twice, but sometimes zero times.

**At-least-once** (`MODE=at-least-once`): do the work, *then* commit. Same crash
point (message 3 = `order-3`, offset 2):

```
processed order-1 (offset 0)
processed order-2 (offset 1)
[CRASH] did the work for key=order-3, exiting BEFORE committing offset=3
```

Restarted the same group:

```
processed order-3 (offset 2)   <- reprocessed, same offset as before the crash
processed order-4 (offset 3)
...
```

Final log has **9 lines** for 8 messages — `order-3` (offset 2) appears **twice**,
written by two different process IDs (one before the crash, one after restart).
That's at-least-once, concretely: the work happens at least once, possibly more.

One more thing surfaced for free: the restart's `GROUP_JOIN` event reported
`duration: 22779` (almost 23 seconds to join) — an echo of the Stage 4 finding.
The crashed process never sent `LeaveGroup`, so the new join had to wait out the
old member's stale session before it could proceed.

### Key concepts introduced

- **At-most-once**: commit before processing. Crash between the two → silent,
  permanent loss of that message's effect. No duplicates, ever — but no
  guarantee of completion either.
- **At-least-once**: process before committing. Crash between the two →
  reprocessing on restart → duplicate effects. No loss — but the consumer (or
  something downstream) must tolerate or dedupe repeats.
- **Exactly-once** isn't a third commit-ordering trick — it requires either (a)
  idempotent *downstream* writes (e.g. upserts keyed by `orderId`, making
  at-least-once safe in effect), or (b) Kafka's transactional API tying the
  consume-offset-commit and the produce together atomically. We haven't built
  (b) yet — that's Stage 10.
- **`acks`** controls how many broker acknowledgments the *producer* waits for
  before `send()` resolves — it's about producer-to-broker durability, entirely
  separate from the consumer-side commit-ordering trade-off above.
- **Idempotent producer** solves a narrow, specific problem (broker-side
  deduplication of the producer's *own* retried requests), not a general
  "prevent duplicates" switch. Conflating the two is a common and important
  misconception.

### Next up

Stage 6 — multi-service event flow: add `payments`, `inventory`, and `shipping`
topics, and build a small chain of services that react to each other's events —
this is where the "control the flow" part of the original goal really kicks in.

---

## Stage 6 — Multi-service event flow

**Date**: 2026-08-02

**Goal**: Build an actual chain of independent services that react to each
other's events — `orders` → `payments` → `inventory` → `shipping` — and watch
what event-driven decoupling really buys you: filtering, backpressure isolation,
and (unplanned, but very real) resilience to bad data.

### What we built

- **New topics**: `payments`, `inventory`, `shipping` (3 partitions each, RF=1,
  same shape as `orders`).
- **`src/stage06-event-flow/payment-service.ts`** — consumes `orders`, simulates a payment
  (~85% succeed, 15% fail at random), produces to `payments`.
- **`src/stage06-event-flow/inventory-service.ts`** — consumes `payments`, **only reacts to
  `status === "succeeded"`**, simulates reserving inventory, produces to
  `inventory`. Accepts `SLOW_MS` to artificially delay each message — used
  deliberately to create a bottleneck.
- **`src/stage06-event-flow/shipping-service.ts`** — consumes `inventory`, creates a shipment,
  produces to `shipping`. End of the chain.
- **`Payment` carries `items` forward** (`src/shared/types.ts`) — `inventory-service`
  needs to know what to reserve, but rather than having it look the original
  order back up (a join, which is Stage 9 territory), `payment-service` just
  includes the items in the event it emits. This is **event-carried state
  transfer**: each event carries what the next stage needs, so consumers stay
  decoupled from each other's data sources, not just their code.
- We deliberately **reused the existing `orders` topic** (17 messages already
  sitting there from Stages 2–5) rather than seeding a fresh one — which
  demonstrated something for free: a brand-new consumer group (`payment-service`,
  first time ever run) subscribed `fromBeginning` and processed the **entire
  history**, not just new arrivals. That's a real Kafka property, not a
  simulation — the topic *is* the durable record, and a new service joining the
  system gets to react to everything that already happened if it wants to.

### The unplanned part: a poison pill

Partway through the first run, `payment-service` **crashed outright**:

```
[Consumer] Crash: KafkaJSNonRetriableError: Unexpected token 'h', "heyyy" is not valid JSON
```

Inspecting the exact offset that broke it:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --partition 0 --offset 8 \
  --max-messages 1 --property print.key=true --property print.timestamp=true
# CreateTime:1785416075493  Partition:0  Offset:8  null  heyyy
```

A plain string, `heyyy`, no key, timestamped from earlier in this same session —
almost certainly sent manually through Kafka UI's "Produce Message" screen while
exploring it. Not malicious, not planned, just real: **topics accumulate
whatever gets produced to them, from anyone, and consumers must be defensive
about that.** Our `eachMessage` handler did a bare `JSON.parse()` with no error
handling, so one bad message took the entire consumer down — the classic
**"poison pill"** failure mode.

**Fix**: `src/shared/util.ts` — a `safeParseJson()` helper that catches the parse
error, logs which message it's dropping and why, and returns `null` so the
caller can skip it and move on to the next message, instead of throwing.
Applied to all three services. Restarted `payment-service`: this time it hit
the same message, logged `skipping unparseable message: "heyyy"`, and kept
going — fully caught up (lag 0) right after.

This is the minimal fix (skip-and-log). A more production-grade version would
route the bad message to a **dead-letter topic** instead of just dropping it,
so it's not silently lost — worth doing if we revisit this later.

### Watching backpressure, live

Ran `inventory-service` with `SLOW_MS=2000` (2s of artificial work per
message) alongside a normal-speed `shipping-service`. Mid-flight:

- `inventory-service`'s lag on `payments` was clearly nonzero and draining
  slowly, one message every ~2s
- `payment-service` (upstream) had already finished everything, lag 0, totally
  unaffected by `inventory-service` being slow
- `shipping-service` (downstream) had almost nothing to do yet — not because
  anything was wrong, but because `inventory` (its input topic) simply didn't
  have new messages until `inventory-service` produced them

Traced a single order (`order-6`) end to end to see the delay directly:

```
orders     : created                                    (early)
payments   : succeeded, processedAt 08:32:31             (seconds after orders)
inventory  : reserved,  reservedAt  08:34:54              (2+ minutes later)
shipping   : shipped,   shippedAt   08:34:54              (same moment as inventory)
```

Over two minutes between `payments` and `inventory` for the exact same
order — purely because of everything queued ahead of it behind the artificial
slowdown. **Nothing broke, nothing errored, nothing needed retrying** — the
topic between the stages absorbed the mismatch in processing speed. That's the
core value of decoupling services with a durable log instead of calling them
synchronously: a slow downstream service creates lag, not a cascading outage.

### Final funnel, end to end

```
orders:     17   (9 + 4 + 4 across partitions)
payments:   16   (17 orders - 1 poison pill, which never became a payment attempt)
inventory:  15   (16 payment attempts - 1 that failed, filtered by inventory-service)
shipping:   15   (1:1 with inventory — shipping-service doesn't filter anything)
```

Each stage either transforms, filters, or both — visible directly as the
message count narrows going down the chain.

### Key concepts introduced

- **Chained topologies**: a service can be a consumer and a producer at once,
  turning topics into pipeline stages rather than terminal endpoints. This is
  the essence of event-driven architecture — no service calls another
  directly; they only know their own input and output topic.
- **Event-carried state transfer**: pass the data downstream needs *in the
  event itself*, rather than making downstream services look it up elsewhere.
  Reduces coupling at the cost of some payload duplication.
- **Filtering as a first-class part of the flow**: `inventory-service` choosing
  to ignore failed payments isn't a special mechanism — it's just an `if` in
  application code. Kafka doesn't route conditionally; consumers decide what
  to act on.
- **Backpressure isolation**: a slow stage in the chain accumulates lag on its
  own input topic without blocking or slowing down the stages before it. This
  is fundamentally different from a synchronous call chain, where a slow
  downstream service blocks everything upstream of it too.
- **Poison pills are a real operational hazard, not a theoretical one** — proven
  by literally hitting one, by accident, in this exact session. Defensive
  parsing (catch, log, skip) is close to the minimum bar for a consumer reading
  a topic it doesn't fully control the producers of; dead-letter topics are the
  more complete answer.
- **Replayability**: a new consumer group joining a long-lived topic doesn't
  just see "what happens from now on" — it can see everything, back to
  whatever the retention policy allows. This is a structural difference from
  most traditional message queues, where a consumer only sees messages
  produced after it connects.

### Next up

Stage 7 — Schema Registry: give `Order`, `Payment`, `InventoryReservation`, and
`Shipment` real schemas (Avro or JSON Schema) instead of "JSON we hope matches
the TypeScript interface," and see what schema evolution looks like when a
field needs to change.

---

## Stage 7 — Schema Registry

**Date**: 2026-08-02

**Goal**: Replace "JSON we hope matches the TypeScript interface" with an
actually-enforced schema, using Confluent Schema Registry — and directly watch
it accept a compatible change and reject an incompatible one, rather than just
reading about compatibility rules.

### What we added

- **`schema-registry` service** in `docker-compose.yml` — `confluentinc/cp-schema-registry:7.6.1`,
  on `localhost:8081`. It's a Confluent product but talks to our broker over
  the plain Kafka protocol (no Confluent-specific broker features needed) —
  it stores every registered schema as a message in its own internal topic
  (`_schemas`), the exact same trick Kafka itself uses for
  `__consumer_offsets`. Also wired into Kafka UI (`KAFKA_CLUSTERS_0_SCHEMAREGISTRY`)
  so schemas are browsable there too.
- **`@kafkajs/confluent-schema-registry`** (npm) — the client library that
  handles the registry HTTP calls, Avro encode/decode, and the wire format.
- **`src/stage07-schema-registry/`**:
  - `schemas/order-v1.avsc` — the initial Avro schema for `Order`
  - `schemas/order-v2.avsc` — v1 plus one new field, `discountCode` (nullable,
    default `null`)
  - `schemas/order-v3-breaking.avsc` — v2 plus one new field, `shippingAddress`
    (**no default** — this is the one designed to be rejected)
  - `registry.ts` — a shared `SchemaRegistry` client pointed at `localhost:8081`
  - `producer-avro.ts` — registers a schema (by file, via `SCHEMA_FILE` env
    var) under subject `orders-avro-value`, encodes each order with
    `registry.encode(id, order)`, sends to a new `orders-avro` topic
  - `consumer-avro.ts` — decodes with `registry.decode(buffer)` and **never
    references a specific schema file at all**

### How the wire format actually works

Every encoded message is: **1 magic byte + 4-byte schema id + Avro binary
payload**. That schema id is what `registry.decode()` reads first — it looks
up (and caches) whichever exact schema version was used to *write* that
specific message, then decodes accordingly. This is why `consumer-avro.ts`
doesn't need to know or care which schema version produced any given message;
the id travels with the data.

### Experiment 1 — register and use v1

```bash
npm run produce:avro
# using schema order-v1.avsc -> registry id=1
# sent order-avro-1 -> partition 0 | avro=52B vs json=117B
# sent order-avro-2 -> partition 0 | avro=52B vs json=114B
# sent order-avro-3 -> partition 2 | avro=52B vs json=117B
```

Avro came out **~55% smaller** than the equivalent JSON for the same data —
binary + schema-known field order beats repeating field names as text in every
single message. Confirmed the registry actually has it:

```bash
curl -s http://localhost:8081/subjects
# ["orders-avro-value"]
curl -s http://localhost:8081/schemas/ids/1   # returns the exact order-v1.avsc content
```

Decoded it back with `npm run consume:avro` — all fields round-tripped
correctly.

### Experiment 2 — compatible evolution (v2)

Added `discountCode` (nullable, `default: null`) and produced with it:

```bash
SCHEMA_FILE=order-v2.avsc npm run produce:avro
# using schema order-v2.avsc -> registry id=2
```

Accepted immediately — no complaint. Then ran the **exact same, unmodified**
`consumer-avro.ts` against the whole topic (mix of v1- and v2-encoded
messages):

```
key=order-avro-1     decoded={...no discountCode key at all...}
key=order-avro-v2-1  decoded={...,"discountCode":"SUMMER10"}
key=order-avro-v2-2  decoded={...,"discountCode":null}
key=order-avro-3     decoded={...no discountCode key at all...}
```

Both old (v1) and new (v2) messages decoded correctly, in the same run, with
**zero code changes** to the consumer. That's the actual payoff of schema
evolution done right — old and new producers and consumers can all coexist
during a rollout.

### Experiment 3 — an actual rejection (v3, breaking)

Added `shippingAddress` (`type: "string"`, **no default**) — a required field
with no fallback for old data that doesn't have it. Checked compatibility
first, without registering:

```bash
curl -X POST http://localhost:8081/compatibility/subjects/orders-avro-value/versions/latest \
  -d '{"schema": "<order-v3-breaking.avsc contents>"}'
# {"is_compatible": false}
```

Then tried to actually register it anyway:

```bash
curl -X POST http://localhost:8081/subjects/orders-avro-value/versions \
  -d '{"schema": "<order-v3-breaking.avsc contents>"}'
```

```json
{
  "error_code": 409,
  "message": "Schema being registered is incompatible with an earlier schema
   for subject \"orders-avro-value\", details: [{errorType:
   'READER_FIELD_MISSING_DEFAULT_VALUE', description:'The field
   'shippingAddress' ... has no default value and is missing in the old
   schema' ...}, {compatibility: 'BACKWARD'}]"
}
```

**HTTP 409, registration refused**, naming the exact field and exact reason.
Confirmed via `curl .../orders-avro-value/versions` afterward: still only
`[1, 2]` — v3 never made it in.

One thing worth being precise about, since it's easy to get backwards:
**removing** a field (like `status`) would *not* have been rejected under the
default `BACKWARD` compatibility mode — old data just has an extra field a new
reader ignores. It's specifically *adding a required field with no default*
that breaks things, because a new reader has no value to use when reading data
that predates the field's existence.

### Connecting this back to Stage 6's poison pill

The `heyyy` string that crashed `payment-service` in Stage 6 could never have
been produced to `orders-avro` in the first place — `registry.encode()` would
have failed immediately, client-side, because it doesn't match any registered
schema. Schema Registry moves the defense from "every consumer must
defensively parse and hope" to "the producer physically cannot write garbage
in the first place." Our plain-JSON topics (`orders`, `payments`, etc.) still
have zero such protection — this is a real, structural difference between the
two approaches, not just a style preference.

### Key concepts introduced

- **Schema Registry as a separate service**: schemas aren't stored in Kafka
  messages themselves — they live in the registry, referenced by a small id
  embedded in each message, keeping messages compact.
- **Subjects and versions**: a subject (`orders-avro-value`) is a named,
  ordered history of schema versions for one topic+key/value slot. Compatibility
  is always checked against the subject's latest version by default.
- **Compatibility modes** (we exercised `BACKWARD`, the default): governs
  which changes are legal — adding fields needs defaults, removing fields is
  generally safe, changing a field's type usually isn't.
- **The registry is a producer-side gate, not a consumer-side filter**: an
  incompatible schema is rejected *before* anything is written to the topic —
  fundamentally different from our JSON topics, where anything goes in and
  consumers find out the hard way.

### One more real bug, caught by actually checking the UI

When Kafka UI was asked to check its own schema registry integration
(`curl localhost:8080/api/clusters/kafkaos-local/schemas`), it returned:
`"Schema Registry is not set for cluster kafkaos-local"` — even though
`docker-compose.yml` clearly had `KAFKA_CLUSTERS_0_SCHEMAREGISTRY` set.

Cause: earlier in this stage we only ran `docker compose up -d schema-registry`
to start the *new* service — that never touches `kafka-ui`, so it kept running
as the container created back in Stage 0, three days earlier, with its
original environment. Docker Compose only recreates a container when you
explicitly tell it to (or run a bare `docker compose up -d` with no service
name, which reconciles everything). Confirmed via
`docker inspect kafka-ui --format '{{range .Config.Env}}{{println .}}{{end}}'`
— no `SCHEMAREGISTRY` var present at all.

**Fix**: `docker compose up -d kafka-ui` to recreate just that container with
the current compose file. Re-checked the same API endpoint afterward — now
correctly returns the registered schema.

**Lesson**: editing `docker-compose.yml` does not retroactively apply to
already-running containers. Bringing up a *new* service by name doesn't
recreate *other* services whose config changed in the same file — each
container needs to be told to pick up the new config, and `docker compose up -d`
(no arguments) is the reliable way to reconcile everything at once.

### Also fixed: one bad order was killing the whole producer batch

`producer-avro.ts` originally had **no error handling around `registry.encode()`**
— exactly the poison-pill risk from Stage 6, but on the producer side instead of
the consumer side. Proved it directly: temporarily inserted an order with
`total: "not-a-number"` (schema expects a `double`) between `order-avro-1` and
`order-avro-2`:

```
sent order-avro-1 -> ...
ConfluentSchemaRegistryValidationError: invalid "double": "not-a-number"
    paths: [ [ 'total' ] ]
```

`encode()` throws synchronously on a schema mismatch. With no `try/catch` in the
loop, that exception propagated straight to the top-level `run().catch()`,
which logs it and calls `process.exit(1)` — **killing the whole process
mid-batch**. Confirmed on the broker: only `order-avro-1` (sent *before* the bad
one) made it in; `order-avro-2` and `order-avro-3` were never even attempted.

**Fix**: wrapped just the `registry.encode()` call in `try/catch` inside the
loop — log which order got skipped and why, `continue` to the next one, leave
`producer.send()` outside the catch (nothing to send if encoding failed
anyway). Re-ran the exact same bad-order test:

```
sent order-avro-1 -> ...
skipping order-BAD: doesn't fit schema order-v1.avsc (invalid "double": "not-a-number")
sent order-avro-2 -> ...
sent order-avro-3 -> ...
```

Confirmed on the broker: message count went from 7 → 10 (all three good orders
landed; `order-BAD` correctly excluded, not silently corrupted or partially
written).

**The asymmetry worth remembering**: Schema Registry guarantees bad data can
never *reach* a topic, but by default it does that by throwing hard and
stopping everything at the point of failure — it does not skip-and-continue on
its own. That behavior has to be added explicitly, the same way `safeParseJson`
had to be added explicitly on the consumer side in Stage 6. Enforcement and
graceful degradation are two separate concerns; the registry only gives you
the first one.

### Next up

Stage 8 — Kafka Connect: sink connector (e.g. `orders` → a database or file)
and a source connector, to see how Kafka integrates with systems outside
itself without hand-writing a producer/consumer for every integration.

---

## Stage 8 — Kafka Connect

**Date**: 2026-08-02

**Goal**: See how Kafka integrates with outside systems *declaratively* — a
JSON config posted to a REST API — instead of hand-writing a producer or
consumer for every integration. Built four connectors: two built-in file
connectors (zero extra infra, to validate Connect itself) and a real JDBC
sink/source pair against Postgres (what Connect is actually used for in
practice). Hit three genuine, unplanned problems along the way — kept all of
them, since each one taught something the happy path wouldn't have.

### What we added

- **`kafka-connect` service** — `confluentinc/cp-kafka-connect-base:7.6.1`,
  distributed mode (single worker), REST API on `localhost:8083`. Its
  `command` runs `confluent-hub install confluentinc/kafka-connect-jdbc` on
  startup before handing off to the normal entrypoint, since the "base" image
  ships the Connect framework only, no connectors pre-installed.
- **`postgres` service** — `postgres:16-alpine`, for the JDBC sink/source pair.
- **`./connect-data/`** — bind-mounted into the Connect container at `/data`,
  used by the file connectors so their output is inspectable straight from the
  host filesystem.
- **`src/stage08-kafka-connect/`** — `schemas/payment-flat-v1.avsc` (a
  deliberately flat Avro schema — see below) and `produce-flat-payments.ts` to
  seed it.

Distributed-mode Connect stores its own config/offsets/status in three more
Kafka topics (`connect-configs`, `connect-offsets`, `connect-status`) — same
pattern as `__consumer_offsets` and `_schemas` before it: Kafka using itself
to store its own subsystems' state.

### Problem 1 — the file connectors weren't actually there

`confluentinc/cp-kafka-connect-base` doesn't bundle Apache Kafka's own
built-in `FileStreamSinkConnector`/`FileStreamSourceConnector` — confirmed via
`GET /connector-plugins`, which listed only the JDBC connectors we'd just
installed. Root cause: `connect-file-*.jar` simply isn't shipped in this
image's plugin path.

**Fix**: our plain `apache/kafka` broker container *does* ship it
(`/opt/kafka/libs/connect-file-3.8.0.jar`, since it's core Kafka, not a
Confluent add-on). Copied it across containers and dropped it into a fresh
plugin directory:

```bash
docker cp kafka:/opt/kafka/libs/connect-file-3.8.0.jar /tmp/
docker exec kafka-connect mkdir -p /usr/share/java/kafka-connect-file
docker cp /tmp/connect-file-3.8.0.jar kafka-connect:/usr/share/java/kafka-connect-file/
docker compose restart kafka-connect   # plugins are only scanned at startup
```

After the restart, both `FileStreamSinkConnector` and `FileStreamSourceConnector`
showed up in `/connector-plugins`. **Lesson**: a Connect "plugin" is just a
jar (or directory of jars) sitting in a directory Connect scans at startup —
nothing more mysterious than that, and it can be added by hand.

### File connectors — sink and source

**Sink**: `orders` topic → `/data/orders-sink.txt`. First attempt used
`JsonConverter` for both key and value and immediately started failing —
which led to two more real problems:

**Problem 2 — wrong key converter.** The failure was on the *key* converter,
not the value: our message keys (`"order-1"`, etc.) are plain strings, not
JSON — `JsonConverter` expects a quoted JSON value and rejects a bare
unquoted string. Nearly every message failed key conversion. Fixed by setting
`"key.converter": "org.apache.kafka.connect.storage.StringConverter"` for
this connector specifically (Connect lets you override converters per
connector, not just at the worker level) while keeping `JsonConverter` for
the value, since our JSON values really are valid JSON.

**Problem 3 — the `heyyy` poison pill, again.** Even after fixing the key
converter, the `orders` topic still contains that non-JSON message from
Stage 6/7 (sent via Kafka UI's "Produce Message" screen). Value conversion
fails on it exactly like it failed our hand-written consumers before we added
`safeParseJson`. This time, instead of custom code, used Connect's **built-in**
answer to the same problem:

```json
"errors.tolerance": "all",
"errors.deadletterqueue.topic.name": "orders-file-sink-dlq",
"errors.deadletterqueue.topic.replication.factor": "1",
"errors.log.enable": "true"
```

Reset the connector's consumer group to `earliest` and reran clean. Result:
**16 of 17 messages** written to `/data/orders-sink.txt`, and exactly **1**
message (`heyyy`) routed to `orders-file-sink-dlq` instead of crashing the
task. Verified both independently — `docker exec kafka-connect cat
/data/orders-sink.txt` for the file, `kafka-console-consumer` on the DLQ topic
for the poison pill.

This is the same "poison pill" problem for the third time in this project
(Stage 6: consumer code, Stage 7: producer code, Stage 8: framework config)
— and three genuinely different mechanisms for handling it, each appropriate
to where the defense actually lives.

**Source**: `/data/source-input.txt` → `manual-events` topic. Started a live
console consumer on `manual-events`, then appended three lines to the file
one at a time from the host shell:

```bash
echo "hello from the filesystem" >> connect-data/source-input.txt
echo "this line became a real Kafka message" >> connect-data/source-input.txt
echo "no producer code was written for this" >> connect-data/source-input.txt
```

All three showed up on the topic, in order, within seconds — genuinely no
producer code involved, just a file connector watching a file grow.

### JDBC sink — and why it needs Stage 7's work

First instinct was to sink the existing `orders` topic straight to Postgres.
That doesn't work: the JDBC sink's `auto.create` feature needs real **schema**
information (column names + types) to create a table, and our plain-JSON
`orders` topic (schemaless `JsonConverter`) carries none — this is exactly
what Schema Registry is for. Also, `orders`' `items` field is a nested array,
which a flat SQL table can't represent without an explicit flattening
transform (not built here — noted as a follow-up).

So: created a small **flat** Avro schema instead
(`payment-flat-v1.avsc` — `orderId`, `amount`, `status`, `processedAt`, no
nesting), registered and produced 3 records to a new `payments-flat` topic
via `npm run produce:flat-payments`, then sank *that*:

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
  "topics": "payments-flat",
  "connection.url": "jdbc:postgresql://postgres:5432/kafkaos",
  "auto.create": "true",
  "auto.evolve": "true",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

Confirmed directly in Postgres — table auto-created, named after the topic,
columns matching the Avro field names exactly:

```
docker exec postgres psql -U kafkaos -d kafkaos -c '\dt'
#  payments-flat | table

docker exec postgres psql -U kafkaos -d kafkaos -c 'SELECT * FROM "payments-flat";'
#  order-flat-1 | 39.98 | succeeded | 2026-08-02T...
#  order-flat-2 |    15 | succeeded | 2026-08-02T...
#  order-flat-3 | 89.97 | failed    | 2026-08-02T...
```

### JDBC source — and watching it live

Created a plain Postgres table directly with `psql` (no Kafka involved yet):

```sql
CREATE TABLE manual_source_items (
  id SERIAL PRIMARY KEY,
  item_name TEXT,
  created_at TIMESTAMP DEFAULT now()
);
INSERT INTO manual_source_items (item_name) VALUES ('widget'), ('gadget');
```

Then a JDBC source connector in `incrementing` mode (tracks progress via the
`id` column, exactly analogous to a consumer tracking offsets):

```json
{
  "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
  "mode": "incrementing",
  "incrementing.column.name": "id",
  "table.whitelist": "manual_source_items",
  "topic.prefix": "pg-",
  "poll.interval.ms": "2000",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

Both existing rows appeared on `pg-manual_source_items` immediately. Then, to
watch it live: started a consumer, and while it was running, ran a plain SQL
`INSERT` directly against Postgres from a separate terminal:

```sql
INSERT INTO manual_source_items (item_name) VALUES ('sprocket-inserted-live');
```

It showed up on the Kafka topic within the 2-second poll interval — decoded
cleanly with `kafka-avro-console-consumer` (ships inside the `schema-registry`
container):

```json
{"id":1,"item_name":{"string":"widget"},"created_at":{"long":1785669295013}}
{"id":2,"item_name":{"string":"gadget"},"created_at":{"long":1785669295013}}
{"id":3,"item_name":{"string":"sprocket-inserted-live"},"created_at":{"long":1785669327420}}
```

No producer code, no manual polling loop — a database write became a Kafka
message, automatically, because a connector was watching.

### Key concepts introduced

- **Connect is declarative**: connectors are JSON configs posted to a REST
  API (`POST /connectors`), not code you write and deploy. `GET
  /connectors/<name>/status` tells you `RUNNING`/`FAILED` per connector *and*
  per task, which is exactly how we diagnosed every failure above.
- **Plugins are just jars in a scanned directory** — nothing about the
  Connect framework or Confluent's images makes this more magical than that,
  as proven by manually copying one across containers and restarting.
- **Converters are configurable per connector**, not just at the worker
  level, and key/value can (and often must) use different converters —
  `StringConverter` for a plain-string key, `JsonConverter` or `AvroConverter`
  for a structured value.
- **`errors.tolerance` + a dead letter queue** is Connect's framework-level
  answer to the poison-pill problem — declarative, no custom code, directly
  comparable to the `try/catch` we wrote by hand in Stages 6 and 7.
- **Sink connectors that need schema (like JDBC's `auto.create`) are exactly
  why Schema Registry (Stage 7) matters** — it's not just an Avro nice-to-have,
  it's the mechanism that lets a downstream system that's never seen your
  TypeScript code figure out what columns to create.
- **JDBC source's `incrementing` mode is conceptually identical to consumer
  offsets** — a saved position (last-seen `id`) that determines where the
  next poll resumes from, same idea as `__consumer_offsets`, different
  storage (Connect's own offset topic instead).

### Addendum — connector configs weren't saved as files (fixed)

Every connector above was created by `curl -X POST ... -d '{...}'` — meaning,
until this addendum, **there was no file anywhere in the repo showing the
actual Kafka↔Postgres connection**, unlike every previous stage where the
`.ts` file *is* the real, permanent artifact. The live source of truth was
sitting inside Kafka itself, in the `connect-configs` internal topic — real,
but invisible to anything except the REST API.

**Fixed**: pulled the actual live config for all four connectors straight
from `GET /connectors/<name>/config` and saved them as
`src/stage08-kafka-connect/connectors/*.json` — e.g.
`payments-flat-postgres-sink.json` has `"connection.url":
"jdbc:postgresql://postgres:5432/kafkaos"` right in it, which is the literal
answer to "where's the connection defined." Also added
`apply-connectors.sh`, which re-POSTs all four files to Connect's REST API —
verified it actually works against the live worker (reapplied all four,
confirmed all still `RUNNING`/`RUNNING` afterward). This makes the connector
configs re-creatable the same way `npm run produce` re-creates Stage 2's
state, instead of being a one-off `curl` command that only ever existed in
this terminal session.

### Next up

Stage 9 — stream processing with ksqlDB: windowed aggregations and joins over
the event streams we've built (`orders`, `payments`, `inventory`, `shipping`),
without hand-writing stateful consumer logic in TypeScript.

---

## Stage 9 — Stream processing with ksqlDB

**Date**: 2026-08-02

**Goal**: Do real stateful stream processing — a join across two topics, a
windowed aggregation, instant point lookups — using declarative SQL instead of
hand-writing consumer logic with local state in TypeScript. Along the way,
found a genuinely important, non-obvious lesson about what a "join" actually
means in stream processing.

### What we added

- **`ksqldb-server`** (`confluentinc/ksqldb-server:0.29.0`) — the actual
  processing engine, REST API on `localhost:8088`. Configured with
  `KSQL_KSQL_STREAMS_AUTO_OFFSET_RESET: earliest` so queries see the full
  history, not just new events from when the query starts (same idea as
  `fromBeginning` back in Stage 3).
- **`ksqldb-cli`** — a persistent container with no fixed command (`tty: true`,
  `stdin_open: true`), just sitting there so we (or the CLI, interactively)
  can `docker exec -i ksqldb-cli ksql http://ksqldb-server:8088` any time.
- **Wired into Kafka UI** (`KAFKA_CLUSTERS_0_KSQLDBSERVER`) — same pattern as
  Schema Registry (Stage 7) and Kafka Connect (Stage 8): every new backing
  service gets connected to the one UI, not left as a separate silo.
- **`src/stage09-ksqldb/statements.sql`** — every `CREATE STREAM`/`CREATE
  TABLE` below, saved as a real, re-runnable file (same lesson as Stage 8's
  connector JSON files: a `curl`/CLI session by itself leaves no permanent
  trace in the repo).

### Streams over our existing topics

Four streams, one per topic from Stages 2 and 6 (`orders`, `payments`,
`inventory`, `shipping`), plain JSON, no schema registry dependency for this
stage. Notably, `items` maps to `ARRAY<STRUCT<sku VARCHAR, qty INT>>` —
**ksqlDB handles our real nested `Order` shape directly**, unlike the JDBC
sink connector in Stage 8, which needed a deliberately flattened schema
because relational tables can't represent nested arrays natively.

### The poison pill, once more — and a pleasant surprise

Querying `orders_stream` hit the `heyyy` message again (of course):

```
org.apache.kafka.common.errors.SerializationException: Failed to deserialize value from topic: orders...
```

But unlike Kafka Connect (Stage 8), which **crashes by default** and needs
`errors.tolerance=all` explicitly configured, ksqlDB's default behavior is
already **log-and-skip** — confirmed by querying for `order-live-demo` (a
message produced well after the poison pill's offset) and getting it back
without issue. Fourth appearance of the same problem, fourth different
handling story: app code (Stage 6), producer code (Stage 7), framework config
(Stage 8), and now — engine default (Stage 9).

### The join — and the real lesson of this stage

Built a stream-stream join correlating `orders` with `payments` by `orderId`:

```sql
CREATE STREAM order_payment_joined AS
SELECT o.orderId, o.customerId, o.total AS orderTotal,
       p.status AS paymentStatus, p.processedAt AS paymentProcessedAt
FROM orders_stream o
INNER JOIN payments_stream p
WITHIN (1 HOURS, 1 HOURS) GRACE PERIOD 1 MINUTES
ON o.orderId = p.orderId
EMIT CHANGES;
```

Querying it only returned **2 rows** — `order-manual-test` and
`order-live-demo`, both produced very recently. None of the original
`order-1`..`order-6` showed up, even though every one of them *does* have a
matching payment sitting in the `payments` topic. This looked like a bug at
first. It wasn't.

**Root cause, confirmed by checking real timestamps**:

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh ... --property print.timestamp=true
# order-1 in 'orders':   CreateTime:1785411444339   (Stage 2, several days ago)
# order-1 in 'payments': CreateTime:1785659551457   (Stage 6, when payment-service *first ran*)
```

```python
(1785659551457 - 1785411444339) / 1000 / 3600  # = 68.9 hours
```

**`order-1` was created three days before `payment-service` ever processed
it** — because `payment-service` didn't exist yet in Stage 2, and only
started running for the first time in Stage 6. A `WITHIN 1 HOUR` join
correctly does **not** consider these two events "close enough" — a
stream-stream join's window is about **event-time proximity**, not "did both
of these ever happen, in any order, at any distance." This is not a bug;
it's the join working exactly as designed on genuinely far-apart data — an
artifact of how *we* ran this tutorial (stopping and restarting services
across days), not something a real continuously-running system would
normally produce.

**Proved the mechanism directly**: recreated the same join with `WITHIN (5
DAYS, 5 DAYS)` — the historical `order-1`, `order-2`, `order-4`, `order-5`
matches immediately appeared. Then tore that experiment down and restored the
1-hour version, which is the realistic choice for a live system (a payment
you'd actually want to alert on if it takes *days* to arrive, not something
to patiently wait for in the same window). Confirmed the 1-hour version still
works going forward: sent a fresh order, and within seconds the join produced
`order-ksql-live` with its matched payment.

**The practical trade-off, worth remembering**: a join's window isn't free —
Kafka Streams (which ksqlDB runs on under the hood) has to hold state for the
*entire* window duration, waiting for a possible late match on either side.
`WITHIN 5 DAYS` means 5 days of retained join-buffer state, continuously, for
as long as that query runs. Wider windows aren't just "more forgiving," they
cost real memory/storage the whole time they're active.

### Windowed aggregation

```sql
CREATE TABLE payments_per_minute AS
SELECT status, COUNT(*) AS payment_count, SUM(amount) AS total_amount
FROM payments_stream
WINDOW TUMBLING (SIZE 1 MINUTES)
GROUP BY status
EMIT CHANGES;
```

Querying it showed multiple separate 1-minute buckets, matching exactly when
payments actually landed — including one bucket with **15 succeeded
payments** totaling ~$909.87, which is the moment `payment-service` first
started and blew through its entire 3-day backlog in under a minute (Kafka's
durability from Stage 1, showing up again here as a very literal timestamp
cluster). Sent one more live order and immediately saw a fresh window appear
reflecting it — a continuously updating, real materialized view, not a
one-time report.

### Pull queries vs. push queries

Every query so far used `EMIT CHANGES` — a **push query**: it streams results
forever (or until `LIMIT` cuts it off) as new matching events arrive. Built
one more table specifically to demonstrate the alternative:

```sql
CREATE TABLE latest_payment_status AS
SELECT orderId, LATEST_BY_OFFSET(status) AS status, LATEST_BY_OFFSET(amount) AS amount
FROM payments_stream
GROUP BY orderId
EMIT CHANGES;
```

Then queried it **without** `EMIT CHANGES`:

```sql
SELECT orderId, status, amount FROM latest_payment_status WHERE orderId = 'order-ksql-live';
```

This is a **pull query** — it returned one row and **terminated immediately
on its own**, unlike every push query above which needed `LIMIT` to stop.
Conceptually this is a normal, synchronous database read (like a REST API
backed by a cache) against continuously-maintained materialized state — the
two query types serve genuinely different purposes: push queries for
"notify me as things happen," pull queries for "tell me the current answer,
right now."

### Key concepts introduced

- **ksqlDB is SQL running continuously over Kafka Streams** — every `CREATE
  STREAM ... AS SELECT` or `CREATE TABLE ... AS SELECT` is a real, persistent
  background job (visible via `SHOW QUERIES`), reading from one topic and
  writing to another, forever, not a one-shot report.
- **Stream-stream joins require a time window (`WITHIN`)**, and that window
  is about event-time proximity between the two sides — not "have these ever
  both occurred." This is the single most important, least obvious thing
  from this stage.
- **Windowed aggregations (`WINDOW TUMBLING`) bucket by event time**, and
  produce a genuinely materialized, incrementally-updated table — not a
  batch job re-run periodically.
- **Push vs. pull queries**: `EMIT CHANGES` = ongoing stream of results;
  without it = one-shot, synchronous lookup against current state. Same
  underlying materialized tables, two different consumption models.
- **Nested/structured data works natively in ksqlDB** (`ARRAY<STRUCT<...>>`),
  in direct contrast to the JDBC sink connector's flat-columns-only
  limitation from Stage 8 — different tools in the same pipeline have
  genuinely different data-shape capabilities.

### Next up

Stage 10 — transactions / exactly-once semantics: Kafka's transactional
producer/consumer API, tying a consume-offset-commit and a produce together
atomically — the piece Stage 5's delivery-semantics discussion explicitly
deferred.

---

## Stage 10 — Transactions / exactly-once semantics

**Date**: 2026-08-02

**Goal**: Close the gap Stage 5 deliberately left open. At-least-once (Stage 5)
guarantees no input is lost, but a crash between "do the work" and "commit the
offset" can duplicate the *output* — exactly the risk in `payment-service`,
`inventory-service`, `shipping-service` (Stage 6), which all consume from one
topic and produce to another. Kafka's transactional API ties the produce and
the offset commit into one atomic unit. Proved it actually holds up under a
real crash, not just in theory.

### What we built

**`src/stage10-transactions/transactional-processor.ts`** — consumes
`orders-crash-demo` (the same deterministic, single-partition, 8-message topic
from Stage 5 — same messages, so results are directly comparable to that
stage's at-least-once experiment), and for each message:

```ts
const transaction = await producer.transaction();
await transaction.send({ topic: outputTopic, messages: [...] });
await transaction.sendOffsets({ consumerGroupId: groupId, topics: [...] });
await transaction.commit();
```

Both the produced message *and* the consumer offset commit are part of the
same transaction — `commit()` makes both visible at once; anything before
that point can be abandoned without a trace (from a `read_committed`
consumer's point of view).

One config detail worth calling out: the producer needs a **stable
`transactionalId`** across restarts (`kafka.producer({ transactionalId,
maxInFlightRequests: 1, idempotent: true })`). This is what lets the broker
recognize "this is the same logical producer coming back" after a crash, so
it can fence off and resolve the dangling transaction left behind — a random
new ID every run would defeat the whole mechanism.

### Experiment 1 — clean run, and a real surprise about offsets

Ran it against all 8 messages in `orders-crash-demo`, no crash:

```
[processed] key=order-1 offset=0 -> committed atomically
...
[processed] key=order-2 offset=7 -> committed atomically
finished run, 8 message(s) processed
```

Checked the output topic's raw offsets: `kafka-get-offsets.sh` reported
**16**, not 8. Not a bug — every committed transaction writes a **control
record** (a commit marker) to the partition, in addition to the actual data
record, and that marker consumes its own offset slot. Confirmed by reading
with `print.offset=true`: real data sits at offsets `0,2,4,6,8,10,12,14`;
the odd offsets in between are invisible commit markers that ordinary
consumers never see displayed, but which are physically part of the log.

### Experiment 2 — crash mid-transaction, and the actual proof

Same topic, fresh group/transactional ID, configured to crash **after**
producing for `order-3` but **before** calling `commit()`:

```
[processed] key=order-1 offset=0 -> committed atomically
[processed] key=order-2 offset=1 -> committed atomically
[CRASH] produced for key=order-3 inside an OPEN transaction, exiting BEFORE commit
```

Checked the output topic immediately, from both isolation levels:

```bash
kafka-console-consumer.sh ... --isolation-level read_uncommitted   # 11 messages — includes the dangling order-3
kafka-console-consumer.sh ... --isolation-level read_committed     # 10 messages — order-3 correctly excluded
```

The dangling transaction is genuinely **in-doubt** at this point — written to
disk, but not yet resolved. `read_uncommitted` sees it; `read_committed`
doesn't.

**Restarted with the same `transactionalId` and group**, no crash this time:

```
[processed] key=order-3 offset=2 -> committed atomically   <- reprocessed, same offset as before the crash
[processed] key=order-4 offset=3 -> committed atomically
...
finished run, 6 message(s) processed
```

Exactly like Stage 5's at-least-once behavior on the **input** side —
`order-3` gets reprocessed because its offset was never committed. The
question is what happens on the **output** side.

**The actual proof** — checked the final state under `read_committed`:

```bash
kafka-console-consumer.sh ... --isolation-level read_committed --print.offset=true
```

`order-3` appears **exactly once** (`sourceOffset: 2`, once), not twice —
total 16 messages across both consumer groups' runs, no duplicates anywhere.
Then confirmed the exact mechanism by checking `read_uncommitted` at the same
spot:

```
Offset:18  order-2  sourceOffset=1
Offset:20  order-3  sourceOffset=2   <- the ABORTED attempt (only visible here)
Offset:22  order-3  sourceOffset=2   <- the COMMITTED retry
Offset:24  order-4  sourceOffset=3
```

**Both attempts physically exist in the log** — Kafka doesn't delete an
aborted transaction's data, it just marks it aborted via a control record.
`read_uncommitted` sees both (17 messages total at this point); `read_committed`
correctly filters the aborted one out (16 messages, no duplicate).

### What this actually buys you, precisely

- **Input side**: still at-least-once. `order-3` genuinely got reprocessed —
  transactions don't prevent reprocessing of the input, and can't (the
  process really did die before committing anything).
- **Output side**: genuinely exactly-once, *for readers using
  `read_committed`*. The duplicate that would have appeared in Stage 5's
  plain at-least-once demo (two visible copies of `order-3`'s side effect)
  simply never becomes visible here — the failed attempt's write exists on
  disk but is filtered out by isolation level, not by any deduplication
  logic.
- **The guarantee is conditional on the consumer's isolation level.** A
  `read_uncommitted` consumer of the *same* topic would see the duplicate
  exactly like Stage 5's non-transactional version. Exactly-once here is a
  property of the write + how it's read, not an unconditional fact about the
  topic.

### Key concepts introduced

- **Transactions tie a produce and a consumer offset commit into one atomic
  unit** — this is specifically for the consume-transform-produce pattern
  (read from topic A, write to topic B), which is exactly what
  `payment-service`/`inventory-service`/`shipping-service` do.
- **`transactionalId` must be stable across restarts** of the same logical
  producer — it's the mechanism the broker uses to recognize and resolve a
  dangling transaction after a crash, not just an arbitrary label.
- **Committed transactions write control/marker records** that consume real
  offset space in the partition, invisible to normal consumption but visible
  in raw offset counts (`log-end-offset` includes them).
- **`isolation.level` (`read_committed` vs `read_uncommitted`) determines
  whether a consumer sees data from transactions that were never committed.**
  Aborted/dangling transactional writes are never deleted — they're filtered
  at read time based on this setting.
- **Exactly-once here means "the output never shows a duplicate to a
  `read_committed` reader,"** not "the work never happens twice." The
  process-level reprocessing from Stage 5 still happens; transactions make
  its *visible effect* clean instead of duplicated.

### Next up

Stage 11 — monitoring & operations: consumer lag, a real multi-broker
cluster, leader election, and in-sync replicas — replication factor finally
gets to mean something, since every topic so far has had RF=1 on our single
broker.

---

## Stage 11 — Monitoring & operations

**Date**: 2026-08-02

**Goal**: Make replication factor mean something for the first time — every
topic in this project so far has lived on our single broker with RF=1
(forced, not chosen, since Stage 0). Stand up a real multi-broker cluster,
watch leader election and ISR shrink/recover under an actual killed broker,
and connect consumer lag (used throughout since Stage 3) to what "monitoring"
actually means operationally.

### Why a *separate* cluster

Our main `kafka` service holds every topic from Stages 1–10 — days of
tutorial history. Converting it in place to multi-broker would mean
reconfiguring an already-formed KRaft quorum, a real risk for no benefit.
Instead: three new services (`kafka-b1`, `kafka-b2`, `kafka-b3`), forming
their own independent KRaft cluster with its own `CLUSTER_ID`
(`kafkaos-multibroker-cluster-1`) and its own controller quorum
(`101@kafka-b1:9093,102@kafka-b2:9093,103@kafka-b3:9093`), completely
separate from the main cluster. Also gave this cluster real production
defaults (`KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3`, etc.) instead of the
RF=1 forced everywhere else — we finally have enough brokers to support it.
Wired as a second cluster into Kafka UI (`KAFKA_CLUSTERS_1_*`) so both
clusters are browsable from the same place.

### Verifying the cluster actually formed correctly

```bash
docker exec kafka-b1 /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server kafka-b1:19092 describe --status
# CurrentVoters: [101,102,103]   <- all 3 recognized as controller quorum voters
```

**Hit a real gotcha immediately**: first attempt used `--bootstrap-server
localhost:9092`, which returned metadata advertising the *host-mapped* ports
(`localhost:9192`, `9193`, `9194` — the `PLAINTEXT_HOST` listener meant for
connections from outside Docker). Run from *inside* a container, those
addresses are meaningless (there's no port 9192 inside `kafka-b1`'s own
network namespace) — the command hung retrying forever. Fixed by using the
internal listener address instead: `--bootstrap-server kafka-b1:19092`. This
is the Stage 0 listener-separation lesson (`PLAINTEXT` for Docker-internal,
`PLAINTEXT_HOST` for the host) showing up again, this time biting an admin
tool instead of an application.

### RF=3, for the first time in this project

```bash
docker exec kafka-b1 /opt/kafka/bin/kafka-topics.sh --create \
  --topic orders-replicated --partitions 3 --replication-factor 3 \
  --bootstrap-server kafka-b1:19092
```

```
Partition: 0   Leader: 102   Replicas: 102,103,101   Isr: 102,103,101
Partition: 1   Leader: 103   Replicas: 103,101,102   Isr: 103,101,102
Partition: 2   Leader: 101   Replicas: 101,102,103   Isr: 101,102,103
```

Direct contrast with Stage 1's `orders` topic (`Leader: 1 Replicas: 1 Isr:
1` — a single broker, no redundancy at all, since there was only ever one
option). Here, every partition has all 3 brokers in `Isr`, and **leadership
is spread across different brokers** (102, 103, 101 for partitions 0, 1, 2
respectively) — Kafka distributes leadership for load balancing rather than
concentrating it on one node.

### The actual experiment: kill a broker, live

Produced 6 messages, then killed `kafka-b3` (node 103) — which happened to be
**both** the leader of partition 1 **and** the current controller quorum
leader:

```bash
docker stop kafka-b3
```

Checked immediately:

```
Partition: 0   Leader: 102   Isr: 102,101        <- 103 dropped from ISR, leader unaffected
Partition: 1   Leader: 101   Isr: 101,102        <- LEADER CHANGED: was 103, now 101
Partition: 2   Leader: 101   Isr: 101,102        <- 103 dropped from ISR, leader unaffected
```

Partition 1's leader failed over from the now-dead `103` to `101`
**automatically, with zero manual intervention** — this is the whole promise
of replication, actually observed rather than taken on faith. Controller
quorum also re-elected:

```bash
docker exec kafka-b1 kafka-metadata-quorum.sh ... describe --status
# LeaderId: 101 (was 103), LeaderEpoch: 2 (was 1)
```

The epoch incrementing from 1 to 2 confirms this is a genuine re-election, not
a stale cached read.

**Proved the cluster kept working** with a broker down: produced 3 more
messages, then consumed all 9 from the beginning — everything present and
readable, no errors, no missing data, despite running on 2 of 3 brokers.

**Brought `kafka-b3` back**: `docker start kafka-b3`. It rejoined the ISR for
every partition almost instantly (`Isr: 102,101,103` etc., confirmed via the
same `--describe`). At that moment, **leadership had not moved back to `103`**
— partition 1's leader stayed on `101`. See the addendum below: this turned
out to be only half the picture — leadership *did* move back, just five
minutes later, via a completely different mechanism than the one that moved
it away in the first place.

### Addendum — what actually happened, per the broker's own logs

`docker logs kafka-b1` was nearly empty (9 lines) — this image writes its
real logs to files inside the container, not stdout. Found them at
`/opt/kafka/logs/`, in particular `controller.log` (every controller
decision) and `state-change.log` (every partition leader/ISR transition).
Reading those revealed two things worth correcting/adding to the record
above.

**1. `docker stop` is not a crash — and that changed everything about the
timing.** `docker stop` sends `SIGTERM` first (only escalating to `SIGKILL`
after a grace period). Kafka catches `SIGTERM` and performs a **controlled
shutdown**, proactively telling the controller it's leaving *before* it
actually exits:

```
13:43:12.993  controller.log:    broker 103 marked inControlledShutdown=1
13:43:12.995  state-change.log:  broker 101 becomes leader of orders-replicated-1,
                                  epoch 1, "Previous leader Some(103)"
```

**Two milliseconds apart.** Compare this to Stage 4, where an ungraceful
`SIGKILL`'d consumer took **4.5 minutes** to even be noticed — the mechanism
here is completely different (and much faster) precisely *because* the dying
broker cooperated by announcing its own departure, rather than the controller
having to infer death from a missed heartbeat. Redoing this same experiment
with `docker kill` (true `SIGKILL`, no grace period, no chance for Kafka to
run its shutdown hook) would very likely reproduce something closer to Stage
4's slow, timeout-based detection instead — worth trying in Stage 12.

**2. Leadership *did* return to `103` — automatically, ~5 minutes later, via
a completely separate mechanism.** Re-checked the topic well after writing
the observation above:

```
Partition: 1   Leader: 103   Isr: 101,102,103
```

`state-change.log` shows exactly when and why:

```
13:44:01.389  broker 103 rejoins, tries to become leader,
              controller: "already have a leader (101) at epoch 1, skip"
13:48:15.215  broker 101 steps down to follower: "Current leader is 103"
```

`13:43:12` → `13:48:15` is ~303 seconds — matching Kafka's default
`leader.imbalance.check.interval.seconds=300` almost exactly. This is
**automatic preferred-leader rebalancing**: a periodic background job,
completely separate from the failover that moved leadership away in the
first place, that quietly restores leadership to each partition's "preferred"
replica (the first one listed in `Replicas`) once it's healthy again — not
instantly, on its own schedule.

**The corrected mental model**: there are two distinct mechanisms, not one —
(a) *reactive* failover the instant a leader is confirmed gone (near-instant
for a graceful shutdown, slow/timeout-based for an ungraceful crash, per
Stage 4), and (b) *proactive* periodic rebalancing that undoes any resulting
imbalance later, on a fixed timer, regardless of how the original failover
happened.

**Follow-up: redid it with `docker kill` (true `SIGKILL`, no grace period)** to
test the "ungraceful crash" side directly, instead of theorizing about it.

```
14:21:23.979  docker kill kafka-b3 (SIGKILL sent)
14:21:31.446  controller.log: "Fencing broker 103 because its session has timed out"
```

**~7.5 seconds** — matching Kafka's default `broker.session.timeout.ms` (9s)
closely, and confirmed via polling that the new leader was already in place
by the first check. This is genuinely different from Stage 4's finding, and
worth being precise about why: Stage 4's consumer-group heartbeat timeout
took **4.5 minutes** despite a similar configured timeout, because (per that
stage's finding) the coordinator only *acted* on the expired heartbeat once
something else touched the group. Here, the broker-level heartbeat mechanism
in the KRaft controller had no such quirk — it fenced the dead broker right
on schedule, independently. Same general concept ("session timeout"), two
different subsystems inside Kafka, two different reliability
characteristics — not something guessable from documentation, only from
actually triggering both and comparing.

### Consumer lag, revisited as an operational concept

We've used `kafka-consumer-groups.sh --describe` (and its `LAG` column) since
Stage 3 purely as a way to see "has this consumer caught up." In an
operational/monitoring context, this is literally the number you'd alert on
in production — a consumer group whose lag is growing (not just nonzero, but
*increasing* over time) means its consumers can't keep up with the topic's
write rate, which is exactly the Stage 6 backpressure scenario we built
deliberately with `SLOW_MS`. Kafka UI's Consumers view is the dashboard
equivalent of this same CLI command — same numbers, graphical instead of
tabular.

### Key concepts introduced

- **Replication factor only means something with enough brokers to back it**
  — every RF=1 topic in this project was a necessity of Stage 0's single
  broker, not a real choice. RF=3 across 3 real brokers is what this
  mechanism is actually for.
- **Leader election is automatic and fast** — no operator action, no
  scripted failover, just Kafka's own controller reassigning partition
  leadership among the remaining ISR members the moment a leader disappears.
- **ISR (in-sync replicas) shrinks and grows dynamically** — it's not a
  static configuration, it's the live set of replicas caught up enough to be
  eligible for leader election, tracked continuously.
- **A recovered broker rejoins as a follower, not immediately as leader again**
  — but it *does* reclaim leadership eventually, via a completely separate,
  periodic preferred-leader-rebalance job (default every 5 minutes), not as
  part of rejoining itself.
- **Graceful shutdown (`SIGTERM`, e.g. `docker stop`) triggers near-instant
  failover** (the dying broker proactively hands off leadership as part of
  shutting down) — a real crash (`SIGKILL`) has no such cooperation and falls
  back to the much slower, timeout-based detection seen in Stage 4. Same
  graceful-vs-ungraceful distinction as Stage 4, now observed at the broker
  level instead of the consumer level.
- **This project's own logs are the actual source of truth, not `docker logs`**
  for this particular image — Kafka writes its real logs (`controller.log`,
  `state-change.log`, etc.) to files under `/opt/kafka/logs/` inside the
  container; `docker logs` only shows a thin startup summary.
- **Listener misconfiguration doesn't just affect apps — it affects your own
  admin tooling too**, if you don't run commands from the right vantage
  point (inside vs. outside Docker's network) with the matching listener
  address.
- **Consumer lag is a monitoring primitive**, not just a debugging tool — the
  exact same command used since Stage 3 to check "did my test finish" is the
  production signal for "is this consumer falling behind."

### Next up

Stage 12 — failure testing: deliberately kill brokers and consumers under
more adversarial conditions (mid-write, mid-transaction, network delays) and
verify — skeptically, not by assumption — whether the guarantees claimed in
Stages 4, 5, and 10 actually hold up.

---

## Stage 12 — Failure testing (final stage)

**Date**: 2026-08-02

**Goal**: The capstone. Deliberately stress-test the claims from Stages 4, 5,
10, and 11 under real adversarial conditions instead of taking them on faith
— including one experiment explicitly deferred all the way back in Stage 5
("the real difference between `acks=1` and `acks=all` only shows up with
RF>1, which is Stage 11 territory"). Along the way, an experiment that failed
in an interesting way, a redesign forced by real architectural constraints,
and the single most surprising finding in this whole project.

### False start: pausing 2 of 3 brokers breaks the *entire* cluster, not just replication

First attempt: `docker pause` both followers of a fresh RF=3 topic
(`failure-test`), produce with `acks=1` (leader alone can ack), then reason
about what a leader crash would do. Producing worked fine — 3 messages
acked instantly, followers frozen and unable to replicate.

Then tried to `--describe` the topic to check ISR. **It hung completely**,
and so did a plain consume from the still-healthy leader:

```
org.apache.kafka.common.errors.TimeoutException
Processed a total of 0 messages
```

Root cause: our 3 brokers are **combined broker+controller nodes** (KRaft),
and the controller quorum needs a **majority** (2 of 3) to make *any*
decision. Pausing 2 of 3 froze 2 of 3 controller voters too — not just their
replication role. With only 1 voter responsive, the Raft quorum couldn't
reach consensus on anything, and that stalled even basic operations against
the one broker that was still fully alive and running.

**This is a bigger, more important finding than the one originally planned**:
in a combined-mode cluster, losing enough nodes to break controller-quorum
majority can make the *entire* cluster unusable — including partitions whose
leader never went down — not just the specific replicas you touched. This is
exactly why production KRaft deployments often run controller-only nodes
separately from broker nodes: it isolates control-plane fragility from
data-plane fragility. Unpausing both brokers recovered the cluster
**instantly** — confirming this was a quorum-availability problem, not
corruption or lost state.

**Consequence for the rest of this stage**: never take down more than 1 of
3 nodes at a time, in order to keep 2-of-3 controller quorum intact
throughout every remaining experiment.

### Redesigning the acks experiment around that constraint

Tried Kafka's real replication-throttling feature
(`leader.replication.throttled.rate`, `follower.replication.throttled.replicas`)
to deterministically simulate "followers are behind" without pausing any
process. Hit a CLI parsing quirk (`kafka-configs.sh --add-config` can't
accept a comma inside a single value like `0:101,0:103` — worked around with
the `*` wildcard instead), but **the throttle itself didn't produce a
detectable lag** for our small test (all 3 brokers showed identical byte
counts). Likely cause: Kafka's quota mechanism allows a small burst before
actually throttling, and our test data was too small to exceed it. Removed
the throttle configs rather than fight this further — a real, honestly-
reported dead end, not force-fit into a fake success.

**Pivoted to something cleaner and fully deterministic**: instead of trying
to catch a live race condition, prove the *guarantee* directly via
configuration. Set `min.insync.replicas=3` on the topic (requiring **all**
3 replicas in sync — an artificially strict setting, chosen specifically so
that killing just 1 of 3 brokers would be enough to breach it, without
needing to touch 2 brokers and re-break quorum).

### The main experiment: acks=all rejects, acks=1 doesn't care

With `min.insync.replicas=3` and full ISR, confirmed `acks=all` works
normally. Then killed exactly **one** follower (`docker kill kafka-b1` — a
real `SIGKILL`, single node down, quorum intact):

```
ISR: 102,101,103  ->  (after ~10s)  ->  ISR: 102,103
```

2 replicas remain — below the required 3. Immediately tried both:

```bash
# acks=all
org.apache.kafka.common.errors.NotEnoughReplicasException: Messages are
rejected since there are fewer in-sync replicas than required.

# acks=1, same broken ISR, same moment
SUCCESS: [{"topicName":"failure-test","partition":0,"errorCode":0,"baseOffset":"11",...}]
```

**Exactly the proof**: `acks=all` refuses to lie — it rejects the write
outright rather than falsely reporting success when durability can't be
guaranteed. `acks=1` doesn't check any of this — it happily keeps accepting
and acknowledging writes regardless of ISR health.

### The actual best finding: "successfully acked" isn't the same as "visible"

Went to verify the `acks=1` writes landed by consuming the topic. **They
weren't there** — despite kafkajs reporting `SUCCESS` with a real assigned
offset (11). Checked the raw log-end-offset directly:

```bash
kafka-get-offsets.sh ... --time -1
# failure-test:0:9        <- high watermark stuck at 9
```

But the leader had already accepted (and kafkajs had already gotten
success responses for) messages up through **offset 11**. Three
successfully-acknowledged messages, sitting on the leader's disk, completely
invisible to every consumer.

**Why**: the **high watermark** — the offset boundary that determines what
consumers are allowed to read — only advances once enough replicas
(governed by the topic's `min.insync.replicas`/ISR state) have caught up.
`acks=1` controls what the *producer* has to wait for before getting a
response; it has **no bearing at all** on when the *broker* considers that
data safe to expose to readers. These are two completely independent gates.

**Proved this conclusively** by restarting the dead follower and watching:

```
[14:53:09] failure-test:0:9
[14:53:13] failure-test:0:12   <- jumped from 9 to 12 the instant ISR was restored
```

All 3 previously-invisible messages appeared at once, at their original
offsets (9, 10, 11) — not re-sent, not duplicated, just finally exposed:

```
Offset:9   test-acks1-succeed    {"n":100}
Offset:10  test-acks1-retry      {"n":101}
Offset:11  kafkajs-acks1-test    {"n":200}
```

### What this means, precisely

There are **three genuinely separate concepts**, easily conflated into one:

1. **"The producer got an ack"** — governed entirely by `acks` (0/1/all)
2. **"The data physically exists on a broker's disk"** — true the moment the
   leader accepts it, regardless of `acks` level
3. **"A consumer can read it"** — governed by the high watermark, which
   depends on ISR/replication catching up, **independent of what `acks`
   level the producer used**

`acks=1` only ever controls concept #1. It has zero influence over #3. This
means an `acks=1` write can be "successfully" acknowledged and then sit in
limbo — accepted but unreadable — for an indefinite time if replication
stalls, *before* anyone even gets to the question "what if the leader
crashes now." That crash-scenario risk (raised back in Stage 5, revisited in
Stage 11) is real too, and follows directly from this: if the leader died
while a message was in this limbo state, the new leader (elected from the
replicas that never got it) simply wouldn't have it — silent loss, exactly
as theorized. But the *visibility* gap this stage actually caught and proved
is arguably the more surprising, more immediately practical thing to
understand: **"my producer got acks=1 success" is not the same claim as "my
data is durable" is not the same claim as "my data is readable" — three
separate guarantees, three separate mechanisms, and this project spent 12
stages slowly separating them one at a time.**

### Key concepts introduced

- **Combined broker+controller nodes share fate at the control-plane level**
  — losing quorum majority (not just data-plane replicas) can stall the
  *entire* cluster, a real reason production KRaft deployments often use
  dedicated controller nodes.
- **`acks=all` + `min.insync.replicas` is a hard, enforced contract**: the
  broker actively rejects writes it can't safely guarantee, rather than
  silently accepting a weaker guarantee than requested.
- **`acks=1` provides zero information about replication state** — it
  reports producer-side success only, completely decoupled from whether
  followers ever catch up.
- **The high watermark (consumer read boundary) is a distinct mechanism
  from producer acknowledgment** — a message can be "successfully written"
  and still be completely unreadable, for as long as replication is
  unhealthy, regardless of `acks` level.
- **Not every experiment goes as planned, and that's fine to report
  honestly** — the replication-throttle attempt didn't work, the first
  pause-based design broke something bigger than intended. Both are still
  genuine, useful findings, documented as what actually happened rather than
  rewritten into a clean narrative that didn't occur.

### Project retrospective

Twelve stages, starting from `docker compose up` on a single broker with no
topics, ending with a second, independent 3-broker cluster deliberately
broken and recovered in three different ways in this stage alone. Every
stage's guiding idea held up under this project's own central method:
**don't trust a claim about Kafka — including this project's own claims from
earlier stages — until you've watched it happen, broken it on purpose, and
checked the raw evidence yourself.** That method caught real surprises at
almost every stage: the Stage 4 stale-session delay, the Stage 6 poison
pill (which went on to reappear in Stages 7, 8, and 9), the Stage 9 join
window discovery, the Stage 11 controlled-shutdown-vs-crash timing gap, and
now this stage's producer-ack-vs-visibility gap. None of these were planned
in advance — all of them came from actually running the thing and reading
the real output instead of assuming.

---

## Stage 13 — Load testing & producer/consumer tuning

**Date**: 2026-08-03

**Goal**: opens Part 2 (Scale). Move past "does the feature work" (Part 1) to
"what actually happens to throughput as volume grows, and does tuning
`batch.size`, `linger.ms`, `compression.type`, and `acks` make a *measured*
difference, not a theoretical one." Built a configurable load producer/
consumer (`src/stage13-load-testing/`) against a new 6-partition `load-test`
topic and ran real experiments on this project's actual single-broker
cluster.

### kafkajs has no `batch.size`/`linger.ms` — had to build the equivalent

First surprise: the Java client's `batch.size`/`linger.ms` are producer-level
settings that automatically accumulate records per partition and flush on
whichever limit hits first. **kafkajs has no equivalent** — every
`producer.send()` call is its own produce request immediately. To actually
tune this axis at all, `load-producer.ts` implements the same policy by
hand: an accumulation buffer that flushes when it reaches `BATCH_SIZE`
messages, or when `LINGER_MS` has elapsed since the first message currently
sitting in it, whichever comes first — real client-side batching, just
application-level instead of library-level.

### Batch size: the single biggest lever by far

Same 50,000 messages (256B each), same `load-test` topic, only `BATCH_SIZE`
changed:

```
BATCH_SIZE=1      ->  50000 requests  ->    2,318 msgs/sec  (0.57 MB/sec)
BATCH_SIZE=100    ->    500 requests  ->   60,459 msgs/sec  (14.76 MB/sec)
BATCH_SIZE=500    ->    100 requests  ->   81,037 msgs/sec  (19.78 MB/sec)
BATCH_SIZE=2000   ->     25 requests  ->  108,932 msgs/sec  (26.59 MB/sec)
BATCH_SIZE=10000  ->     10 requests  ->  115,473 msgs/sec  (28.19 MB/sec)  [100k msgs]
```

**~47x throughput** between unbatched and `BATCH_SIZE=2000`, on the exact
same broker, same network, same payload — the entire difference is fewer,
larger produce requests instead of one network round trip per message. Gains
clearly plateau past ~2000–10000 messages/batch (single producer client,
single broker, loopback network — this is client/request-overhead-bound,
not broker-bound, well before compression or partitioning enter the
picture). This is the real, measured version of the "batch.size matters
more than almost anything else" claim from the Part 2 roadmap notes —
confirmed on this project's own cluster, not taken on faith from the
tuning-guide sources cited there.

### Compression: real ratios, real CPU cost — and a real dead end fixed along the way

Wanted `none` vs `gzip` vs `lz4` vs `zstd`, matching the roadmap's specific
claim ("zstd is the modern default — better ratio than lz4 at acceptable CPU
cost"). Getting all four working was its own multi-step investigation:

**kafkajs ships only `gzip` natively** — `lz4`/`zstd`/`snappy` all throw
`KafkaJSNotImplemented` unless a codec is registered via
`CompressionCodecs`/`CompressionTypes`. First attempt: the "obvious" npm
packages, `kafkajs-lz4` and `@kafkajs/zstd`, both wrap native (`node-gyp`)
bindings. Both failed to build in this environment:

```
fatal error: 'memory' file not found
    at v8.h:21:10
```

— a broken system C++ toolchain (Xcode Command Line Tools installed but
unable to compile even a trivial `#include <memory>`), not a project-specific
issue. Rather than repair the whole machine's toolchain for one npm install
(out of scope, and a real environment problem worth reporting honestly
rather than silently working around), used prebuilt-binary/pure-JS
alternatives instead: **`@mongodb-js/zstd`** (ships prebuilt native binaries
via `prebuild-install`, no local compile needed — worked immediately) and
**`lz4js`** (pure JavaScript, no native code at all).

`lz4js` compressed and round-tripped fine *locally*, but sending it through
the real broker hard-failed:

```
Caused by: java.lang.RuntimeException: Dependent block stream is unsupported
	at Lz4BlockOutputStream$FLG.validate
	at Lz4Compression.wrapForInput
	at LogValidator.validateMessagesAndAssignOffsetsCompressed
```

Root cause, found by reading `lz4js`'s own source: its `compressFrame()`
reuses **one shared hash table across the entire frame** instead of a fresh
one per block, meaning later blocks can genuinely reference earlier blocks'
data — real block dependency, not just a mislabeled flag. Worse, the LZ4
frame spec's block-independence bit exists in the source as a dead constant
(`// var fdBlockIndep = 0x20;`, commented out) — the library never sets it,
so the frame header doesn't even honestly declare what it's doing. Kafka's
broker validates this on write and rejects it outright rather than accepting
a frame it can't safely decompress.

Fixed it properly instead of giving up on LZ4 entirely: `lz4-codec.ts`
reimplements just the frame-writing loop around `lz4js`'s own low-level
`compressBlock`/`compressBound` primitives, allocating a **fresh
`Uint32Array` hash table per block** (so blocks are genuinely
independent) and setting the independence bit correctly in the frame
header, with a manually-recomputed header checksum via `lz4js`'s own
`xxh32` module. Verified two ways: kafkajs round-trip, and independently
via the real Java `kafka-console-consumer.sh` reading the same messages
back as plain JSON — proof this isn't just self-consistent with our own
decoder, a real Kafka client accepts and decompresses it correctly.

Ratios and CPU cost, same 50,000 messages, 512B payloads (a repeated-phrase
JSON `notes` field — deliberately compressible, unlike random bytes, to be
representative of real JSON/log traffic rather than an artificially
compression-hostile benchmark):

| Compression | Throughput | Ratio | Compress CPU time (50k msgs / 100 requests) |
|---|---|---|---|
| none | 70,721 msgs/sec | — | — |
| gzip | 80,906 msgs/sec | 60.09x (measured separately via `zlib`) | 171ms |
| lz4 | 77,882 msgs/sec | 20.97x | 117ms |
| zstd | 74,074 msgs/sec | **46.61x** | **48ms** |

**zstd beats lz4 decisively on both axes at once** — more than double the
ratio *and* less than half the CPU time — a clean, direct confirmation of
the roadmap's claim, not just a repeated vendor benchmark. The one genuine
surprise: **gzip's ratio actually beat zstd's here**, which contradicts the
usual "zstd > gzip" folklore. Cause, not a discrepancy: this payload is
extreme, repeated-phrase text, a shape gzip's Huffman+LZ77 combination
handles unusually well, at the cost of being ~3.5x slower than zstd to get
there. Real lesson, not a canned one: **compression ratio is payload-shape
dependent enough that "zstd is best" needs verifying on your own actual
data, not assumed from a generic benchmark** — this project's own numbers
argue for measuring, echoing exactly the method every earlier stage used.
Throughput differences between codecs here are within this environment's
noise (single broker, loopback network, no real I/O cost being saved) —
the ratio and CPU-time numbers are the meaningful signal, not the
msgs/sec column, at this scale.

### acks: still costs something even at RF=1

Same 50,000 messages, `BATCH_SIZE=500`, only `acks` changed:

```
acks=0   ->  172,414 msgs/sec
acks=1   ->   94,518 msgs/sec
acks=-1  ->   83,195 msgs/sec
```

Expected `acks=0` to win (no broker round-trip wait at all) — it does, by
~1.8x over `acks=1`. Mildly surprising: `acks=-1` (all in-sync replicas)
is still measurably slower than `acks=1`, **on this exact single-broker,
RF=1 topic**, where there are no followers at all for the leader to wait on
— Stage 5/12 already established `acks=-1` degenerates to waiting on just
the leader when RF=1. The gap here isn't replication wait time (there's
nothing to replicate to); it's the extra ISR-bookkeeping/acknowledgment
path the broker still runs for `acks=-1` even when that set has exactly one
member. Small, but real, and not something the "RF=1 makes acks=1 and
acks=-1 equivalent" mental model from Stage 5 fully predicted.

### linger.ms: not about throughput, it's about bounded latency

Tried to design a clean before/after and got a more instructive result than
planned. Paced production at a realistic `RATE=200` msgs/sec (not
max-throughput) with a deliberately large `BATCH_SIZE=1000`:

```
LINGER_MS=0,  BATCH_SIZE=1000  ->  1 produce request total, sent at 5310ms
LINGER_MS=50, BATCH_SIZE=1000  ->  93 produce requests, ~11 msgs/request
BATCH_SIZE=1  (no batching at all, immediate per-message send)  ->  1000 requests, 7278ms
```

The `LINGER_MS=0` run wasn't a mistake to fix — it's the actual point.
With no linger bound and a batch size larger than what arrives during the
test, the buffer **never filled**, so nothing was sent until the script's
final explicit flush at the very end — meaning the very first message
produced sat unsent, entirely unlingered, for the full ~5.3 seconds of the
run. That's the real production risk `linger.ms` exists to prevent: without
it, "batch when full" has an **unbounded worst case** under low/uneven
traffic. `LINGER_MS=50` fixed that directly — 93 requests instead of 1000
(far fewer than the unbatched baseline) while capping any message's added
wait to ~50ms. The unbatched (`BATCH_SIZE=1`) baseline, despite sending
immediately, was actually the *slowest* to finish overall (7278ms vs
5658ms) — 1000 sequential network round-trips compounding on top of the
pacing delay costs more, in aggregate, than the latency `linger.ms` was
supposedly there to avoid. **linger.ms's real job is bounding worst-case
per-message latency, not maximizing throughput** — the throughput benefit
of batching is `batch.size`'s contribution; `linger.ms` is what makes
batching viable under real, uneven traffic instead of only at saturation.

### End-to-end verification

Real proof the tuned settings still deliver correctly, not just "the
producer's own send() call reported success" (a lesson Stage 12 made
impossible to take for granted again): produced 200,000 messages
(`BATCH_SIZE=500`, `zstd`, `acks=1`) — 80,873 msgs/sec, 106.5MB compressed
down to 2.29MB (46.46x) — then consumed the topic fresh from the beginning:

```
--- load-consumer: received 200000 messages from "load-test" ---
elapsed (first->last message): 284ms | 704225 msgs/sec | 343.86 MB/sec
```

All 200,000 accounted for. Hit one more real bug getting here: the consumer
crashed outright the first time (`KafkaJSNotImplemented: ZSTD compression
not implemented`) — registering a custom compression codec via
`CompressionCodecs` has to happen on **both** the producer and the consumer
side; it's not implied by whatever codec the message was written with.
Obvious in hindsight, not obvious until it crashed.

Also worth recording: a fresh consumer group's **initial join/rebalance
alone took ~3 seconds** before a single message could arrive, independent
of topic size — `load-consumer.ts`'s default idle-timeout had to be raised
from 5s to 10s after an early run reported "0 messages" that turned out to
be the idle timer firing mid-join, not an empty topic.

### Key concepts introduced

- **kafkajs has no client-side auto-batching (`batch.size`/`linger.ms`)** —
  unlike the Java client, every `send()` is its own request; matching that
  tuning behavior requires implementing the accumulate-then-flush policy by
  hand.
- **Batch size is the dominant throughput lever** — ~47x measured between
  unbatched and a tuned batch size on identical hardware/payload, dwarfing
  every other single knob tested this stage.
- **`linger.ms` bounds worst-case latency, it doesn't raise throughput** —
  its value shows up specifically under low/uneven traffic, capping how
  long a message can sit unsent in a not-yet-full batch; conflating it with
  a throughput knob misses its actual job.
- **Compression ratio is payload-shape dependent** — zstd beat lz4 on both
  ratio and CPU cost as expected, but gzip's ratio (not its speed) beat
  zstd's on this specific highly-repetitive payload, a direct demonstration
  that generic "zstd is best" claims need verifying against real data.
- **Compression codecs must be registered on both producer and consumer** —
  a non-obvious kafkajs requirement; the consumer doesn't infer the codec
  from anywhere but its own registered `CompressionCodecs` table.
- **`acks=-1` still costs something even when RF=1 gives it nothing to
  replicate to** — the ISR-bookkeeping path itself has overhead, not just
  the network wait for followers.
- **A third-party library can be wrong in a way worth fixing, not just
  working around** — `lz4js`'s frame-independence bug had a small, provable
  fix once traced to its source, verified independently against a real
  Java Kafka client rather than trusted on the strength of a passing local
  test.

---

## Stage 14 — Partitioning strategy under skewed data

**Date**: 2026-08-03

**Goal**: prove the specific counter-intuitive claim from the Part 2
roadmap directly — adding more partitions does not fix a single hot key —
then fix it for real with key salting, and measure both, on this project's
own cluster. Built `src/stage14-partitioning/`: a producer that generates
deliberately skewed traffic (one hot key gets 80% of messages, the rest
spread across 5000 normal keys) and a distribution checker that reads real
per-partition message counts back from the broker via the admin client
(`fetchTopicOffsets`) — not a guess, the actual on-disk count per partition.

### Phase 1: 6 partitions, one hot key — instantly lopsided

```
partition 0:   1789 (3.6%)
partition 1:   1699 (3.4%)
partition 2:   1533 (3.1%)
partition 3:   1664 (3.3%)
partition 4:   1732 (3.5%)
partition 5:  41583 (83.2%)  <-- over the ~20% guideline
```

Exactly as expected: the hot key hashes to one partition (5, via kafkajs's
`murmur2(key) % numPartitions` — same algorithm as the Java client's
default partitioner), and that partition alone carries 83.2% of all 50,000
messages.

### Phase 2: 4x the partitions, same hot key — barely moves

Recreated the topic with 24 partitions instead of 6 (everything else
identical) and reran:

```
partition 23:  40351 (80.7%)  <-- over the ~20% guideline
(all other 23 partitions: 0.7%–1.0% each)
```

**This is the actual proof, not just the claim repeated**: going from 6 to
24 partitions — 4x more — moved the worst-case share by less than 3
percentage points (83.2% → 80.7%), well within noise. The hot key still
hashes to exactly *one* partition out of however many exist; more
partitions only ever thins out the *other* keys sharing that pool, never
splits the one key that's actually the problem. A single key, being
indivisible, can only ever live on one partition — no partition count fixes
that, by construction.

**Why this actually matters, concretely**: from Stage 4, a partition is
only ever consumed by one consumer within a group at a time. An
80%-loaded partition means one consumer instance does ~5x the work of
every other instance in the group, permanently — adding a 7th, 8th, 25th
consumer to a 24-partition topic changes nothing about that, since there's
nothing left for the extra consumers to claim once every partition already
has one owner.

### Phase 3 & 4: fixing it with key salting — and a real collision lesson

Went back to 6 partitions (isolating the variable: only salting changed
from Phase 1, not partition count) and split the hot key into `N` sub-keys
(`customer-VIP#0` .. `customer-VIP#N-1`, round-robin), the standard fix:

```
SALT_BUCKETS=8:   worst partition 23.5%  <-- still just over the ~20% guideline
SALT_BUCKETS=64:  worst partition 19.7%  OK
```

8 buckets already turned an 83.2% worst case into 23.5% — a huge
improvement — but not quite under the 20% target. Real reason, not a fluke:
with only 6 partitions and 8 salt buckets, by the pigeonhole principle at
least one partition is guaranteed to land 2 of the 8 sub-keys, so it still
gets roughly double the average share. **Salting only works as well as
`salt buckets` vs. `partition count` allows** — 64 buckets against 6
partitions gives every partition many chances to collect a
close-to-even mix of sub-keys, landing right at the 20% line.

### A simulator to test-drive this before touching a real broker

Built `simulate-partitions.ts` to answer "how many salt buckets do I
actually need" without producing 50,000 real messages per candidate value —
it reaches into kafkajs's own (internal, undocumented) `murmur2` module
directly, so the simulation uses the *exact* hash real traffic would get,
not a reimplementation that could quietly drift from it:

```
partitions | salt buckets | worst-partition share
         6 |          off | 83.2%  <-- over ~20%
         6 |            8 | 23.5%  <-- over ~20%
         6 |           64 | 19.6%  OK
        12 |            8 | 21.7%  <-- over ~20%
        12 |           16 | 11.8%  OK
        24 |            8 | 20.8%  <-- over ~20%
        24 |           16 | 10.9%  OK
```

**Every one of these numbers matches the real-broker measurements from
Phases 1–4 to within a few tenths of a percent** (83.2% vs 83.2%, 23.5% vs
23.5%, 19.6% vs 19.7%) — real, useful cross-validation that the simulator
is trustworthy, and a genuinely practical tool: this is what "simulate
partition assignment before committing to a key" (from the roadmap's
research sources) means concretely — cheap enough to sweep dozens of
partition-count/salt-bucket combinations in milliseconds before deploying
any of them for real.

### Summary: how to actually decide partition count and salt buckets

Pulling Phases 1–4 into a concrete decision process, not just isolated
experiments — this is the order the decisions actually need to happen in
for a real pipeline, each step grounded in something this stage measured
rather than a rule taken on faith:

1. **Start from the consumer side.** Partition count caps how many
   consumer instances can ever run in parallel (Stage 4: one partition, one
   consumer within a group, at a time). Decide the realistic max consumer
   count *first* — partition count is downstream of that, not independent
   of it.
2. **Overprovision partitions to ~2–3x that planned consumer count.**
   1x leaves no room to scale consumers later without a disruptive
   repartition (which reshuffles every key's partition assignment, not
   just new traffic); far more than 3x just adds broker overhead
   (file handles, replication, metadata) for no benefit.
3. **Measure real key distribution before deciding anything about
   salting** — from actual traffic/logs, not a guess. This is the input
   Phase 1–2 assumed as given (an 80/20-style hot key); a real system has
   to go get this number first.
4. **Check the worst-case share at the chosen partition count** — via
   `check-distribution.ts` against a real topic, or `simulate-partitions.ts`
   against none yet. Phase 2 proved this explicitly: quadrupling partitions
   alone (6 → 24) barely moved an 83%-share key to 80.7% — partition count
   by itself does not fix this, so this check has to happen regardless of
   how many partitions you already provisioned in step 2.
5. **Only salt the keys that actually fail step 4** — not every key.
   Salting has a real cost (next step), so it should be applied
   surgically to identified hot keys, not as a blanket default.
6. **Pick salt bucket count by simulation, then confirm for real.**
   Phase 3/4 showed buckets need to meaningfully *outnumber* partitions
   (roughly 8–10x) to avoid pigeonhole collisions eating the fix — 8
   buckets on 6 partitions only reached 23.5% (still failing the ~20%
   target); 64 buckets reached 19.7%. `simulate-partitions.ts` sweeps this
   in milliseconds; `check-distribution.ts` confirms it held on the real
   broker afterward.
7. **Weigh the ordering trade-off before shipping the salt.** Salting
   breaks per-key ordering — a customer's events can now land on different
   partitions. Worth it for traffic that doesn't need strict per-key order
   (most analytics/logging shapes); needs a re-aggregation step downstream,
   or a different fix entirely, for traffic that does.
8. **Recheck periodically, not once.** Which key is "hot" drifts as real
   traffic grows and shifts — today's average key can become tomorrow's
   80%-share key. The check from step 4 is a recurring operational check,
   not a one-time launch decision.

### Key concepts introduced

- **More partitions never fixes a single hot key** — proved directly, not
  assumed: 4x the partitions moved the worst-case share by under 3
  points. A key is atomic with respect to partition assignment; only
  splitting the key itself (salting) can split its load.
- **Key salting is the real fix, but its effectiveness depends on salt
  bucket count relative to partition count** — too few buckets relative to
  partitions still collides and under-delivers (8 buckets over 6
  partitions: 23.5%, still over target); buckets need to meaningfully
  outnumber partitions to approach even distribution.
- **Partition-level hot spots are a permanent consumer-group bottleneck**
  — not a transient one — because a partition is owned by exactly one
  consumer within a group at a time (Stage 4); no amount of horizontal
  consumer scaling helps once every partition already has an owner.
- **You can simulate partition assignment cheaply before deploying a key
  design** — using the exact same hash function the client uses in
  production (not an approximation), verified here against real
  measured broker data to build confidence it's trustworthy.

---

## Stage 15 — Kafka → ClickHouse

**Date**: 2026-08-03

**Goal**: the Part 2 centerpiece — a real analytics pipeline for millions
of events, not just another ingestion exercise. Stand up ClickHouse, wire
it to Kafka via the canonical pattern (Kafka Engine table → Materialized
View → MergeTree storage table), push millions of synthetic e-commerce
events through it, run real analytical aggregation queries, and compare
against the alternative of a Kafka Connect ClickHouse sink connector —
directly extending Stage 8's Postgres/Connect work with a store actually
built for OLAP.

### Setup: `analytics-events` topic and a realistic synthetic event stream

`src/stage15-clickhouse/events-producer.ts` generates a funnel-shaped event
stream (`view` → `add_to_cart` → `purchase`, weighted 70/22/8 — not
uniform, real user funnels drop off) across 6 categories, 8 countries,
20,000 products, and 500,000 users, with `event_time` spread across a
7-day window (not all "now") so time-bucketed queries have something real
to show. Same accumulate-then-flush batching approach as Stage 13.

### The pipeline: Kafka Engine table → Materialized View → MergeTree

`src/stage15-clickhouse/schema.sql` is the actual mechanism, and it's three
distinct objects, not one:

1. **`analytics_events_queue`** (`ENGINE = Kafka`) — not a real table, a
   *view* over the Kafka topic. Querying it directly consumes messages
   like a normal consumer (once, destructively) — it stores nothing.
2. **`analytics_events`** (`ENGINE = MergeTree`) — the actual persistent,
   columnar storage table every analytical query in this stage runs
   against.
3. **`analytics_events_mv`** (materialized view) — the glue. ClickHouse
   runs this `SELECT ... FROM analytics_events_queue` continuously in the
   background and `INSERT`s every result block into `analytics_events`.
   **Without this MV attached, the Kafka engine table just sits there
   unread** — it's easy to assume step 1 alone does the ingesting; it
   doesn't.

One real conversion step lives in the MV: the producer sends `event_time`
as a Unix-epoch-millis integer (simplest, most portable format across a
JSON-based pipeline); the MV converts it with
`fromUnixTimestamp64Milli(event_time)` into a proper `DateTime64(3)` column
on the way into `MergeTree` — done once, declaratively, in SQL.

### 2 million events, verified end-to-end

```
--- events-producer: sent 2,000,000 events to "analytics-events" in 400 requests ---
elapsed: 13.9s | 144279 events/sec
```

Checked ClickHouse's count within a couple seconds of the producer
finishing — already fully caught up:

```sql
SELECT count() FROM kafkaos.analytics_events  -->  2000000
```

The funnel weights show up correctly in the real aggregated data too
(70/22/8 target vs. 69.98/22.04/7.99 observed) — confirms the whole path
(producer → Kafka → Kafka engine table → MV → MergeTree) preserves data
correctly, not just message counts.

### Real analytical queries, real timings (via `system.query_log`, not a stopwatch)

All four ran against the full 2,000,000-row table, timings from
ClickHouse's own `query_duration_ms` (server-side, not client round-trip):

| Query | Result shape | Duration | Rows read |
|---|---|---|---|
| Revenue by category | 6 groups | **8ms** | 2,000,000 |
| Event counts by type | 3 groups | **13ms** | 2,000,001 |
| Top-10 products by revenue | 10 rows | **19ms** | 704,512 |
| Daily volume/revenue trend | 8 days | **18ms** | 2,000,000 |

Full-table aggregations over 2 million rows, single digit-to-low-double-
digit milliseconds, with no indexes beyond the table's own `ORDER BY
(category, event_type, event_time)` — the entire point of a columnar
engine: these queries only ever touch the 2–3 columns they actually
reference (`read_size` for the daily-trend query: 32.42 MiB, a fraction of
the table's 103 MiB total).

### The comparison that actually matters: same data, same query, Postgres

Rather than assert "ClickHouse is faster for analytics" from general
knowledge, loaded the **exact same 2,000,000 rows** (exported from
ClickHouse as CSV, `\COPY`'d into a fresh Postgres table — Stage 8's
Postgres, no special tuning, no extra indexes, the realistic "general-
purpose OLTP store asked to do analytics" scenario) and ran equivalent
queries:

| Query | ClickHouse | Postgres | Ratio |
|---|---|---|---|
| Revenue by category | 8ms | 392ms | **~49x** |
| Event counts by type | 13ms | 183ms | **~14x** |
| Daily volume/revenue trend | 18ms | 128ms | **~7x** |

Plus storage: the identical 2,000,000 rows took **103.28 MiB** in
ClickHouse vs. **247 MB** in Postgres — ~2.4x smaller, from per-column
compression (the `LowCardinality(String)` columns — `category`,
`event_type`, `country` — compress especially well with only a handful of
distinct values each). Not a fabricated benchmark: identical data, same
machine, same moment, only the storage engine differs.

### Comparison: Kafka Engine + MV vs. the Kafka Connect ClickHouse sink connector

Set up the alternative for real, not just in theory: registered ClickHouse
Inc's official sink connector (`clickhouse/clickhouse-kafka-connect`, on
Confluent Hub — installed the same way Stage 8 installed the JDBC
connector) against a second topic (`analytics-events-connect`) feeding a
separate identical table. Hit three real, honestly-worked-through failures
getting there:

1. **ClickHouse's HTTP interface rejected Kafka Connect's connection
   entirely** (`Authentication failed`) even though `clickhouse-client`
   worked fine via `docker exec` the whole stage. Root cause: the
   `clickhouse-server` image only trusts the `default` user
   password-lessly **from inside its own container** (localhost) — Kafka
   Connect, a different container, is a real remote client and got a real
   auth rejection. Fixed by setting `CLICKHOUSE_PASSWORD` in
   docker-compose (a genuine config gap this stage's local-only testing
   had been silently relying on).
2. **The exact same key-converter mistake from Stage 8, recurring
   independently**: `key.converter: JsonConverter` failed trying to parse
   a plain string key (`"user-153658"`) as JSON. Same fix as Stage 8:
   `key.converter: org.apache.kafka.connect.storage.StringConverter` —
   worth noting this wasn't remembered from Stage 8's notes going in, it
   was rediscovered the same way, by hitting the same real error again.
3. **`Table 'kafkaos'.'analytics-events-connect' does not exist`** — this
   connector maps topic name → table name **literally, by default** (no
   config option used here to remap it); the target table had been named
   `analytics_events_connect` (underscores) while the topic used hyphens.
   Fixed by creating the table with the topic's exact name (backtick-
   quoted: `` `analytics-events-connect` ``).

Once fixed (`src/stage15-clickhouse/connectors/clickhouse-analytics-sink.json`,
applied via `apply-connectors.sh` — the same reproducible-connector-config
convention Stage 8 established), it worked identically well: 2,000,000
events ingested, caught up within seconds, **103.14 MiB** on disk (matching
the MV path's 103.28 MiB almost exactly — same `MergeTree` engine
underneath, so no surprise), and the same category-revenue query ran in
32ms — same order of magnitude as the MV path's 8ms, the gap well within
what's explained by one being a freshly-warmed cache and the other not.

**One genuine, non-obvious operational difference favoring the Connect
path**: it converted the `event_time` epoch-millis integer straight into
the target `DateTime64(3)` column **automatically**, with no equivalent of
the MV's explicit `fromUnixTimestamp64Milli()` cast needed anywhere in the
connector config.

**The real trade-off, from actually running both**:
- **Kafka Engine + MV** — no extra service to run (ClickHouse itself is
  the Kafka consumer); the transform lives declaratively in versioned SQL
  (`schema.sql`, checked into this repo); but it's ClickHouse-specific
  plumbing with no standard external monitoring surface — no REST API to
  check consumer status the way Connect has.
- **Kafka Connect sink** — reuses infrastructure this project already runs
  for Stage 8 (and will again for Stage 23's Debezium outbox work) with a
  standard, uniform operational surface (`GET /connectors/.../status`,
  pause/resume/restart) across every sink regardless of target system; but
  it's another moving service, and, as demonstrated here, comes with its
  own set of connector-specific configuration gotchas to work through
  (converter mismatches, literal topic→table naming) independent of
  ClickHouse itself.

Neither is a strictly better default — which one wins depends on whether
this pipeline needs to fit into an existing standardized Connect-based
ops story (favors Connect) or stay minimal with transform logic living in
version-controlled SQL (favors the native Kafka engine).

### Key concepts introduced

- **The Kafka table engine is a view, not a table — the materialized view
  is what actually makes ingestion happen.** Querying the Kafka engine
  table directly consumes messages destructively and stores nothing;
  without an attached MV, nothing lands in real storage at all.
- **ClickHouse's columnar storage delivers a real, measured, order-of-
  magnitude analytical-query advantage over a general-purpose row store**
  — 7x to 49x faster across four different aggregation shapes, on
  identical data, not a vendor claim taken on faith.
- **That advantage comes specifically from reading only the columns a
  query touches** — confirmed directly via `read_bytes`/`read_rows` in
  `system.query_log`, not inferred.
- **Columnar + per-column compression roughly halves storage for the same
  data** (103MB vs 247MB here) — `LowCardinality` columns compress
  especially well when the real cardinality is genuinely low.
- **Kafka Engine+MV and Kafka Connect sink converge to the same
  destination** (same `MergeTree` engine, same query performance, near-
  identical disk usage) — the real difference between them is
  operational (where transform logic lives, what monitoring surface
  exists), not a data-correctness or performance one.
- **The same integration mistakes recur across unrelated stages** — the
  JSON-vs-string key-converter error from Stage 8 reappeared here,
  independently rediscovered rather than remembered, a real argument for
  why this journal exists instead of relying on memory.
- **Docker image auth defaults can silently work in dev and fail for a
  different container** — `clickhouse-client` via `docker exec` succeeded
  the entire stage on an implicit localhost trust that Kafka Connect,
  a genuinely different container, doesn't get; caught only once a second,
  real client tried to connect.

---

## Stage 16 — Scaling stateful processing

**Date**: 2026-08-03

**Goal**: extend Stage 9's ksqlDB work to see how RocksDB-backed state
actually gets distributed and rebalanced as processing instances are added
or removed under real load — and prove co-partitioning requirements get
sharper, not easier, once processing is genuinely distributed across
multiple instances instead of running on a single server where
partition-to-instance mapping is moot.

### Getting a real 3-node ksqlDB cluster up was its own fight

Added `ksqldb-server-2` and `ksqldb-server-3` to `docker-compose.yml`,
sharing the same `KSQL_KSQL_SERVICE_ID` as the original `ksqldb-server` —
that shared service ID is what makes three independent servers into one
logical cluster (it becomes the underlying Kafka Streams `application.id`
and consumer group, so all three fight over the same persistent queries'
partitions via the same rebalance protocol from Stage 4).

Three real problems before it actually worked:

1. **`/clusterStatus` returned 404 on every server** — off by default in
   open-source ksqlDB. Needed `KSQL_KSQL_HEARTBEAT_ENABLE=true` and
   `KSQL_KSQL_LAG_REPORTING_ENABLE=true` explicitly.
2. **Servers self-advertised their raw, unresolvable container hostname**
   (e.g. `bbbe5b54f536:8088`) to peers instead of something reachable —
   confirmed via `docker exec ksqldb-server hostname`. Docker's embedded
   DNS resolves `container_name`/service name, not a container's internal
   Linux hostname, so peers could never actually reach each other for the
   heartbeat protocol. Fixed with an explicit
   `KSQL_KSQL_ADVERTISED_LISTENER: http://ksqldb-server:8088` (and the
   matching value on each of the other two) pointing at each server's own
   `container_name`.
3. **Real memory exhaustion, not a config error**: starting all three
   ksqlDB JVMs simultaneously alongside everything else already running
   (4 Kafka brokers, Kafka Connect, ClickHouse, schema registry, Postgres)
   silently killed the *original* `ksqldb-server` (`docker logs` showed a
   bare `Killed` — an OOM kill, not a crash with a stack trace) — this
   Docker Desktop VM has a 7.661 GiB memory ceiling, and each ksqlDB JVM
   alone settled around 1.3–1.9 GiB. Fixed pragmatically: stopped the
   Stage 11 3-broker cluster, Kafka Connect, and ClickHouse (none needed
   for this stage), brought the three ksqlDB servers up **one at a time**,
   confirmed each was healthy before starting the next, then restored the
   other services afterward and scaled back down to one ksqlDB server as
   the resting state (the two extra services stay defined in
   `docker-compose.yml`, just not running by default — this environment
   can't comfortably run everything at once).

### Before scaling: one server holds every partition's state

With just `ksqldb-server` running, `kafka-consumer-groups.sh --describe`
against `PAYMENTS_PER_MINUTE`'s underlying consumer group
(`_confluent-ksql-kafkaos_ksql_query_CTAS_PAYMENTS_PER_MINUTE_25`) showed
all 3 partitions of `payments`, plus all 3 partitions of the query's
internal repartition topic, owned by the same single host
(`172.19.0.6`) — 4 separate `StreamThread`s, all running inside one JVM.
Expected, but worth confirming directly rather than assuming.

### After scaling to 3: real redistribution, verified via the actual consumer group

Brought `ksqldb-server-2` and `ksqldb-server-3` online and re-ran the exact
same `--describe`. Each of the 3 `payments` partitions now belonged to a
**different** host:

```
payments-0  ->  ksqldb-server-2 (172.19.0.8)
payments-1  ->  ksqldb-server   (172.19.0.6)
payments-2  ->  ksqldb-server-3 (172.19.0.13)
```

One partition, and its RocksDB-backed aggregation state, per instance —
confirmed against `/clusterStatus`'s `activeStandbyPerQuery` too, which
independently reported the same per-host, per-partition ownership for all
three persistent queries at once (the join, the windowed aggregation, and
the latest-status table). Sent a batch of fresh payments and confirmed the
windowed aggregation (`PAYMENTS_PER_MINUTE`) kept updating correctly with
processing now genuinely split across three separate JVMs instead of one.

### Killing an instance mid-processing: real failover, not just a status flip

Identified `ksqldb-server-3` as the live owner of `payments` partition 2,
then killed it outright (`docker kill`, a real `SIGKILL` — same style as
Stage 12's broker-failure tests). The consumer group's metadata didn't
update immediately (Kafka's default consumer `session.timeout.ms` — a
real, measurable delay before a dead member is even detected, not
instant); after that window, partition 2 (and, in this rebalance, also
partition 1) had moved to the two surviving hosts, with **zero lag** on
either.

Confirming the group merely reassigned the partition isn't proof the
*state* actually recovered — proved that separately: sent a fresh,
properly-keyed payment (`order-post-failover-2`) after the failover and
ran a real pull query for it against `LATEST_PAYMENT_STATUS`:

```
SELECT * FROM LATEST_PAYMENT_STATUS WHERE ORDERID = 'order-post-failover-2';
-->  order-post-failover-2 | succeeded | 123.45
```

Correct, immediately. This is the concrete proof RocksDB state
reconstruction from Kafka's changelog topics — the mechanism Stage 9's
notes mentioned only in passing — actually works: the surviving host that
picked up the dead server's partition didn't have that partition's state
locally, rebuilt it from the changelog, and produced a correct answer with
no manual intervention.

**A real methodology mistake worth recording**: the first two attempts to
verify this used `kafka-console-producer.sh` piped in via `<<<` without
`docker exec -i`, and without setting a message key — both silently
produced either no message at all or a null-keyed one, which looked like
"the aggregation is broken after failover" until traced back to the test
method itself, not the system under test. Re-verified properly with
`docker exec -i` and an explicit key before trusting the result — a small
echo of Stage 12's own lesson about checking real evidence rather than the
first plausible-looking output.

### Co-partitioning: ksqlDB refuses a mismatched join outright

Created two new topics on purpose with **mismatched** partition counts —
`clicks-hot` (3 partitions) and `sessions-hot` (5) — declared streams over
each, then tried to join them:

```
Can't join `C` with `S` since the number of partitions don't match.
`C` partitions = 3; `S` partitions = 5. Please repartition either one
so that the number of partitions match.
```

**A hard rejection, not a silent wrong answer** — exactly the safer
failure mode. This is the concrete version of "co-partitioning
requirements get sharper at scale": on one instance, a join across
differently-partitioned topics might still look conceptually sensible
(everything runs in one place); the moment you actually need to
distribute processing across multiple instances — the entire point of
this stage — partition-to-instance ownership is only well-defined when
every joined topic shares the same partition count and partitioning key.
Recreated `sessions-hot` with 3 partitions (matching `clicks-hot`), and
the identical join statement succeeded immediately, then produced a
correct joined row for a real test event.

### Key concepts introduced

- **A shared `ksql.service.id` (`KSQL_KSQL_SERVICE_ID`) is what turns
  independent ksqlDB servers into one logical cluster** — it becomes the
  Kafka Streams `application.id`, so every server with the same value
  competes for the same persistent queries' partitions via ordinary
  consumer-group rebalancing.
- **Stateful task distribution is just consumer group partition assignment
  under the hood** — the exact mechanism from Stage 4, now assigning
  Kafka Streams tasks (and their RocksDB state stores) instead of raw
  partitions to a plain consumer.
- **RocksDB state is a local cache, not the source of truth** — the real
  source of truth is each state store's Kafka changelog topic; killing an
  instance and watching a survivor rebuild correct state from scratch
  (verified with a real pull query, not just a status check) is direct
  proof of that, not a theoretical claim.
- **Co-partitioning is enforced, not advisory** — ksqlDB refuses a join
  across topics with different partition counts outright, with a specific,
  actionable error, rather than silently producing incomplete or
  incorrect joined output.
- **This machine's resource ceiling is a real, load-bearing constraint on
  this project now** — running 3 ksqlDB JVMs alongside the rest of the
  stack silently OOM-killed a container with no application-level error at
  all; scaling stateful processing has a genuine hardware cost that a
  single-instance setup never surfaces.
- **A test's own tooling mistake can look identical to a real bug** — two
  broken manual verification attempts (missing `-i`, missing a message
  key) briefly looked like a state-recovery failure; re-checked with
  correct tooling before drawing any conclusion about the system itself.

---

## Stage 17 — Redpanda

**Date**: 2026-08-03

**Goal**: same Kafka wire protocol, a completely different implementation
(C++, thread-per-core, single binary — no JVM, no separate controller
quorum process to reason about) — a cheap, concrete way to see which parts
of everything learned so far are *Kafka-the-API* versus
*Kafka-the-implementation*. Reran Stage 13's exact load tests, unmodified,
against Redpanda instead of our real Kafka broker, and compared real
numbers plus operational feel. Also a specific question worth checking
directly rather than assuming: does Kafka 4.0's KRaft mode (no separate
ZooKeeper — this project's own broker already runs KRaft-only) already
close the operational-simplicity gap Redpanda was originally built to
exploit?

### The one actual code change: a connection string

Added a single `BROKERS` env var to `src/shared/kafka.ts` (defaulting to
our real broker, `localhost:9092`), replacing the previously hardcoded
address. That's it — every producer, consumer, and script from every prior
stage now points at Redpanda just by setting `BROKERS=localhost:9195`, with
**zero changes to any actual application logic** — the same
`load-producer.ts`/`load-consumer.ts` from Stage 13, byte-for-byte, ran
against Redpanda and produced correct results. This is the whole point of
wire-protocol compatibility, demonstrated directly rather than asserted.

### Throughput: Kafka vs. Redpanda, same code, same machine

| Config | Kafka (Stage 13) | Redpanda |
|---|---|---|
| `BATCH_SIZE=1` (unbatched) | 2,318 msgs/sec | 2,733 msgs/sec |
| `BATCH_SIZE=500` | 81,037 msgs/sec | 73,964 msgs/sec |
| `BATCH_SIZE=2000` | 108,932 msgs/sec | 110,375 msgs/sec |
| `BATCH_SIZE=10000` | 115,473 msgs/sec | 110,742 msgs/sec |
| `acks=0` | 172,414 msgs/sec | 161,290 msgs/sec |
| `acks=1` | 94,518 msgs/sec | 87,413 msgs/sec |
| `acks=-1` | 83,195 msgs/sec | 88,028 msgs/sec |

Broadly the same order of magnitude across the board — neither broker wins
decisively on raw single-node throughput here, which itself is a useful,
honest finding: **this comparison isn't really about which one is
"faster"** at this scale; both implementations handle batching the same
way from the client's perspective, so the client-side lever (`batch.size`)
dominates either way, exactly as Stage 13 already established.

### The one throughput number that's genuinely different: `acks=-1`

Stage 13 (real Kafka broker, RF=1) found `acks=-1` measurably *slower*
than `acks=1` (83,195 vs 94,518 msgs/sec) even with no real followers to
replicate to, attributed to ISR-bookkeeping overhead the broker still runs
regardless of ISR set size. **Redpanda showed no such gap**: `acks=1`
(87,413) and `acks=-1` (88,028) landed within noise of each other. Not
proof one architecture is "more correct" — but a real, measured difference
in how each implementation's replication/acknowledgment path is built
under the hood, surfaced by running the identical experiment twice rather
than assumed to transfer from one broker to the other.

### Operational feel: where the real gap actually is

| | Kafka (this project's broker) | Redpanda |
|---|---|---|
| Memory (idle-ish, post-load-test) | 1.065 GiB | 345.7 MiB (~3x less) |
| Process model | JVM, 110 OS threads/PIDs | native, 3 PIDs (thread-per-core) |
| Time from container start to "server started" | ~15.4s | ~0.65s (~23x faster) |

**Answering the roadmap's specific question directly**: this project's own
Kafka broker already runs in pure KRaft mode (no ZooKeeper at all, since
Stage 0) — and it still took over 15 seconds to start and used 3x
Redpanda's memory. **KRaft closed the *coordination-complexity* gap**
(no separate ZooKeeper ensemble to run and reason about, which was
historically Redpanda's sharpest argument) — **but it did not close the
resource-footprint or startup-time gap**, because that gap was never about
ZooKeeper vs. KRaft in the first place; it's JVM-with-garbage-collection
vs. a natively-compiled, thread-per-core C++ process. Two genuinely
different gaps, easy to conflate, only one of which KRaft actually
addressed.

### Key concepts introduced

- **Wire-protocol compatibility is real, not marketing** — every
  kafkajs producer/consumer/service written across 16 prior stages runs
  against Redpanda with a one-line connection-string change and zero
  logic changes, verified by actually running Stage 13's scripts
  unmodified rather than taking the compatibility claim on faith.
- **Raw single-node throughput is not where these two implementations
  meaningfully differ** — batch size dominates both, as already
  established in Stage 13; the interesting differences show up elsewhere.
- **`acks=-1`'s overhead-even-at-RF=1 finding from Stage 13 doesn't
  automatically transfer to a different broker implementation** — Redpanda
  showed no equivalent gap, a concrete reminder that broker-internal
  behavior (not just the wire protocol) can differ even when every
  client-visible API is identical.
- **KRaft and "single native binary" solve two different problems** —
  KRaft removes the operational burden of running a separate ZooKeeper
  ensemble; it doesn't touch the JVM-vs-native resource/startup gap, which
  is what actually produced the ~3x memory and ~23x startup-time
  differences measured here.
- **This project's memory ceiling is now a recurring, load-bearing
  constraint, not a one-off** — same lesson as Stage 16, hit again here:
  Redpanda was deliberately configured lightweight (`--smp=1 --memory=1G`)
  and run temporarily rather than left up permanently, to stay within this
  machine's real budget.

---

## Stage 18 — Distributed tracing with OpenTelemetry

**Date**: 2026-08-03

**Goal**: extend Stage 6's `orders → payments → inventory → shipping`
pipeline — four independent services, each its own process, connected only
by Kafka topics — with real distributed tracing, so one order's entire
journey shows up as a single connected trace instead of four disconnected
sets of `console.log` lines. Kafka has no built-in equivalent of an HTTP
request header for this: unlike an HTTP call chain, there's no framework
automatically forwarding a `traceparent` for you. It has to be propagated
by hand, through the Kafka message's own headers.

### Infrastructure: Jaeger, chosen deliberately for its footprint

Added `jaeger` (`jaegertracing/all-in-one`, OTLP HTTP receiver + query UI +
in-memory storage in one Go binary) to `docker-compose.yml`. Explicitly
picked for being the lightest realistic tracing backend available — this
project's memory ceiling has now bitten twice (Stages 16 and 17), and
Jaeger's actual footprint once running confirmed that was the right call:
**9.86 MiB**, next to nothing next to any of this project's JVM services.

### The actual mechanism: `traceparent` in, `traceparent` out

`src/shared/tracing.ts` is the real content of this stage — everything
else is wiring it into Stage 6's pattern:

- `injectTraceHeaders(ctx)` — turns the *current* span's context into a
  plain W3C `traceparent` string (via OpenTelemetry's
  `propagation.inject()`), which then gets attached as a literal Kafka
  message header (`{ headers: { traceparent: '00-<trace-id>-<span-id>-01' } }`)
  right next to the JSON business payload.
- `extractTraceContext(headers)` — the inverse, run by the *next* consumer
  in the chain: reads `traceparent` back out of the incoming message's
  headers and reconstructs an OpenTelemetry context from it, so a new span
  started here attaches as a **child** of the span that sent the message,
  instead of starting a disconnected trace of its own.

Built a parallel, traced version of Stage 6's four services in
`src/stage18-tracing/` (`order-producer.ts`,
`payment-service-traced.ts`, `inventory-service-traced.ts`,
`shipping-service-traced.ts` — separate consumer groups from Stage 6's
originals, e.g. `payment-service-traced`, so both versions can run
independently against the same topics) rather than editing Stage 6's
files directly — same reasoning as every other stage that extends earlier
work without disturbing its own already-documented lesson. Every service
does the same three things: extract the incoming context, start a child
span inside it, inject the *new* current context into whatever message it
produces downstream.

### First real result: 3 spans instead of 4 — the root span went missing

Ran all three traced services plus the traced producer, then queried
Jaeger's API directly for the resulting trace ID
(`GET /api/traces/<traceID>`) rather than eyeballing the UI, to get a
precise, checkable answer. First result: only **3 spans** —
`payment.process → inventory.reserve → shipping.create`, correctly chained
to each other, but the root `order.create` span from `order-producer.ts`
was simply missing.

**Root cause**: `BatchSpanProcessor` (the standard, recommended span
processor) buffers finished spans and exports them on a timer, or once the
buffer fills — completely fine for a long-running consumer service, which
stays alive long enough for that timer to fire naturally. `order-producer.ts`
is a short one-shot script: it sends its message, disconnects, and the
Node process exits — **before the batch processor's first scheduled export
ever ran**, silently dropping the one span that mattered most (the trace's
own root). Fixed with an explicit `shutdownTracing()` (calling
`provider.shutdown()`, which flushes synchronously) added right before the
script exits. Rerunning with a fresh order ID produced the correct, full
4-span trace:

```
order.create        (order-producer,     no parent)
  └─ payment.process    (payment-service,    parent = order.create)
       └─ inventory.reserve (inventory-service, parent = payment.process)
            └─ shipping.create  (shipping-service,  parent = inventory.reserve)
```

One order, four independently-running Node.js processes, one connected
trace — verified via Jaeger's own API (parent/child `spanID` references
checked directly, not just "the UI drew a nice waterfall"), not asserted.

### What this specifically proves that logs alone can't

Before this stage, chasing one order's actual path through Stage 6's
pipeline meant `grep`-ing four separate services' logs for a matching
`orderId` and manually reconstructing the order of events (exactly what
Stage 9's join investigation had to do, by hand, with raw timestamps). A
trace does that reconstruction automatically and durably: the parent/child
relationships stored in Jaeger reflect the true causal chain (this
payment happened *because of* this order; this shipment happened *because
of* this reservation), not just "these things happened at similar times,"
which is all a shared timestamp can actually prove.

### Key concepts introduced

- **Kafka propagates nothing about causality on its own** — a consumer has
  no inherent way to know which upstream event caused the message it's
  processing; that link only exists if something explicitly puts it in
  the message (here: `traceparent` in the headers) and something else
  reads it back out.
- **The W3C Trace Context format (`traceparent`) is transport-agnostic** —
  the exact same header format HTTP frameworks pass automatically works
  identically stuffed into a Kafka message header by hand; the wire
  format doesn't care what carried it.
- **`BatchSpanProcessor` can silently drop spans from short-lived
  processes** — it's built for long-running services and assumes there's
  time for its export timer to fire; a one-shot script needs an explicit
  flush/shutdown before exit, or its spans (root spans especially, from
  producers) may simply never arrive.
- **A trace's parent/child structure encodes real causality, not just
  temporal proximity** — the exact distinction Stage 9's join investigation
  had to painstakingly reconstruct from raw timestamps; tracing gets this
  for free, by construction, once the propagation plumbing is in place.
- **Picking infrastructure with this project's actual resource ceiling in
  mind is now a standing design constraint** — Jaeger was chosen
  specifically for being lightweight (9.86 MiB observed), not just for
  being the "standard" choice, a direct consequence of Stages 16 and 17's
  repeated OOM problems on this machine.

---

## Stage 19 — Prometheus + Grafana monitoring

**Date**: 2026-08-03

**Goal**: the piece Stage 11 deliberately left undone — Stage 11 checked
lag and ISR *by hand*, on demand, via CLI and Kafka UI. This stage builds
what a real production operator would actually run continuously: metrics
scraped on a schedule, stored as a time series, visualized on a dashboard
that stays useful after the terminal closes. Distinct from Stage 18's
OpenTelemetry work in exactly the way the roadmap framed it: Stage 18
traces **one order's** journey; this stage is **fleet-level** — lag,
throughput, and replication health across every topic and consumer group
at once, none of it tied to a single request.

### A deliberate deviation from the "obvious" JMX approach

The standard way to get Prometheus metrics out of Kafka is a JMX
Prometheus javaagent injected into the broker's JVM (`-javaagent:...`),
which means mounting a jar + YAML config into the `kafka` container and
adding JVM options. Used **`kafka-exporter`**
(`danielqsj/kafka-exporter`) instead — a small Go binary that talks the
plain Kafka protocol directly (`AdminClient`-style metadata + consumer
group describes), no JVM agent injection, no separate JMX config to
maintain, and — the actual deciding factor — it already exposes exactly
what this stage needs (`kafka_consumergroup_lag`,
`kafka_topic_partition_under_replicated_partition` for ISR health, and
per-partition offsets Grafana derives throughput from via `rate()`)
without touching raw broker JMX at all. A direct, reasoned trade-off
against the "standard" approach, made explicitly because of Stages 16 and
17's repeated real OOM problems on this machine — confirmed justified
after the fact: **kafka-exporter + Prometheus + Grafana together used
~157 MiB** (15.3 + 50.8 + 91.5 MiB measured), lighter than almost any
single JVM service already running in this stack.

### Everything provisioned as files, not clicked together

`src/stage19-monitoring/prometheus.yml` (scrape config),
`grafana/provisioning/datasources/prometheus.yml` (the Prometheus
datasource), `grafana/provisioning/dashboards/dashboards.yml` (tells
Grafana where to load dashboard JSON from), and
`grafana/dashboards/kafkaos-overview.json` (the actual dashboard: consumer
lag by group, under-replicated partitions by topic, throughput by topic,
brokers reporting) — all mounted into the containers read-only via
`docker-compose.yml`. Same reasoning as Stage 8's connector JSON files and
Stage 9's `statements.sql`: a dashboard clicked together by hand in the
Grafana UI leaves no trace in this repo and can't be reproduced by
`docker compose up`; this one can.

### Verified end-to-end with a real, known lag number — not just "the graph looks right"

Produced 6,000 messages to a fresh `monitoring-demo` topic (Stage 13's
load producer, `RATE=200` so Prometheus's 5s scrape interval would catch
several real data points), then deliberately created a consumer group that
read back only 2,000 of them (`kafka-console-consumer.sh --max-messages 2000`)
— leaving an exact, known, predictable lag of **4,000**.

Checked it at every layer instead of trusting the dashboard on faith:

```
kafka-exporter (curl :9308/metrics)  ->  raw kafka_consumergroup_lag present
Prometheus (direct query)            ->  sum(kafka_consumergroup_lag{consumergroup="monitoring-demo-slow"}) = 4000
Grafana (via its own datasource proxy, the literal code path a dashboard panel uses) -> same query = 4146
                                          (4000 ours + 146 pre-existing lag from other groups already in the cluster)
```

Every number accounted for, traced through every hop of the actual
pipeline (Kafka → kafka-exporter → Prometheus → Grafana) rather than
assumed correct because a panel rendered something. Also confirmed
`kafka_topic_partition_under_replicated_partition` reports `0` across the
board — correct and expected, since every topic in this project's main
cluster is RF=1 — a live version of the ISR health check Stage 11/12 did
by hand with `kafka-topics.sh --describe`, now running continuously and
queryable at any moment instead of on demand.

### Key concepts introduced

- **Fleet-level monitoring answers a different question than tracing** —
  "is anything, anywhere, falling behind or under-replicated right now"
  (this stage) vs. "what happened to this one specific message" (Stage
  18); production systems need both, and neither substitutes for the
  other.
- **You don't need JMX to get real Kafka operational metrics** —
  `kafka-exporter` gets consumer lag, ISR/under-replication, and offsets
  (throughput) directly over the Kafka protocol; JMX access only becomes
  necessary for metrics genuinely internal to the broker's JVM (GC pauses,
  request-handler thread pool saturation, etc.) that this stage didn't
  need.
- **A dashboard is only as trustworthy as its query path has been
  verified** — checked the same number (a deliberately engineered, known
  lag of 4,000) at the exporter, at Prometheus, and through Grafana's own
  proxy endpoint, rather than trusting a rendered graph on sight.
- **Provisioning-as-code applies to dashboards, not just infrastructure**
  — every panel, datasource, and scrape target lives in a file checked
  into this repo and reconstructed automatically by `docker compose up`,
  not manually recreated by clicking through a UI after every restart.
- **This stage's own tooling choice is itself evidence for the pattern
  Stages 16–18 established** — resource footprint is now a first-class
  selection criterion for this project's infrastructure, not an
  afterthought; the ~157 MiB combined footprint here is the direct payoff
  of treating it that way from the start instead of defaulting to the
  heaviest "standard" option.

---

## Stage 20 — Large payloads & the claim-check pattern (bonus)

**Date**: 2026-08-03

**Goal**: prove, not assert, why huge messages hurt — both throughput
degradation as messages grow, and a hard wall at some point, not just
"gets slower forever" — then implement the standard fix: store the
payload somewhere actually built for large blobs, and let Kafka carry
only a small reference to it.

### Proving the problem: throughput degrades, then a hard wall

Reused Stage 13's load producer against a fresh topic, escalating
`SIZE` with everything else held constant (`BATCH_SIZE=1`, to isolate
per-message payload cost from batching effects):

| Payload size | Throughput (msgs/sec) | Byte throughput |
|---|---|---|
| 1,000B | 1,242 msgs/sec | 1.18 MB/sec |
| 10,000B | 873 msgs/sec | 8.33 MB/sec |
| 100,000B | 415 msgs/sec | 39.57 MB/sec |
| 500,000B | 131 msgs/sec | 62.50 MB/sec |

Message-count throughput craters as expected (1,242 → 131, a ~9.5x drop)
— but byte throughput actually *keeps climbing* the whole time. Not a
contradiction: fewer, bigger messages still move more total bytes per
second, right up until per-message overhead and network/GC cost from
handling huge single payloads would eventually dominate. That point never
even had to be found, because something else stopped this test first:

```
KafkaJSProtocolError: The request included a message larger than the
max message size the server will accept
type: 'MESSAGE_TOO_LARGE', code: 10
```

At `SIZE=2,000,000` (2MB), the broker rejected the produce request
outright. Confirmed the exact real number instead of assuming Kafka's
textbook 1MB default: `kafka-configs.sh --describe` on this actual broker
reports `message.max.bytes=1048588`. **Not degraded throughput — a hard,
unconditional rejection.** No partial send, no truncation, no silent
data loss: the broker refuses to accept the message at all.

### The fix: claim-check, built against real MinIO (S3-compatible), not simulated

Added `minio` to `docker-compose.yml` — a real S3-compatible object store
(single Go binary, ~106 MiB observed, in the same weight class as every
other piece of infrastructure this project has favored since Stage 16's
memory problems started). `src/stage20-claim-check/blob-store.ts` wraps
the real AWS SDK (`@aws-sdk/client-s3`) pointed at MinIO's endpoint — the
exact same client code that would talk to real AWS S3 in production,
just with `forcePathStyle: true` and a local endpoint.

The pattern itself, in `claim-check-producer.ts`: generate the large
payload, upload it to the blob store, compute a SHA-256 checksum, then
send Kafka **only** a small JSON reference — `{ eventId, bucket, key,
sizeBytes, checksum, contentType }`. `claim-check-consumer.ts` is the
other half: read the small reference, fetch the actual blob from MinIO
only when it's actually needed, verify the checksum matches before
trusting it.

### Proof it actually works — including past where the naive approach couldn't even try

```
--- claim-check-producer: 20 attachments, 2,000,000B each ---
elapsed: 0.61s | 32.7 attachments/sec | avg Kafka message size: 281B

--- claim-check-producer: 5 attachments, 10,000,000B each ---
elapsed: 1.28s | 3.9 attachments/sec | avg Kafka message size: 282B
```

The second run used a payload **10x larger than the exact size that
produced a hard `MESSAGE_TOO_LARGE` rejection minutes earlier** — no
error, no special configuration, because Kafka never sees more than a
~280-byte reference regardless of whether the real attachment is 2MB or
10MB or larger. Verified the round-trip is actually correct, not just
"didn't crash": ran `claim-check-consumer.ts` against all 20 of the 2MB
attachments and confirmed **every single one** — `integrity OK`, meaning
the SHA-256 computed on download matched the checksum computed at upload
time, byte-for-byte, real content round-tripping through a real object
store. Confirmed independently via `mc du` on the MinIO bucket directly:
25 objects, 86 MiB total — matching 20×2MB + 5×10MB almost exactly.

### A real gap found live: no check that a message is actually a claim check

Asked, and checked directly instead of assuming: does anything stop a
message that *isn't* a claim-check reference from landing on
`attachment-events` and being trusted as one? Answer, proven live: no.

`claim-check-consumer.ts` originally did
`safeParseJson<AttachmentReference>(...)` — `<AttachmentReference>` reads
like a safety net, but it's purely a TypeScript compile-time annotation,
erased completely at runtime. `safeParseJson` only proves the message is
*valid JSON*; it says nothing about whether it's the *right shape*.

Proved this concretely by hand-producing a plain order JSON (Stage 2's
shape — `orderId`, `customerId`, `total`, no `key`/`bucket`/`checksum` at
all) onto `attachment-events`, then running the real consumer against it:

```
Error: No value provided for input HTTP label: Key.
```

`ref.key` was `undefined`, so the S3 SDK refused to even build the
download request. **Worse than a clean failure**: the log showed the
*identical* error repeated 5-6 times at the *same offset* — kafkajs's
default behavior on a throwing `eachMessage` handler is to retry the same
message indefinitely, not skip it. One wrong-shaped message **permanently
stalled the entire partition**. This is Stage 6's poison-pill problem
again, in a new disguise: not malformed JSON this time, perfectly valid
JSON, just the wrong shape for what this consumer assumes.

**Fixed with a real runtime type guard**, `isAttachmentReference()` —
checks every field the redeem step actually depends on (`eventId`,
`bucket`, `key`, `sizeBytes`, `checksum`, `contentType`) actually exists
with the right primitive type before trusting the message at all. A
message that fails the check is logged and skipped, exactly like
`safeParseJson` already does for unparseable JSON — treated as "not a
claim-check reference," not as "a broken one."

**Verified both outcomes in the same consumer run, back to back** —
the actual requirement: handle a message that satisfies the shape *and*
one that doesn't, without either one breaking the other:

```
[claim-check-consumer] a3638a3f-... -> fetched 3,000,000B from kafkaos-attachments/attachments/5b14d5ed-....bin -- integrity OK
[claim-check-consumer] skipping non-claim-check message at offset=8 (valid JSON, wrong shape — not a claim-check reference): {"eventType":"page_view","userId":"user-42","page":"/home"}
```

A real, valid 3MB attachment reference downloaded and verified correctly;
an unrelated, differently-shaped message immediately after it was
recognized as not a claim check and skipped cleanly — no crash, no
infinite retry, no stalled partition.

### Key concepts introduced

- **Kafka's message size limit is a hard rejection, not a soft
  degradation** — `message.max.bytes` (1,048,588B, confirmed on this
  actual broker) is a wall, not a slope; throughput getting worse as
  messages grow is a real, separate problem from this specific limit.
- **Byte throughput and message throughput tell different stories as
  payload size grows** — msgs/sec collapsed while MB/sec kept climbing;
  neither number alone is "the" throughput picture for large payloads.
- **The claim-check pattern moves the size problem to a system actually
  designed for it** — Kafka carries a small, constant-size reference
  regardless of the real payload's size; the blob store (S3/MinIO here)
  handles the part it's actually built for.
- **A claim check is only trustworthy if the reference includes enough
  to verify the redemption** — the checksum in the reference message is
  what turns "fetched *something* from that key" into "fetched exactly
  what was uploaded," proven here by actually computing and comparing
  SHA-256 on both ends, not just trusting a successful download.
- **The same AWS SDK code targets real S3 or a local MinIO with one
  config difference** (`forcePathStyle` + a local endpoint) — the same
  "wire-compatible, swap the endpoint" shape as Stage 17's Redpanda
  comparison, a recurring, genuinely useful pattern for building against
  local infrastructure that mirrors production without a rewrite later.
- **A generic type parameter is not a runtime check** —
  `safeParseJson<AttachmentReference>(...)` looks like it validates the
  shape; it only validates that the string was parseable JSON. A message
  of some *other* valid shape sails straight through unless something
  explicitly checks the fields the rest of the code actually depends on.
- **An unhandled throw inside `eachMessage` doesn't fail once — it stalls
  the partition forever** — kafkajs retries the same offset indefinitely
  on error by default; a single wrong-shaped message is enough to block
  every message behind it, proven live here exactly like Stage 6's
  poison pill, just triggered by valid JSON with the wrong shape instead
  of invalid JSON.

---

## Stage 21 — Idempotent consumers & deduplication

**Date**: 2026-08-03

**Goal**: opens Part 3 (Distributed Data Patterns). Rerun Stage 5's
at-least-once crash experiment *exactly* — same 8 seeded messages, same
crash point (after processing message 3, before committing) — but this
time with a real dedup layer, and prove the identical Kafka-level
duplicate delivery now produces **zero duplicate effect**, not just a
theoretical claim about what "should" happen.

### The dedup key: delivery identity, not business identity

`processed_events` is keyed by **`(topic, partition, offset)`** — not by
`order_id`. This distinction is the actual substance of this stage, not
a footnote: Stage 5's seed deliberately resends `order-1` and `order-2`
as legitimate new messages (an "update") at new offsets. Deduping by
`order_id` would have silently swallowed those as "already seen" —
correctness bug, not a safety feature. Deduping by delivery coordinates
catches only genuine Kafka *redelivery* (the exact same offset,
reprocessed after a crash) without collapsing legitimate repeat business
events that happen to share a business key.

`src/stage21-idempotent-consumer/schema.sql`: `processed_events`
(the dedup ledger, primary key `(topic, partition, "offset")`) and
`orders_processed` (the actual side effect — stands in for a real
downstream write, with an `effect_count` column that makes "did this
really run twice" directly observable instead of inferred).

### The mechanism: dedup check and side effect, same transaction

`idempotent-consumer.ts` keeps Stage 5's exact at-least-once ordering
(do the work, *then* commit the Kafka offset — same crash hazard, not
fixed, not fixable short of Stage 10's transactions) but wraps the "work"
step in one Postgres transaction: `INSERT` into `processed_events` first;
if that hits the primary key's unique constraint, `ROLLBACK` immediately
and skip the side effect entirely (return `"duplicate"`); if it succeeds,
perform the real side effect in the *same* transaction, then `COMMIT`
both together. No window exists where the dedup record exists without the
effect, or the effect exists without the dedup record.

### A real environment bug, found immediately trying to connect

First attempt at running this failed with a genuinely confusing error:
`role "kafkaos" does not exist` — despite `docker exec postgres psql -U
kafkaos` working fine all project long. Root cause, confirmed with
`lsof -nP -iTCP:5432`: **this Mac has a native Postgres installation
already listening on `127.0.0.1:5432`**, silently winning every
`localhost:5432` TCP connection over Docker's forwarded port. Every prior
stage's Postgres access went through `docker exec` (straight into the
container, no host networking involved at all), so this had been sitting
invisible the entire project — the first real host-side TCP client
(`pg.Pool`, here) hit an entirely different, real database server by
accident. Fixed by remapping the container's host port to `5433` in
`docker-compose.yml` — same category of problem as Stage 16's ksqlDB
advertised-listener bug, different specific cause: "the address you're
connecting to doesn't necessarily route to the container you think it
does" keeps recurring across this project in new forms.

### The crash, rerun exactly

```
[idempotent-consumer] order=order-1 offset=0 -> applied
[idempotent-consumer] order=order-2 offset=1 -> applied
[idempotent-consumer] order=order-3 offset=2 -> applied
[CRASH] handled offset=2 (applied), exiting BEFORE committing offset=3
```

Identical crash point to Stage 5. Restarted the same consumer group, no
code changes:

```
[idempotent-consumer] order=order-3 offset=2 -> duplicate   <- redelivered, exactly like Stage 5
[idempotent-consumer] order=order-4 offset=3 -> applied
[idempotent-consumer] order=order-5 offset=4 -> applied
[idempotent-consumer] order=order-6 offset=5 -> applied
[idempotent-consumer] order=order-1 offset=6 -> applied     <- the legitimate resend, correctly NOT flagged
[idempotent-consumer] order=order-2 offset=7 -> applied     <- same
```

`order-3` at offset 2 was redelivered by Kafka exactly like Stage 5 (the
offset was never committed before the crash) — but this time the dedup
layer caught it explicitly: `"duplicate"`, not a second `"applied"`.

### Proof, straight from the database, not just the console log

```sql
SELECT order_id, status, effect_count FROM orders_processed ORDER BY order_id;
--  order_id | effect_count
--  order-1  |      2   <- two REAL business events (created, then updated) — correctly not deduped
--  order-2  |      2   <- same
--  order-3  |      1   <- delivered twice by Kafka, effect ran exactly once
--  order-4  |      1
--  order-5  |      1
--  order-6  |      1

SELECT count(*) FROM processed_events;  -->  8
-- exactly one row per unique (topic, partition, offset) — including
-- offset 2, despite that offset being delivered to eachMessage twice.
```

This is the precise, provable version of Stage 5's finding turned inside
out: Stage 5 showed at-least-once *causes* a duplicate effect (`order-3`
appeared twice in the log). This stage shows the *exact same Kafka-level
redelivery* — same crash point, same offset, same "message 3 gets
reprocessed" — now produces **one** effect, not two, while still
correctly applying **two** effects for `order-1`/`order-2`, which
legitimately needed two.

### Key concepts introduced

- **Idempotent-consumer dedup keys on delivery identity
  (topic+partition+offset), not business identity** — the specific
  design choice that makes Stage 5's legitimate order-1/order-2 resend
  behave correctly (applied twice) while true redelivery of order-3
  behaves correctly (applied once). Keying on `order_id` instead would
  have silently broken the legitimate case.
- **The dedup check and the side effect must be atomic together** — same
  transaction, same commit — or a crash between them reopens exactly the
  gap this pattern exists to close (recorded as processed, but the effect
  never happened, or vice versa).
- **This pattern does not remove the at-least-once crash hazard** — the
  exact same crash-before-commit window from Stage 5 still exists and
  still causes redelivery; what changed is entirely what redelivery
  *does* once it happens, not whether it can happen.
- **A host-side TCP client can silently hit the wrong server entirely**
  — this Mac's native Postgres on port 5432 had been invisible all
  project long purely because every prior access method (`docker exec`)
  never touched host networking; the first real TCP client from outside
  Docker exposed it immediately, with a confusing but ultimately correct
  error (`role "kafkaos" does not exist`) once traced to its real cause.
- **Proof belongs in the system of record, not just the console log** —
  verified this stage's actual claim (one effect, not two) with a direct
  SQL query against `orders_processed`/`processed_events`, not by trusting
  the printed `"applied"`/`"duplicate"` lines alone.

---

## Stage 22 — Retry policies & DLQ with alerting

**Date**: 2026-08-03

**Goal**: a real application-level resilience pattern for a downstream
failure — retry with backoff a bounded number of times, and only after
exhausting retries, route to a dead-letter topic **and fire a real,
metrics-driven alert**, not just log it the way Stage 8's Connect-level
DLQ did for malformed messages. Deliberately built right after Stage 21:
retrying is only safe to combine with Kafka's at-least-once redelivery
because the consumer is already idempotent — otherwise a retry is just a
slower way to reproduce Stage 5's duplicate-effect problem.

### The simulated failure, made deterministic on purpose

`flaky-downstream.ts` stands in for an unreliable dependency (a payment
gateway, a third-party API) — but fails by **order ID convention**, not
randomness: `order-transient-fail-N` fails the first `N-1` calls then
succeeds; `order-permanent-fail` always fails. Deterministic on purpose —
a real, reproducible experiment beats a flaky one that might not fail the
same way twice.

### Two failure modes, three seeded orders, one consumer

`resilient-payment-service.ts` retries each order up to `MAX_ATTEMPTS=3`
with exponential backoff (200ms, 400ms), reusing **Stage 21's exact
idempotent-write pattern verbatim** (`processed_events` dedup check +
side effect, same transaction) for the success path — direct, working
proof the two stages' patterns compose rather than just sitting next to
each other in the roadmap:

```
order-normal-1          -> succeeded on attempt 1/3
order-transient-fail-3  -> attempt 1/3 failed, retrying in 200ms
                         -> attempt 2/3 failed, retrying in 400ms
                         -> succeeded on attempt 3/3
order-permanent-fail-1  -> attempt 1/3 failed, retrying in 200ms
                         -> attempt 2/3 failed, retrying in 400ms
                         -> EXHAUSTED 3 attempts, routed to payments-dlq
```

Verified in the database, not just the log: `payments_processed` holds
exactly `order-normal-1` (attempts=1) and `order-transient-fail-3`
(attempts=3) — `order-permanent-fail-1` correctly absent, since it never
actually succeeded. The DLQ message itself carries full failure context,
not just the original payload:

```json
{"originalTopic":"orders-resilience-demo","originalPartition":0,"originalOffset":"2",
 "order":{"orderId":"order-permanent-fail-1", ...},
 "failureReason":"downstream rejected order-permanent-fail-1 (attempt 3) — simulated permanent outage",
 "attempts":3,"failedAt":"2026-08-03T14:02:25.525Z"}
```

Enough to actually investigate and replay the failure later — the DLQ
topic is a real audit trail, not just a bucket of orphaned payloads.

### Wiring a real alert, not a log line — and a real Prometheus tuning lesson hit along the way

Added `alertmanager` to `docker-compose.yml`, a `/metrics` endpoint
(`prom-client`) on the resilient service exposing `payment_dlq_messages_total`,
a Prometheus scrape target, and an alerting rule
(`increase(payment_dlq_messages_total[...]) > 0`). The service runs on
the **host** (`ts-node`, not a container) — scraped via
`host.docker.internal:9301`, the Docker Desktop mechanism built for
exactly this direction, the same category of cross-boundary addressing
problem as Stage 16's advertised listeners and Stage 21's port collision,
recurring again in a new specific form.

First attempt: the alert briefly went `pending`, and by the time it was
checked again it had already reverted to `inactive` — **not a bug, a real
tuning miss**. `evaluation_interval` was never set, defaulting to a full
minute, and the rule's `increase(...[2m])` lookback was barely wider than
that — for a single, one-off metric increment (not sustained traffic),
the window in which the rule could even see the spike and the window in
which `for: 15s` had to elapse barely overlapped, so the alert fired and
expired between polls. Fixed by actually understanding the mechanism, not
guessing: `evaluation_interval: 5s` (matching `scrape_interval`) and
widening the lookback to `increase(...[5m])` so a one-off DLQ event stays
observably `firing` for minutes, not seconds. Retriggered a fresh DLQ
event and this time caught it cleanly at every layer:

```
Prometheus:    GET /api/v1/alerts    -> state: "firing"
Alertmanager:  GET /api/v2/alerts    -> status.state: "active", receiver: "default"
```

A genuine alert, evaluated by a real rules engine and received by a real
Alertmanager instance — not a Slack message (no real webhook configured
in a local dev repo), but the actual alerting *mechanism* proven correct
end-to-end, which is the part Stage 8's plain-log DLQ never had at all.

### Key concepts introduced

- **Retry-with-backoff and idempotent writes are complementary, not
  redundant** — retry handles a downstream dependency's *own* transient
  failures; the Stage 21 dedup layer handles *this consumer's own*
  crash-and-redeliver hazard. Removing either one reopens a different
  failure mode.
- **A DLQ message should carry enough context to investigate and replay
  later** — the original payload alone isn't enough; failure reason,
  attempt count, and original topic/partition/offset turn a DLQ into an
  audit trail instead of an orphan bin.
- **A metric that only fires once is easy to miss with infrequent
  evaluation** — `evaluation_interval` defaults to 1 minute if unset, and
  a `for` duration plus an `increase()` lookback both need to be sized
  relative to that, or a genuinely correct alert rule can fire and expire
  invisibly between checks. Confirmed by hitting it for real, not by
  reading the docs.
- **A real alert means verifiable state in a real alerting engine, not a
  log line** — checked via Prometheus's own `/api/v1/alerts` (rule
  evaluation state) and Alertmanager's own `/api/v2/alerts` (received,
  active alert), the same "verify at every hop, not just the end" method
  Stage 19 established for its dashboard's query path.
- **`host.docker.internal` is the general answer for "container needs to
  reach a process running on the host"** — the same shape of problem as
  every other cross-boundary addressing issue this project has hit
  (Stage 16, Stage 21), here solved with the mechanism specifically built
  for this exact direction.

---

## Stage 23 — Transactional Outbox Pattern

**Date**: 2026-08-03

**Goal**: solve the dual-write problem — a service that needs to update
its own database *and* reliably publish a Kafka event as one atomic
unit, when Postgres and Kafka are two completely separate systems with no
shared transaction between them. Proved the naive approach loses events
for real, then proved the outbox pattern (with Debezium doing the actual
publishing via CDC) survives the identical crash.

### Part A: proving the naive approach really loses data

`naive-order-service.ts` does the textbook-wrong thing: commit the order
to Postgres, *then* separately call `producer.send()` to Kafka. Crashed
it right after the DB commit for the second of three orders:

```
[naive-order-service] DB commit: order-naive-1
[naive-order-service] Kafka publish: order-naive-1
[naive-order-service] DB commit: order-naive-2
[CRASH] DB already committed for order-naive-2, exiting BEFORE publishing to Kafka
```

Checked both systems directly afterward — not assumed:

```sql
SELECT order_id FROM orders_naive;  -->  order-naive-1, order-naive-2   (both, permanently)
```
```
kafka-console-consumer --topic orders-naive-events  -->  only order-naive-1
```

`order-naive-2` exists in Postgres **forever** — a real business record —
but its Kafka event is gone, and nothing will ever retry it: the crashed
process has no memory of the gap, and nothing else in the system knows
one exists. This is the dual-write problem made concrete, not
hypothetical.

### Part B: the fix — write together, publish independently

`outbox-order-service.ts` writes the business row (`orders_outbox_demo`)
and an event row (`outbox`, column names matching Debezium's
outbox-event-router defaults exactly — `aggregatetype`, `aggregateid`,
`type`, `payload`) in the **same Postgres transaction**. Nothing about
*publishing to Kafka* happens in this process at all anymore — there's no
code path left that can commit the DB write and forget to publish,
because publishing was never this process's job to begin with.

Debezium (`io.debezium.connector.postgresql.PostgresConnector`, installed
into Kafka Connect) reads Postgres's write-ahead log directly via logical
replication (`pgoutput`, built into Postgres core — no extra extension)
and republishes every new `outbox` row as a clean Kafka event via the
`EventRouter` SMT, entirely independently of whatever the application
process is doing or whether it's even still running.

### Two real environment problems before it worked

1. **`wal_level=logical` isn't the default** — Postgres defaults to
   `replica` (replication for failover, no logical decoding). Required an
   explicit `command: ["postgres", "-c", "wal_level=logical"]` and a real
   container restart, not just a config-table change.
2. **Debezium 3.x (`:latest` on Confluent Hub) doesn't run on this Connect
   image's JVM**: `UnsupportedClassVersionError: ... class file version
   61.0, this version of the Java Runtime only recognizes class file
   versions up to 55.0` — Debezium 3.x targets Java 17; `cp-kafka-connect-base:7.6.1`
   ships Java 11. Confirmed via `docker exec kafka-connect java -version`,
   then fixed by pinning `debezium/debezium-connector-postgresql:2.5.4`
   (the newest version still built for Java 11) instead of `:latest`.
   Also re-learned a Stage 15 lesson the hard way: `docker compose
   restart` does **not** re-read `docker-compose.yml` changes (command,
   volumes) — only `docker compose up -d` triggers a real recreate;
   `restart` just restarts the already-existing container with its
   already-existing config.

### The same crash, rerun through the fixed pattern

```
[outbox-order-service] committed order + outbox row: order-outbox-1
[outbox-order-service] committed order + outbox row: order-outbox-2
[CRASH] DB transaction already committed for order-outbox-2 (order row + outbox row, together) — exiting now
```

Identical crash point to Part A: crash immediately after the second
order's DB work commits, before anything else happens. Checked the
resulting Kafka topic (Debezium named it `outbox.event.order`, not
something guessed in advance — checked directly with `kafka-topics.sh
--list` rather than assumed):

```
order-outbox-1  {"type":"OrderCreated","order":{...orderId":"order-outbox-1"...}}
order-outbox-2  {"type":"OrderCreated","order":{...orderId":"order-outbox-2"...}}
```

**Both events arrived — including `order-outbox-2`, whose application
process crashed at the exact same point `order-naive-2`'s did.** The
difference isn't luck or timing; it's structural: Debezium had already
seen the committed transaction in the WAL before the crash even happened,
and publishing from there doesn't depend on the application process
being alive at all.

### Key concepts introduced

- **The dual-write problem is real and provable, not theoretical** — two
  independent systems, no shared transaction, a crash at the seam loses
  data permanently with no way to even detect the loss afterward. Proved
  it by actually losing an event, not by describing the risk.
- **The outbox pattern's actual mechanism is decoupling *who* publishes
  from *the application process*** — Debezium reads committed WAL data
  independently; the crash that would have lost the naive publish has
  nothing left to interrupt, because the publish was never this
  process's responsibility to begin with.
- **Debezium's outbox-event-router SMT expects specific default column
  names** (`aggregatetype`, `aggregateid`, `type`, `payload`) — matching
  them meant zero extra SMT configuration; a real, meaningful convention,
  not a starter-tutorial simplification.
- **JVM class file version mismatches are a real, checkable failure
  mode for Kafka Connect plugins** — `UnsupportedClassVersionError` names
  the exact class file version gap; cross-referencing it against the
  Connect image's actual `java -version` turned a mysterious "plugin just
  doesn't show up" into an exact, fixable version constraint.
- **`docker compose restart` vs. `docker compose up -d` are not
  interchangeable** — `restart` reuses the existing container's already-
  materialized config; only `up -d` reads the compose file again and
  recreates the container if anything relevant changed. The same-shaped
  gap as Stage 7's `kafka-ui`/Schema Registry finding ("editing
  `docker-compose.yml` does not retroactively apply to already-running
  containers"), just hit again here from the `restart` side instead of
  the "forgot to touch that service at all" side.

---

## Stage 24 — Saga Pattern

**Date**: 2026-08-03

**Goal**: Stage 6's `orders → payments → inventory → shipping` chain has
always technically been a choreography-style saga — each service reacts
independently to the previous one's events — but it was missing the half
that makes it a *real* saga: compensation. `inventory-service` just
skipped a failed payment and moved on; it never had a way to *reverse* a
payment that had already succeeded when the next step failed. Built that
missing compensating path, proved both the happy path and the
compensation path work end-to-end, then built the same saga a second way
— orchestration — to make the actual trade-off between the two concrete
instead of just described.

### The case Stage 6 never modeled

Three deterministic scenarios, same convention as Stages 22/23:
`order-happy-*` (everything succeeds), `order-payment-declined-*`
(payment fails outright — Stage 6's existing case), and
`order-out-of-stock-*` (**payment succeeds, but inventory reservation
fails afterward** — the case this stage actually adds).

### Choreography: each service reacts, no one has the full picture

`payment-service-saga.ts` now subscribes to **two** independent topics —
`orders-saga` (process new orders) and a new `payment-compensation-saga`
(react to refund requests) — with no relationship between the two beyond
both mattering to this service. `inventory-service-saga.ts` is the other
half: on a reservation failure, it doesn't call payment-service or wait
for anything; it just publishes a compensation request to
`payment-compensation-saga` and moves on, trusting something is
listening. Neither service has (or needs) a global view of the saga.

Ran all three scenarios together against the three running services:

```
payment-service-saga:    order-happy-1 -> payment succeeded
                          order-out-of-stock-1 -> payment succeeded
                          order-payment-declined-1 -> payment failed
                          order-out-of-stock-1 -> REFUNDED (reason: inventory reservation failed: out of stock)

inventory-service-saga:  order-happy-1 -> reserved
                          order-payment-declined-1 skipped (payment failed)
                          order-out-of-stock-1 -> RESERVATION FAILED, requesting compensation
                          order-out-of-stock-1 skipped (payment refunded)   <- the refund event, correctly not re-processed

shipping-service-saga:   order-happy-1 -> shipped, tracking=TRK-order-happy-1-...
```

The full compensating cycle, working exactly as designed:
payment succeeds → inventory reservation fails → compensation request
published → payment-service reacts independently and issues a refund →
that refund event flows back through `payments-saga` → inventory-service
sees it and correctly does **not** re-attempt reservation (an explicit
status check — `payment.status !== "succeeded"` — is what prevents an
infinite compensation loop; refund events share the same topic as
original payment events, and without that check the refund would look
like a second successful payment to react to). `order-out-of-stock-1`
never reaches shipping, correctly — its saga ended in a real, reversed
state, not a half-finished one.

### Orchestration: the exact same saga, one process, one linear function

`order-saga-orchestrator.ts` implements the identical business logic —
same three scenarios, same compensation decision — but as a single
`runSaga()` function that directly, sequentially calls each step and
explicitly decides when to compensate, publishing an audit event at every
step to one topic (`saga-orchestrator-audit`) instead of driving behavior
through topic subscriptions at all:

```
order=order-out-of-stock-1 step=payment      { result: 'succeeded' }
order=order-out-of-stock-1 step=inventory    { result: 'out-of-stock' }
order=order-out-of-stock-1 step=compensate-payment { reason: 'inventory reservation failed: out of stock' }
order=order-out-of-stock-1 step=saga-ended   { outcome: 'compensated: payment refunded after inventory failure' }
```

Identical outcome to the choreography version, for all three scenarios.
**The difference is entirely legibility and control, not correctness**:
this saga's entire definition — every step, every branch, every
compensation — is one readable function in one file. Confirmed the audit
topic actually captures this: a single `kafka-console-consumer` read
against `saga-orchestrator-audit`, filtered to one `orderId`, reconstructs
the complete, ordered story of that one saga — no correlation across
multiple topics required, unlike the choreography version, where the same
story only exists implicitly, split across `payments-saga`,
`payment-compensation-saga`, and each service's own independent log
output.

### The actual trade-off, from having built both

- **Choreography**: no central point of failure or bottleneck, each
  service is independently deployable and only needs to know about the
  events it cares about — but the *saga's logic itself* isn't written
  down anywhere as a single artifact. Understanding "what happens when
  inventory fails" means reading `inventory-service-saga.ts` **and**
  `payment-service-saga.ts` and mentally joining them by topic name.
- **Orchestration**: the saga's full logic, happy path and every
  compensation, is one artifact, trivially auditable and testable as a
  unit — but the orchestrator now knows about every step's business
  rules directly (payment logic, inventory logic), and every step funnels
  through one process, which is both a bottleneck and a single point of
  saga-level failure that choreography doesn't have.

Neither is a strictly better default — matches this project's Stage 15
finding about Kafka Engine vs. Connect sink (two paths converging on the
same correct outcome, genuinely different operationally) more than it
resembles a right-answer-vs-wrong-answer comparison.

### Key concepts introduced

- **A saga isn't complete without its compensating paths** — Stage 6's
  chain looked like a saga but wasn't one until failure could be
  *reversed*, not just stopped partway through; "skip and move on" and
  "compensate what already happened" are different guarantees.
- **Choreography requires an explicit guard against reacting to your own
  compensation** — refund events and original payment events shared one
  topic here; without checking `status`, inventory-service would have
  treated a refund as a new successful payment and looped the
  compensation forever.
- **Orchestration makes the saga's definition a single, readable
  artifact; choreography makes it an emergent property of several
  services' independent subscriptions** — proved concretely by comparing
  one topic's worth of ordered audit trail against needing to correlate
  three separate topics' worth of independent logs for the same story.
- **Same business outcome, two structurally different systems** — both
  patterns produced byte-for-byte identical results for all three test
  scenarios; the real difference this stage surfaced was operational
  (where the logic lives, what the failure/bottleneck shape looks like),
  not correctness.

---

## Stage 25 — The Capstone: Orderweave

**Date**: 2026-08-04

**Goal**: Stages 21–24 each proved one reliability pattern in isolation,
on its own dedicated demo topics, never combined with each other and
never wired into the real `orders → payments → inventory → shipping`
business pipeline from Stage 6. This stage closes that gap: assemble
idempotency (21), retry/DLQ (22), the transactional outbox (23), and saga
compensation (24) into one coherent, named system — **Orderweave** — wire
Stages 18/19's tracing and monitoring around it for real, then prove it
at the scale the whole project has been building toward: millions of
messages, a real mid-burst crash, and a real analytics sink, with every
number in this section coming from an actually-executed command, not an
estimate or a number carried over from an earlier stage's smaller test.

### Assembly, not new patterns

`src/stage25-capstone/` combines, adapted rather than cross-imported (this
project's established convention — only `src/shared/` is genuinely
shared):

- **`order-service.ts`** — Stage 23's outbox dual-write (one Postgres
  transaction: `orders` row + `capstone_outbox` row), Stage 18's root-span
  tracing via Phase 1's new payload-embedding helper (`traceContext`
  injected into the outbox JSON payload, not Kafka headers — Debezium,
  not this process, produces the eventual Kafka message, so there are no
  headers of ours to set). Deterministic scenario IDs matching Stage
  22/24's convention (`order-happy-*`, `*-payment-declined-*`,
  `*-out-of-stock-*`, `*-transient-fail-N-*`, `*-permanent-fail-*`) so one
  seed run exercises every real path.
- **`payment-service.ts`** — idempotent consumption (Stage 21:
  `processed_events`, same transaction as the side effect) + retry with
  exponential backoff and a DLQ (Stage 22, scoped to this service only —
  it's the only step with a genuine transient-infrastructure failure
  mode; inventory's out-of-stock is a business decision, not a transient
  fault) + outbox (Stage 23) + compensation consumption (Stage 24, on a
  *separate* `payment-compensation` topic, avoiding the self-referential
  "don't reprocess your own refund" guard Stage 24's choreography demo
  needed).
- **`inventory-service.ts`** / **`shipping-service.ts`** — same
  idempotent+outbox+tracing shape; inventory additionally triggers a
  compensation request on out-of-stock (Stage 24).
- **Schema** (`schema.sql`): `processed_events` reused byte-for-byte from
  Stage 21 (now the *third* stage to reuse it unmodified — real evidence
  it's general infrastructure, not a one-stage throwaway). `capstone_outbox`
  is a **separate** table from Stage 23's `outbox`, deliberately — one
  shared table watched by two independent Debezium connectors would have
  cross-contaminated both stages' demo data.
- **Metrics** (`metrics.ts`): `orderweave_events_processed_total{service,
  outcome}` (`applied`/`duplicate` — the direct, queryable idempotency
  proof), `orderweave_retry_attempts_total`, `orderweave_dlq_messages_total`,
  `orderweave_compensations_total`.
- **Observability**: a new Grafana dashboard
  (`stage19-monitoring/grafana/dashboards/orderweave-overview.json` — 5
  panels: pipeline funnel, compensation rate, DLQ messages, duplicate-vs-
  applied outcome ratio, consumer lag by group) and a new alert rule
  (`orderweave_alerts.yml`: `OrderweaveConsumerLagHigh`, lag > 20 for 10s
  — same `for:`-duration lesson Stage 22 learned the hard way), both
  additive to Stage 19/22's existing files, not replacing them.

### Two real failures worked through in Phase 1–4 assembly

1. **Connector name collision**: applying `outbox-postgres-source.json`
   under the same name as Stage 23's live connector silently overwrote
   Stage 23's config via the same PUT endpoint. Fixed by renaming to
   `orderweave-outbox-postgres-source.json` and restoring Stage 23's
   original from its own `apply-connectors.sh` — confirmed both connectors
   now run independently, watching different tables.
2. **Debezium double-JSON-encoding**: the `capstone_outbox.payload`
   column is JSONB, written via our own `JSON.stringify()` — so Postgres
   stores a JSON *string*, not a native JSON value. Debezium's
   `EventRouter` SMT then treats that column as a Connect STRING, and
   `JsonConverter` (schemas.enable=false) JSON-encodes a STRING value by
   quoting it — so the real Kafka message value is a JSON string literal
   containing another JSON string, confirmed directly via byte inspection.
   Every consumer needs `parseOutboxMessage()` (`outbox-message.ts`) to
   `JSON.parse()` twice, not once. This exact quirk resurfaces in Phase 6
   below.

### Phase 4: kill `payment-service` mid-burst

With all four services and the full monitoring stack running against a
continuous order burst, `kill -9`'d the live `payment-service` process
(the real PID, located via `ps -p <pid> -o pid,command` — the wrapper PID
from a backgrounded `npm run` is npm itself, not the actual `ts-node`
process, a lesson this project has now hit and re-solved several times).
Captured, via direct API/SQL evidence rather than assertion: the
`payment-service` consumer group's lag climbing in Grafana while `orders`
kept flowing uninterrupted (the topic-as-buffer property Stage 6 first
demonstrated, now visible at fleet scale); `OrderweaveConsumerLagHigh`
transitioning to `firing` in both Prometheus's and Alertmanager's own
APIs (the same two-hop verification Stage 22 established); on restart,
the redelivered in-flight message's outcome recorded as `"duplicate"` in
`processed_events`, with **exactly one** row in `payments` for that order
despite being processed twice; and the lag panel draining back to zero
with the alert resolving.

### Phase 5: the millions-of-messages load test

**The real, measured problem, not a hypothetical one**: strict one-
Postgres-transaction-per-message throughput — the same granularity that
makes the idempotency proof above easy to reason about one message at a
time — measured at roughly 4–5 messages/sec once real per-message
transactions were in the critical path (two independent rehearsal
measurements agreed on this order of magnitude). At that rate, 2,000,000
messages would take **over four days**. Nowhere near Stage 13's raw-Kafka
74–108K msg/s, and that gap is the finding, not something to hide.

**The fix, directly citing Stage 13's own conclusion that batching is the
dominant Kafka throughput lever** — it turns out to apply just as much to
the Postgres side of a pipeline as to the producer side. Built two
load-test-only variants (the strict, one-transaction-per-message services
above are untouched — they're what makes Phases 2–4's demos easy to
reason about, and stay exactly as built):

- **`order-producer-batched.ts`** — accumulates N `orders` (+
  `capstone_outbox`) rows and commits them via one `UNNEST`-based
  multi-row `INSERT` per batch, instead of one transaction per order.
- **`payment-service-batched.ts`** — consumes via `eachBatch` instead of
  `eachMessage`; one Postgres statement per batch does the whole
  idempotent-outbox write as chained data-modifying CTEs (dedup-insert
  into `processed_events` with `ON CONFLICT DO NOTHING`, then only the
  rows that were genuinely new flow into a `payments` upsert, then into
  `capstone_outbox`) — the batch equivalent of the strict version's
  "processed_events + business row + outbox row, one transaction" shape.
  Deliberately scoped to `SCENARIO=happy` traffic only (flaky-downstream's
  failure modes are keyed off order-ID substrings this mode never
  generates, so retry logic isn't needed here) and to the `orders` topic
  only (no compensation traffic exists in an all-happy-path run) — a
  documented scope narrowing, not an oversight. One real subtlety caught
  and fixed here: kafkajs's `commitOffsetsIfNecessary()` throttles
  internally, which would leave the *reported* Kafka lag trailing behind
  what was actually committed to Postgres — switched to an explicit
  `commitOffsets()` every batch so the lag-based catch-up measurement
  below is trustworthy.

Verified correct at small scale first (5,000 messages: exact 1:1 row
counts, zero duplicates) before trusting the real run.

**The real run**, against a freshly truncated Postgres and consumer
group offsets reset past the old backlog:

```
[order-producer-batched] DONE: 2000000 orders in 85.0s (23532 orders/sec avg)
```

Consumer catch-up, measured via `kafka-consumer-groups.sh --describe`
against the real `payment-service` group (not estimated): fully drained
to **zero lag on all three partitions** within **181 seconds total**
wall-clock from the start of production — the batched consumer was
already ~68% caught up (1,366,566 of 2,000,000) by the time the producer
itself finished, i.e. consumption and production overlapped almost the
whole way through, not a sequential produce-then-drain.

Correctness at 2,000,000-message scale, queried directly, not asserted:

```sql
SELECT (SELECT count(*) FROM payments)         -- 2000000
     , (SELECT count(*) FROM processed_events) -- 2000000
     , (SELECT count(*) FROM capstone_outbox); -- 4000000  (2M OrderCreated + 2M PaymentSucceeded)
```

Exact 1:1 counts — zero duplicates, zero lost messages, despite however
many internal batch/fetch cycles a run this size naturally involves. The
batching itself: only **132** Postgres transactions committed all 2
million messages (average batch size ≈15,151; min 3, max 17,086) — a
striking, concrete number for "how much does batching actually save."
`kafka-connect`'s memory grew to ~2.4GiB relaying the CDC load (total
stack ~6.1GiB, still under the documented ~7.661GiB Docker ceiling but a
real, worth-noting cost of the outbox pattern at this volume).

### Phase 6: ClickHouse sink — three more real failures

Plan originally called for copying Stage 15's Kafka Connect sink
connector config. Deviated deliberately: the Connect sink's default
insert path expects one JSON *object* per Kafka record, and — same
double-JSON-encoding quirk as above — `orders`/`payments`' message values
are JSON *strings*, not objects. Reused Stage 15's *other* proven
mechanism instead (Kafka engine table → Materialized View → MergeTree),
picking the one that actually fits this pipeline's data shape rather than
forcing the Connect sink to. Three real failures, same evidentiary
standard as Stage 15's own three:

1. `kafka_auto_offset_reset` is rejected as an unknown per-table
   `SETTINGS` key on this ClickHouse version (24.8.14) — that knob lives
   in server-side `config.xml`, not `CREATE TABLE`. Worked around by
   pre-seeding each consumer group's committed offset via
   `kafka-consumer-groups.sh --execute` before creating the tables.
2. `kafka_format = 'JSONAsString'` — despite the name — actually requires
   the top-level JSON value to be an **object** (`Code: 117: JSON object
   must begin with '{'`). Every message was being silently swallowed as
   "broken" by `kafka_skip_broken_messages`; removing that setting to let
   the real exception surface is what revealed the true cause. Our wire
   bytes are a JSON *string* at the top level, not an object, so
   `JSONAsString` rejected literally every message.
3. Switching to `kafka_format = 'RawBLOB'` (verbatim bytes, zero
   validation) fixed the parsing, but the container was **OOM-killed
   twice** backfilling the ~4.1M pre-existing messages across both topics
   through a Kafka engine table with an attached materialized view doing
   the JSON-unwrap **inline**, even with ample free headroom (~4.3GiB) at
   crash time — the live per-row `JSONExtract`/`concat` transform under a
   multi-million-row streaming backfill is genuinely heavier than a
   straight byte copy (and the *first* crash, still on `JSONAsString`,
   was almost certainly the ~4.1M per-message C++ exception objects being
   generated and swallowed, not the raw volume itself). Fixed by
   splitting into two phases — `clickhouse-schema.sql` ingests raw bytes
   only (cheap, stable, confirmed via direct monitoring: memory stayed
   under 1.1GiB backfilling all ~4.1M rows), then `clickhouse-parse.sql`
   runs the JSON extraction as a single, one-time, controllable batch
   `INSERT ... SELECT` afterward (stable at ~1.4GiB peak) — decoupling
   "get bytes in safely" from "parse them," instead of doing both live
   during the highest-volume phase.

Verified independently of Postgres — a third, separate confirmation of
Phase 5's exact numbers:

```sql
SELECT count() FROM orderweave_orders   WHERE order_id LIKE 'order-happy-loadtest-<run-tag>-%';   -- 2000000
SELECT count() FROM orderweave_payments WHERE order_id LIKE 'order-happy-loadtest-<run-tag>-%';   -- 2000000
```

One real analytical query, timed via `clickhouse-client --time` (not a
stopwatch):

```sql
SELECT status, count(), sum(amount) FROM orderweave_payments
WHERE order_id LIKE 'order-happy-loadtest-<run-tag>-%' GROUP BY status;
-- succeeded | 2000000 | 50000000        -- 0.045s
```

2,062,586 orders / 2,040,187 payments landed in total (the small gap from
2,000,000 is genuinely old Stage 6-era messages sharing these same topic
names — the pre-outbox flat JSON shape fails the unwrap gracefully to an
empty `order_id`, filtered out rather than silently corrupting real
rows). Storage: ~10.5MB / ~10.2MB on disk for 2M+ rows each — far smaller
than Stage 15's 103MB/2M rows, since this schema has fewer, narrower
columns.

### Key concepts introduced

- **Assembly is its own kind of work, not a formality** — combining four
  already-individually-correct patterns into one system surfaced two new
  integration-specific failures (the connector name collision, the
  double-JSON-encoding quirk) that none of Stages 21–24 hit in isolation,
  because none of them shared a table, a topic namespace, or a Debezium
  connector with anything else.
- **The granularity that makes a proof easy to read is often not the
  granularity that scales** — one-transaction-per-message is the clearest
  possible idempotency demonstration and also, measured directly, over
  400,000x too slow for a 2-million-message target; batching the exact
  same guarantees (dedup ledger + business row + outbox row) into one
  transaction per few thousand messages closed that entire gap without
  weakening the guarantee itself, only the unit it's applied to.
- **A format living up to its name is not something to assume** —
  `JSONAsString` sounds like "accept any JSON value as a string" and
  instead requires a JSON object specifically; the only way this surfaced
  was deliberately removing the setting that was quietly hiding it
  (`kafka_skip_broken_messages`) and reading the real exception.
  Silently-tolerant settings make debugging *harder*, not safer, when the
  real failure mode is "nothing is happening" rather than "something
  crashed loudly."
- **A live per-row transform and a live raw-byte copy are not the same
  cost, even when the transform itself is cheap per row** — the same
  ~4.1M-message backfill that OOM-killed ClickHouse twice with an inline
  JSON-unwrap completed in seconds, memory barely above baseline, once
  the transform was moved to a separate, controllable batch step after
  ingestion. Decoupling "durably capture the data" from "shape it into
  its final form" is a real, general pattern, not specific to this one
  pipeline.
- **Every headline number in this section — 23,532 orders/sec, 181
  seconds to drain 2 million messages, 132 transactions, $50,000,000 in
  45ms — is something a real command actually printed**, cross-checked
  across three independent systems (Postgres row counts, Kafka consumer-
  group lag, ClickHouse row counts), matching the standard every prior
  stage in this project held itself to.
