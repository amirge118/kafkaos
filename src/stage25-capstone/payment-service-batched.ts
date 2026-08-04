import { Pool } from "pg";
import { kafka } from "../shared/kafka";
import { Order } from "../shared/types";
import { parseOutboxMessage } from "./outbox-message";
import { startMetricsServer, eventsProcessedTotal } from "./metrics";

// Load-test-only variant of payment-service.ts. NOT a replacement — the
// strict one-transaction-per-message version stays exactly as built for
// Phases 2-4 (it's what makes the idempotency/retry/DLQ/compensation
// demos easy to reason about one message at a time). This file exists
// because the real rehearsal numbers (two independent measurements, both
// single-digit messages/sec on a per-message Postgres transaction) proved
// that granularity can't reach millions of messages in a practical amount
// of time. Stage 13's own finding — batching is the dominant Kafka
// throughput lever — turns out to apply just as much to the Postgres side
// of this pipeline as it does to the producer side.
//
// Deliberate scope narrowing versus the strict version (both documented
// trade-offs, not oversights):
//   - Consumes ONLY the `orders` topic. The load test drives SCENARIO=happy
//     traffic exclusively (order-service.ts), which never produces a
//     `payment-declined`/out-of-stock order, so there is nothing for this
//     process to compensate — subscribing to `payment-compensation` would
//     be dead code for this run.
//   - No retry/backoff loop. flaky-downstream.ts's failure modes are keyed
//     off order ID substrings ("permanent-fail", "transient-fail-N") that
//     SCENARIO=happy never generates, so every call succeeds on the first
//     attempt by construction. A batch that hit a real failure would need
//     per-message fallback handling this file intentionally doesn't build —
//     it is not meant to run against mixed-scenario traffic.
//   - Same group ID as the strict service ("payment-service") since it's a
//     substitute for the same role, not a second consumer. Only one of the
//     two processes should ever be running at a time.
const SERVICE = "payment-service";
const METRICS_PORT = Number(process.env.METRICS_PORT ?? 9311);

const consumer = kafka.consumer({
  groupId: SERVICE,
  // Wide per-partition fetch window so a fetch pulls a large run of
  // backlog at once instead of trickling in small batches — this is the
  // whole point of the eachBatch switch.
  maxBytesPerPartition: 5 * 1024 * 1024,
  maxWaitTimeInMs: 500,
});
const pool = new Pool({
  host: process.env.PGHOST ?? "localhost",
  port: Number(process.env.PGPORT ?? 5433),
  user: "kafkaos",
  password: "kafkaos",
  database: "kafkaos",
  max: 10,
});

interface OrderCreatedPayload {
  type: string;
  order: Order;
}

interface Row {
  partition: number;
  offset: string;
  orderId: string;
  amount: number;
}

// One Postgres round trip for the whole batch: chained data-modifying CTEs
// (Postgres guarantees a later CTE sees an earlier CTE's effects when it
// references it by name) do the dedup-insert, the payments upsert, and the
// outbox insert as a single atomic statement — the batch equivalent of
// Stage 21+23's per-message "processed_events + business row + outbox row,
// one transaction" pattern.
const BATCH_SQL = `
  WITH batch AS (
    SELECT * FROM UNNEST($1::text[], $2::int[], $3::bigint[], $4::text[], $5::numeric[])
      AS t(topic, partition, "offset", order_id, amount)
  ),
  ins_events AS (
    INSERT INTO processed_events (topic, partition, "offset")
    SELECT topic, partition, "offset" FROM batch
    ON CONFLICT (topic, partition, "offset") DO NOTHING
    RETURNING topic, partition, "offset"
  ),
  ins_payments AS (
    INSERT INTO payments (order_id, amount, status, retry_attempts)
    SELECT b.order_id, b.amount, 'succeeded', 1
    FROM batch b
    JOIN ins_events e USING (topic, partition, "offset")
    ON CONFLICT (order_id) DO UPDATE
      SET status = EXCLUDED.status, retry_attempts = payments.retry_attempts + 1
    RETURNING order_id, amount
  )
  INSERT INTO capstone_outbox (aggregatetype, aggregateid, type, payload)
  SELECT 'payments', order_id, 'PaymentSucceeded',
    jsonb_build_object('type', 'PaymentSucceeded', 'orderId', order_id, 'amount', amount, 'status', 'succeeded')
  FROM ins_payments;
`;

async function commitBatch(topic: string, rows: Row[]): Promise<number> {
  const topics = rows.map(() => topic);
  const partitions = rows.map((r) => r.partition);
  const offsets = rows.map((r) => r.offset);
  const orderIds = rows.map((r) => r.orderId);
  const amounts = rows.map((r) => r.amount);

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await client.query(BATCH_SQL, [topics, partitions, offsets, orderIds, amounts]);
    await client.query("COMMIT");
    return result.rowCount ?? 0;
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function run() {
  startMetricsServer(METRICS_PORT);
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
      if (!isRunning() || isStale()) return;

      const rows: Row[] = [];
      for (const message of batch.messages) {
        const parsed = parseOutboxMessage<OrderCreatedPayload>(message.value?.toString(), SERVICE);
        if (parsed?.order) {
          rows.push({ partition: batch.partition, offset: message.offset, orderId: parsed.order.orderId, amount: parsed.order.total });
        }
      }

      if (rows.length > 0) {
        const applied = await commitBatch(batch.topic, rows);
        const duplicate = rows.length - applied;
        eventsProcessedTotal.inc({ service: SERVICE, outcome: "applied" }, applied);
        if (duplicate > 0) eventsProcessedTotal.inc({ service: SERVICE, outcome: "duplicate" }, duplicate);
        console.log(
          `[payment-service-batched] partition=${batch.partition} batch=${batch.messages.length} applied=${applied} duplicate=${duplicate}`
        );
      }

      for (const message of batch.messages) resolveOffset(message.offset);
      await heartbeat();
      // Explicit commit every batch, not commitOffsetsIfNecessary() — that
      // helper throttles internally (commits only past an internal
      // count/time threshold), which would leave Kafka's reported lag
      // trailing behind what's actually been committed to Postgres. For a
      // load test whose whole point is measuring real catch-up time via
      // lag, the committed offset needs to track actual progress exactly.
      const lastOffset = batch.messages[batch.messages.length - 1].offset;
      await consumer.commitOffsets([
        { topic: batch.topic, partition: batch.partition, offset: (BigInt(lastOffset) + 1n).toString() },
      ]);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
