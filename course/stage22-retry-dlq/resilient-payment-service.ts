import { Pool } from "pg";
import { Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";
import { Order } from "../../shared/types";
import { safeParseJson } from "../../shared/util";
import { callFlakyDownstream, DownstreamError } from "./flaky-downstream";
import { startMetricsServer, retryAttemptsTotal, dlqMessagesTotal } from "./metrics";

const TOPIC = process.env.TOPIC ?? "orders-resilience-demo";
const DLQ_TOPIC = process.env.DLQ_TOPIC ?? "payments-dlq";
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS ?? 3);
const BASE_BACKOFF_MS = Number(process.env.BASE_BACKOFF_MS ?? 200);
const METRICS_PORT = Number(process.env.METRICS_PORT ?? 9301);

const consumer = kafka.consumer({ groupId: "resilient-payment-service" });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const pool = new Pool({ host: "localhost", port: 5433, user: "kafkaos", password: "kafkaos", database: "kafkaos" });

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function isUniqueViolation(err: unknown): boolean {
  return typeof err === "object" && err !== null && "code" in err && (err as { code: unknown }).code === "23505";
}

// Same idempotent-write pattern as Stage 21: the dedup check and the side
// effect commit together, in one transaction — this is the piece that
// makes retrying (this stage) safe to combine with at-least-once Kafka
// redelivery (Stage 5) at all. Retrying a call that hasn't reached Kafka
// yet is safe on its own; what Stage 21's pattern additionally protects
// against is *this whole message* being redelivered later (e.g. after a
// crash right after these retries finished) and re-running the effect.
async function recordSuccess(t: string, partition: number, offset: string, order: Order, attempts: number) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    try {
      await client.query(`INSERT INTO processed_events (topic, partition, "offset") VALUES ($1, $2, $3)`, [
        t,
        partition,
        offset,
      ]);
      await client.query(
        `INSERT INTO payments_processed (order_id, status, attempts) VALUES ($1, 'succeeded', $2)
         ON CONFLICT (order_id) DO UPDATE SET attempts = payments_processed.attempts + $2`,
        [order.orderId, attempts]
      );
      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      if (!isUniqueViolation(err)) throw err;
      // Redelivery of an already-recorded success — nothing to do.
    }
  } finally {
    client.release();
  }
}

async function handleOrder(t: string, partition: number, offset: string, order: Order) {
  for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
    try {
      await callFlakyDownstream(order.orderId);
      retryAttemptsTotal.inc({ outcome: attempt === 1 ? "success" : "retry" });
      await recordSuccess(t, partition, offset, order, attempt);
      console.log(`[resilient-payment-service] order=${order.orderId} -> succeeded on attempt ${attempt}/${MAX_ATTEMPTS}`);
      return;
    } catch (err) {
      const reason = err instanceof DownstreamError ? err.message : String(err);
      if (attempt < MAX_ATTEMPTS) {
        const backoffMs = BASE_BACKOFF_MS * 2 ** (attempt - 1);
        console.log(
          `[resilient-payment-service] order=${order.orderId} attempt ${attempt}/${MAX_ATTEMPTS} failed (${reason}), retrying in ${backoffMs}ms`
        );
        await sleep(backoffMs);
        continue;
      }

      // Retries exhausted — this is a real, unrecoverable-for-now failure,
      // not something to keep retrying forever inline (that would block
      // every message behind it on this partition, same lesson as Stage
      // 20's poison-pill finding). Route to the DLQ with full context, and
      // make the failure loudly measurable, not just logged.
      retryAttemptsTotal.inc({ outcome: "exhausted" });
      dlqMessagesTotal.inc();
      await producer.send({
        topic: DLQ_TOPIC,
        messages: [
          {
            key: order.orderId,
            value: JSON.stringify({
              originalTopic: t,
              originalPartition: partition,
              originalOffset: offset,
              order,
              failureReason: reason,
              attempts: MAX_ATTEMPTS,
              failedAt: new Date().toISOString(),
            }),
          },
        ],
      });
      console.log(`[resilient-payment-service] order=${order.orderId} -> EXHAUSTED ${MAX_ATTEMPTS} attempts, routed to ${DLQ_TOPIC}`);
      return;
    }
  }
}

async function run() {
  startMetricsServer(METRICS_PORT);
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: t, partition, message }) => {
      const order = safeParseJson<Order>(message.value?.toString(), "resilient-payment-service");
      const nextOffset = (BigInt(message.offset) + 1n).toString();
      if (order) {
        await handleOrder(t, partition, message.offset, order);
      }
      await consumer.commitOffsets([{ topic: t, partition, offset: nextOffset }]);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
