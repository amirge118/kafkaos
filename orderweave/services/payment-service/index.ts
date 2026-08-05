import { Pool } from "pg";
import { Partitioners } from "kafkajs";
import { context, SpanStatusCode, type Context } from "@opentelemetry/api";
import { kafka } from "../../../shared/kafka";
import { initTracing, extractTraceContextFromPayload, injectTraceContextIntoPayload } from "../../../shared/tracing";
import { Order } from "../../../shared/types";
import { callFlakyDownstream, DownstreamError } from "../../shared/flaky-downstream";
import { parseOutboxMessage } from "../../shared/outbox-message";
import {
  startMetricsServer,
  eventsProcessedTotal,
  retryAttemptsTotal,
  dlqMessagesTotal,
  compensationsTotal,
} from "../../shared/metrics";

// Orderweave's most complex service — combines all four patterns:
//   - idempotent write (Stage 21): processed_events dedup, same transaction
//     as the side effect
//   - outbox (Stage 23): the side effect includes an outbox row, so the
//     "publish payments" step can never be forgotten independently of the
//     DB commit
//   - retry + backoff + DLQ (Stage 22): scoped to THIS service only,
//     deliberately — payment-service is the only step with a genuine
//     transient-infrastructure failure mode (flaky-downstream.ts). A
//     business decline ("payment-declined") is NOT retried — retrying a
//     declined card doesn't make it valid, so that path skips straight to
//     recording a failure, same as Stage 6/24's original behavior.
//   - compensation consumption (Stage 24): reacts to refund requests on a
//     separate topic, guaranteeing it can never mistake its own refund for
//     a new successful payment (no shared topic with the request, unlike
//     Stage 24's choreography demo, which needed an explicit status guard
//     for exactly that reason).
const SERVICE = "payment-service";
const MAX_ATTEMPTS = Number(process.env.MAX_ATTEMPTS ?? 3);
const BASE_BACKOFF_MS = Number(process.env.BASE_BACKOFF_MS ?? 200);
const METRICS_PORT = Number(process.env.METRICS_PORT ?? 9311);
const DLQ_TOPIC = "payments-dlq";
const COMPENSATION_TOPIC = "payment-compensation";

const tracer = initTracing(SERVICE);
const consumer = kafka.consumer({ groupId: SERVICE });
const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
const pool = new Pool({
  host: process.env.PGHOST ?? "localhost",
  port: Number(process.env.PGPORT ?? 5433),
  user: "kafkaos",
  password: "kafkaos",
  database: "kafkaos",
});

interface OrderCreatedPayload {
  type: string;
  order: Order;
  traceContext?: Record<string, string>;
}
interface CompensationRequestPayload {
  type: string;
  orderId: string;
  reason: string;
  traceContext?: Record<string, string>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
function isUniqueViolation(err: unknown): boolean {
  return typeof err === "object" && err !== null && "code" in err && (err as { code: unknown }).code === "23505";
}

async function recordPaymentOutcome(
  t: string,
  partition: number,
  offset: string,
  orderId: string,
  amount: number,
  status: "succeeded" | "failed",
  attempts: number,
  traceContext: Record<string, string>
): Promise<"applied" | "duplicate"> {
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
        `INSERT INTO payments (order_id, amount, status, retry_attempts) VALUES ($1, $2, $3, $4)
         ON CONFLICT (order_id) DO UPDATE SET status = $3, retry_attempts = payments.retry_attempts + $4`,
        [orderId, amount, status, attempts]
      );
      await client.query(`INSERT INTO capstone_outbox (aggregatetype, aggregateid, type, payload) VALUES ($1, $2, $3, $4)`, [
        "payments",
        orderId,
        status === "succeeded" ? "PaymentSucceeded" : "PaymentFailed",
        JSON.stringify({
          type: status === "succeeded" ? "PaymentSucceeded" : "PaymentFailed",
          orderId,
          amount,
          status,
          traceContext,
        }),
      ]);
      await client.query("COMMIT");
      return "applied";
    } catch (err) {
      await client.query("ROLLBACK");
      if (isUniqueViolation(err)) return "duplicate";
      throw err;
    }
  } finally {
    client.release();
  }
}

async function recordRefund(
  t: string,
  partition: number,
  offset: string,
  orderId: string,
  reason: string,
  traceContext: Record<string, string>
): Promise<"applied" | "duplicate"> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    try {
      await client.query(`INSERT INTO processed_events (topic, partition, "offset") VALUES ($1, $2, $3)`, [
        t,
        partition,
        offset,
      ]);
      await client.query(`INSERT INTO refunds (order_id, reason, status) VALUES ($1, $2, 'issued') ON CONFLICT (order_id) DO NOTHING`, [
        orderId,
        reason,
      ]);
      await client.query(`INSERT INTO capstone_outbox (aggregatetype, aggregateid, type, payload) VALUES ($1, $2, $3, $4)`, [
        "payments",
        orderId,
        "PaymentRefunded",
        JSON.stringify({ type: "PaymentRefunded", orderId, status: "refunded", reason, traceContext }),
      ]);
      await client.query("COMMIT");
      return "applied";
    } catch (err) {
      await client.query("ROLLBACK");
      if (isUniqueViolation(err)) return "duplicate";
      throw err;
    }
  } finally {
    client.release();
  }
}

async function handleOrder(t: string, partition: number, offset: string, order: Order, parentCtx: Context) {
  await tracer.startActiveSpan("payment.process", {}, parentCtx, async (span) => {
    span.setAttribute("order.id", order.orderId);

    if (order.orderId.includes("payment-declined")) {
      const traceContext = injectTraceContextIntoPayload(context.active());
      const outcome = await recordPaymentOutcome(t, partition, offset, order.orderId, order.total, "failed", 1, traceContext);
      eventsProcessedTotal.inc({ service: SERVICE, outcome });
      console.log(`[payment-service] order=${order.orderId} -> declined (${outcome})`);
      span.setStatus({ code: SpanStatusCode.OK });
      span.end();
      return;
    }

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      try {
        await callFlakyDownstream(order.orderId);
        retryAttemptsTotal.inc({ service: SERVICE, outcome: attempt === 1 ? "success" : "retry" });
        const traceContext = injectTraceContextIntoPayload(context.active());
        const outcome = await recordPaymentOutcome(t, partition, offset, order.orderId, order.total, "succeeded", attempt, traceContext);
        eventsProcessedTotal.inc({ service: SERVICE, outcome });
        console.log(`[payment-service] order=${order.orderId} -> succeeded on attempt ${attempt}/${MAX_ATTEMPTS} (${outcome})`);
        span.setStatus({ code: SpanStatusCode.OK });
        span.end();
        return;
      } catch (err) {
        const reason = err instanceof DownstreamError ? err.message : String(err);
        if (attempt < MAX_ATTEMPTS) {
          const backoffMs = BASE_BACKOFF_MS * 2 ** (attempt - 1);
          console.log(
            `[payment-service] order=${order.orderId} attempt ${attempt}/${MAX_ATTEMPTS} failed (${reason}), retrying in ${backoffMs}ms`
          );
          await sleep(backoffMs);
          continue;
        }

        retryAttemptsTotal.inc({ service: SERVICE, outcome: "exhausted" });
        dlqMessagesTotal.inc({ service: SERVICE });
        const traceContext = injectTraceContextIntoPayload(context.active());
        const outcome = await recordPaymentOutcome(t, partition, offset, order.orderId, order.total, "failed", MAX_ATTEMPTS, traceContext);
        eventsProcessedTotal.inc({ service: SERVICE, outcome });
        await producer.send({
          topic: DLQ_TOPIC,
          messages: [
            {
              key: order.orderId,
              value: JSON.stringify({ order, failureReason: reason, attempts: MAX_ATTEMPTS, failedAt: new Date().toISOString() }),
            },
          ],
        });
        console.log(
          `[payment-service] order=${order.orderId} -> EXHAUSTED ${MAX_ATTEMPTS} attempts (${outcome}), routed to ${DLQ_TOPIC}`
        );
        span.setStatus({ code: SpanStatusCode.OK });
        span.end();
        return;
      }
    }
  });
}

async function handleCompensationRequest(
  t: string,
  partition: number,
  offset: string,
  req: CompensationRequestPayload,
  parentCtx: Context
) {
  await tracer.startActiveSpan("payment.compensate", {}, parentCtx, async (span) => {
    span.setAttribute("order.id", req.orderId);
    const traceContext = injectTraceContextIntoPayload(context.active());
    const outcome = await recordRefund(t, partition, offset, req.orderId, req.reason, traceContext);
    if (outcome === "applied") compensationsTotal.inc({ service: SERVICE });
    console.log(`[payment-service] order=${req.orderId} -> REFUNDED (${outcome}), reason: ${req.reason}`);
    span.setStatus({ code: SpanStatusCode.OK });
    span.end();
  });
}

// Deterministic crash point, same convention as Stage 5/21/22's
// CRASH_AFTER — a real SIGKILL lands at an unpredictable point in
// kafkajs's processing loop, which is realistic for the "does this
// survive a real crash" story but not reliable for cleanly demonstrating
// the specific redelivery -> duplicate mechanism. This crashes right
// after the Nth message's DB transaction commits but before the Kafka
// offset commit — the exact window where redelivery is guaranteed.
const crashAfterN = process.env.CRASH_AFTER_N ? Number(process.env.CRASH_AFTER_N) : undefined;
let processedCount = 0;

async function run() {
  startMetricsServer(METRICS_PORT);
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: "orders", fromBeginning: true });
  await consumer.subscribe({ topic: COMPENSATION_TOPIC, fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: t, partition, message }) => {
      const nextOffset = (BigInt(message.offset) + 1n).toString();

      if (t === "orders") {
        const parsed = parseOutboxMessage<OrderCreatedPayload>(message.value?.toString(), SERVICE);
        if (parsed?.order) {
          const parentCtx = extractTraceContextFromPayload(parsed.traceContext);
          await handleOrder(t, partition, message.offset, parsed.order, parentCtx);
        }
      } else if (t === COMPENSATION_TOPIC) {
        const parsed = parseOutboxMessage<CompensationRequestPayload>(message.value?.toString(), SERVICE);
        if (parsed?.orderId) {
          const parentCtx = extractTraceContextFromPayload(parsed.traceContext);
          await handleCompensationRequest(t, partition, message.offset, parsed, parentCtx);
        }
      }

      processedCount++;
      if (processedCount === crashAfterN) {
        console.log(`[CRASH] handled message ${processedCount} (offset=${message.offset}), exiting BEFORE committing offset=${nextOffset}`);
        process.exit(1);
      }

      await consumer.commitOffsets([{ topic: t, partition, offset: nextOffset }]);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
