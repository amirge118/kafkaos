import { Pool } from "pg";
import { context, SpanStatusCode, type Context } from "@opentelemetry/api";
import { kafka } from "../../../shared/kafka";
import { initTracing, extractTraceContextFromPayload, injectTraceContextIntoPayload } from "../../../shared/tracing";
import { parseOutboxMessage } from "../../shared/outbox-message";
import { startMetricsServer, eventsProcessedTotal } from "../../shared/metrics";

// Reservation logic + the SLOW_MS backpressure knob are Stage 6's
// inventory-service unchanged in intent. The out-of-stock -> compensation-
// request path is Stage 24's addition: unlike Stage 6, a reservation
// failure here doesn't just skip and move on — it explicitly asks
// payment-service to reverse a payment that already succeeded. This
// service only ever *requests* compensation (via a separate topic); it
// never consumes its own request or payment-service's refund confirmation,
// so there's no possible loop here the way Stage 24's choreography demo
// had to guard against explicitly.
const SERVICE = "inventory-service";
const METRICS_PORT = Number(process.env.METRICS_PORT ?? 9312);
const COMPENSATION_TOPIC = "payment-compensation";
const slowMs = Number(process.env.SLOW_MS ?? 0);

const tracer = initTracing(SERVICE);
const consumer = kafka.consumer({ groupId: SERVICE });
const pool = new Pool({
  host: process.env.PGHOST ?? "localhost",
  port: Number(process.env.PGPORT ?? 5433),
  user: "kafkaos",
  password: "kafkaos",
  database: "kafkaos",
});

interface PaymentEventPayload {
  type: string;
  orderId: string;
  amount?: number;
  status: string;
  traceContext?: Record<string, string>;
}

function isUniqueViolation(err: unknown): boolean {
  return typeof err === "object" && err !== null && "code" in err && (err as { code: unknown }).code === "23505";
}
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function recordReservation(
  t: string,
  partition: number,
  offset: string,
  orderId: string,
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
        `INSERT INTO inventory_reservations (order_id, reserved) VALUES ($1, true)
         ON CONFLICT (order_id) DO NOTHING`,
        [orderId]
      );
      await client.query(`INSERT INTO capstone_outbox (aggregatetype, aggregateid, type, payload) VALUES ($1, $2, $3, $4)`, [
        "inventory",
        orderId,
        "InventoryReserved",
        JSON.stringify({ type: "InventoryReserved", orderId, reserved: true, traceContext }),
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

async function recordCompensationRequest(
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
      await client.query(`INSERT INTO capstone_outbox (aggregatetype, aggregateid, type, payload) VALUES ($1, $2, $3, $4)`, [
        "payment-compensation",
        orderId,
        "CompensationRequested",
        JSON.stringify({ type: "CompensationRequested", orderId, reason, traceContext }),
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

async function handlePayment(t: string, partition: number, offset: string, payment: PaymentEventPayload, parentCtx: Context) {
  await tracer.startActiveSpan("inventory.reserve", {}, parentCtx, async (span) => {
    span.setAttribute("order.id", payment.orderId);

    if (payment.status !== "succeeded") {
      console.log(`[inventory-service] order=${payment.orderId} skipped (payment ${payment.status})`);
      span.setStatus({ code: SpanStatusCode.OK });
      span.end();
      return;
    }

    if (slowMs > 0) await sleep(slowMs);

    if (payment.orderId.includes("out-of-stock")) {
      const traceContext = injectTraceContextIntoPayload(context.active());
      const outcome = await recordCompensationRequest(
        t,
        partition,
        offset,
        payment.orderId,
        "inventory reservation failed: out of stock",
        traceContext
      );
      eventsProcessedTotal.inc({ service: SERVICE, outcome: outcome === "applied" ? "compensation-triggered" : "duplicate" });
      console.log(`[inventory-service] order=${payment.orderId} -> RESERVATION FAILED, requested compensation (${outcome})`);
      span.setStatus({ code: SpanStatusCode.OK });
      span.end();
      return;
    }

    const traceContext = injectTraceContextIntoPayload(context.active());
    const outcome = await recordReservation(t, partition, offset, payment.orderId, traceContext);
    eventsProcessedTotal.inc({ service: SERVICE, outcome });
    console.log(`[inventory-service] order=${payment.orderId} -> reserved (${outcome})`);
    span.setStatus({ code: SpanStatusCode.OK });
    span.end();
  });
}

async function run() {
  startMetricsServer(METRICS_PORT);
  await consumer.connect();
  await consumer.subscribe({ topic: "payments", fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: t, partition, message }) => {
      const nextOffset = (BigInt(message.offset) + 1n).toString();
      const parsed = parseOutboxMessage<PaymentEventPayload>(message.value?.toString(), SERVICE);
      if (parsed?.orderId) {
        const parentCtx = extractTraceContextFromPayload(parsed.traceContext);
        await handlePayment(t, partition, message.offset, parsed, parentCtx);
      }
      await consumer.commitOffsets([{ topic: t, partition, offset: nextOffset }]);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
