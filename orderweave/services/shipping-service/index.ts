import { Pool } from "pg";
import { context, SpanStatusCode, type Context } from "@opentelemetry/api";
import { kafka } from "../../../shared/kafka";
import { initTracing, extractTraceContextFromPayload, injectTraceContextIntoPayload } from "../../../shared/tracing";
import { parseOutboxMessage } from "../../shared/outbox-message";
import { startMetricsServer, eventsProcessedTotal } from "../../shared/metrics";

// End of the chain — unchanged in intent from Stage 6: every message on
// `inventory` already represents a successful reservation (a failed
// reservation goes to `payment-compensation` instead and never reaches
// this topic at all), so there's no status filter needed here, just
// idempotent+outbox+tracing added for consistency with the other three
// services — interview-defensible even though shipping can't fail today:
// "the plumbing for a future compensable shipping failure is already here."
const SERVICE = "shipping-service";
const METRICS_PORT = Number(process.env.METRICS_PORT ?? 9313);

const tracer = initTracing(SERVICE);
const consumer = kafka.consumer({ groupId: SERVICE });
const pool = new Pool({
  host: process.env.PGHOST ?? "localhost",
  port: Number(process.env.PGPORT ?? 5433),
  user: "kafkaos",
  password: "kafkaos",
  database: "kafkaos",
});

interface InventoryEventPayload {
  type: string;
  orderId: string;
  reserved: boolean;
  traceContext?: Record<string, string>;
}

function isUniqueViolation(err: unknown): boolean {
  return typeof err === "object" && err !== null && "code" in err && (err as { code: unknown }).code === "23505";
}

async function recordShipment(
  t: string,
  partition: number,
  offset: string,
  orderId: string,
  trackingId: string,
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
      await client.query(`INSERT INTO shipments (order_id, tracking_id) VALUES ($1, $2) ON CONFLICT (order_id) DO NOTHING`, [
        orderId,
        trackingId,
      ]);
      await client.query(`INSERT INTO capstone_outbox (aggregatetype, aggregateid, type, payload) VALUES ($1, $2, $3, $4)`, [
        "shipping",
        orderId,
        "ShipmentCreated",
        JSON.stringify({ type: "ShipmentCreated", orderId, trackingId, traceContext }),
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

async function handleReservation(t: string, partition: number, offset: string, reservation: InventoryEventPayload, parentCtx: Context) {
  await tracer.startActiveSpan("shipping.create", {}, parentCtx, async (span) => {
    span.setAttribute("order.id", reservation.orderId);

    const trackingId = `TRK-${reservation.orderId}-${Date.now()}`;
    span.setAttribute("shipment.trackingId", trackingId);

    const traceContext = injectTraceContextIntoPayload(context.active());
    const outcome = await recordShipment(t, partition, offset, reservation.orderId, trackingId, traceContext);
    eventsProcessedTotal.inc({ service: SERVICE, outcome });
    console.log(`[shipping-service] order=${reservation.orderId} -> shipped, tracking=${trackingId} (${outcome})`);

    span.setStatus({ code: SpanStatusCode.OK });
    span.end();
  });
}

async function run() {
  startMetricsServer(METRICS_PORT);
  await consumer.connect();
  await consumer.subscribe({ topic: "inventory", fromBeginning: true });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: t, partition, message }) => {
      const nextOffset = (BigInt(message.offset) + 1n).toString();
      const parsed = parseOutboxMessage<InventoryEventPayload>(message.value?.toString(), SERVICE);
      if (parsed?.orderId) {
        const parentCtx = extractTraceContextFromPayload(parsed.traceContext);
        await handleReservation(t, partition, message.offset, parsed, parentCtx);
      }
      await consumer.commitOffsets([{ topic: t, partition, offset: nextOffset }]);
    },
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
