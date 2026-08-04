import { randomUUID } from "crypto";
import { Pool } from "pg";
import { context, SpanStatusCode } from "@opentelemetry/api";
import { initTracing, injectTraceContextIntoPayload, shutdownTracing } from "../shared/tracing";
import { Order } from "../shared/types";

// Orderweave's entry point. Dual-write logic is Stage 23's outbox pattern
// verbatim (one transaction: business row + outbox row — Debezium, not
// this process, is what actually publishes to Kafka). Trace propagation
// uses Phase 1's payload-embedding helper instead of Stage 18's Kafka-
// header helper, since this process never gets to set the eventual Kafka
// message's headers itself.
const tracer = initTracing("order-service");
const pool = new Pool({
  host: process.env.PGHOST ?? "localhost",
  port: Number(process.env.PGPORT ?? 5433),
  user: "kafkaos",
  password: "kafkaos",
  database: "kafkaos",
});

// 0-based index into the generated orders array — crash right after that
// order's transaction commits, same mechanism as Stage 21's CRASH_AFTER.
const crashAfterIndex = process.env.CRASH_AFTER_INDEX ? Number(process.env.CRASH_AFTER_INDEX) : undefined;

// Deterministic scenario coverage matching Stage 22/24's ID conventions
// exactly, so the same flaky-downstream/inventory-failure/DLQ logic those
// stages already proved correct works here completely unmodified — one
// seed run exercises every real path through the assembled system.
const SCENARIOS = [
  { suffix: "happy", customerId: "cust-A", total: 25 },
  { suffix: "payment-declined", customerId: "cust-B", total: 40 },
  { suffix: "out-of-stock", customerId: "cust-C", total: 60 },
  { suffix: "transient-fail-3", customerId: "cust-D", total: 35 },
  { suffix: "permanent-fail", customerId: "cust-E", total: 80 },
];

// Load-test mode (Stage 25's millions-of-messages run): pure happy-path
// orders only. Mixing in the scenario variety above would confound a
// throughput measurement with retry-service's *deliberate* 200-600ms
// backoff sleeps for transient-fail/permanent-fail orders — real and
// correct behavior, but not what "how fast can this pipeline go" is
// asking. Kept as a separate mode rather than replacing the default, so
// Phase 2-4's correctness demos keep their real scenario coverage.
const onlyHappy = process.env.SCENARIO === "happy";

function makeOrders(count: number): Order[] {
  const orders: Order[] = [];
  for (let i = 0; i < count; i++) {
    const scenario = onlyHappy ? SCENARIOS[0] : SCENARIOS[i % SCENARIOS.length];
    orders.push({
      orderId: `order-${scenario.suffix}-${randomUUID().slice(0, 8)}`,
      customerId: scenario.customerId,
      items: [{ sku: "sku-100", qty: 1 }],
      total: scenario.total,
      status: "created",
    });
  }
  return orders;
}

async function run() {
  const count = Number(process.env.COUNT ?? SCENARIOS.length);
  const orders = makeOrders(count);

  for (let i = 0; i < orders.length; i++) {
    const order = orders[i];

    await tracer.startActiveSpan("order.create", async (span) => {
      span.setAttribute("order.id", order.orderId);
      span.setAttribute("order.total", order.total);

      const traceContext = injectTraceContextIntoPayload(context.active());

      const client = await pool.connect();
      try {
        await client.query("BEGIN");
        await client.query(`INSERT INTO orders (order_id, customer_id, total, status) VALUES ($1, $2, $3, $4)`, [
          order.orderId,
          order.customerId,
          order.total,
          order.status,
        ]);
        await client.query(
          `INSERT INTO capstone_outbox (aggregatetype, aggregateid, type, payload) VALUES ($1, $2, $3, $4)`,
          ["orders", order.orderId, "OrderCreated", JSON.stringify({ type: "OrderCreated", order, traceContext })]
        );
        await client.query("COMMIT");
        console.log(`[order-service] committed order + outbox row: ${order.orderId}`);
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }

      span.setStatus({ code: SpanStatusCode.OK });
      span.end();
    });

    if (i === crashAfterIndex) {
      console.log(`[CRASH] DB transaction already committed for ${order.orderId} — exiting now`);
      await shutdownTracing();
      process.exit(1);
    }
  }

  await pool.end();
  await shutdownTracing();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
