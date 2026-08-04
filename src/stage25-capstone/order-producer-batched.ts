import { randomUUID } from "crypto";
import { Pool } from "pg";

// Load-test-only producer, same rationale as payment-service-batched.ts.
// order-service.ts measured ~562 orders/sec doing one Postgres transaction
// per order, sequentially awaited — fine for the correctness demos (Phases
// 2-4), but at that rate 2,000,000 orders would take ~1 hour of wall clock
// just to produce, which would dominate the load test and starve the more
// interesting question (can the *consumer* side keep up). This batches the
// exact same dual-write (orders row + capstone_outbox row) N at a time in
// one transaction, mirroring payment-service-batched.ts's technique.
//
// SCENARIO=happy only, by construction — this file has no other mode. The
// mixed-scenario ID conventions stay exclusive to order-service.ts.
const pool = new Pool({
  host: process.env.PGHOST ?? "localhost",
  port: Number(process.env.PGPORT ?? 5433),
  user: "kafkaos",
  password: "kafkaos",
  database: "kafkaos",
  max: 10,
});

const RUN_TAG = randomUUID().slice(0, 8);
const TOTAL = Number(process.env.COUNT ?? 2_000_000);
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 2000);

const BATCH_SQL = `
  WITH batch AS (
    SELECT * FROM UNNEST($1::text[], $2::text[], $3::numeric[]) AS t(order_id, customer_id, total)
  ),
  ins_orders AS (
    INSERT INTO orders (order_id, customer_id, total, status)
    SELECT order_id, customer_id, total, 'created' FROM batch
    RETURNING order_id, customer_id, total
  )
  INSERT INTO capstone_outbox (aggregatetype, aggregateid, type, payload)
  SELECT 'orders', order_id, 'OrderCreated',
    jsonb_build_object(
      'type', 'OrderCreated',
      'order', jsonb_build_object(
        'orderId', order_id,
        'customerId', customer_id,
        'items', jsonb_build_array(jsonb_build_object('sku', 'sku-100', 'qty', 1)),
        'total', total,
        'status', 'created'
      )
    )
  FROM ins_orders;
`;

async function commitBatch(startIndex: number, count: number): Promise<void> {
  const orderIds: string[] = [];
  const customerIds: string[] = [];
  const totals: number[] = [];
  for (let i = 0; i < count; i++) {
    orderIds.push(`order-happy-loadtest-${RUN_TAG}-${startIndex + i}`);
    customerIds.push("cust-loadtest");
    totals.push(25);
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(BATCH_SQL, [orderIds, customerIds, totals]);
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

async function run() {
  const start = Date.now();
  let produced = 0;
  while (produced < TOTAL) {
    const count = Math.min(BATCH_SIZE, TOTAL - produced);
    await commitBatch(produced, count);
    produced += count;
    if (produced % (BATCH_SIZE * 25) === 0 || produced === TOTAL) {
      const elapsedSec = (Date.now() - start) / 1000;
      console.log(
        `[order-producer-batched] ${produced}/${TOTAL} committed (${(produced / elapsedSec).toFixed(0)} orders/sec avg)`
      );
    }
  }
  const elapsedSec = (Date.now() - start) / 1000;
  console.log(`[order-producer-batched] DONE: ${TOTAL} orders in ${elapsedSec.toFixed(1)}s (${(TOTAL / elapsedSec).toFixed(0)} orders/sec avg), run tag=${RUN_TAG}`);
  await pool.end();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
