import { Pool } from "pg";
import { kafka } from "../shared/kafka";
import { Order } from "../shared/types";
import { safeParseJson } from "../shared/util";

// Same at-least-once ordering, same crash mechanism, as Stage 5's
// consumer-semantics.ts (MODE=at-least-once): do the work, THEN commit the
// Kafka offset. A crash between those two steps still causes the exact
// same redelivery on restart — that hazard isn't fixed here, and can't be,
// short of Kafka transactions (Stage 10). What's fixed is what redelivery
// *does*: instead of a second real side effect, it hits a unique
// constraint and gets skipped.
const groupId = process.env.GROUP_ID ?? "idempotent-demo";
const topic = process.env.TOPIC ?? "orders-idempotent-demo";
const crashAfter = process.env.CRASH_AFTER ? Number(process.env.CRASH_AFTER) : undefined;

const consumer = kafka.consumer({ groupId });
const pool = new Pool({
  host: process.env.PGHOST ?? "localhost",
  // Not 5432 — see docker-compose.yml's postgres service comment and
  // NOTES.md Stage 21: this Mac has a native Postgres already on 5432.
  port: Number(process.env.PGPORT ?? 5433),
  user: "kafkaos",
  password: "kafkaos",
  database: "kafkaos",
});

function isUniqueViolation(err: unknown): boolean {
  return typeof err === "object" && err !== null && "code" in err && (err as { code: unknown }).code === "23505";
}

// Returns "applied" if the side effect actually ran this call, "duplicate"
// if this exact (topic, partition, offset) delivery was already handled —
// the dedup check and the side effect share one transaction, so there's no
// window where one committed without the other.
async function processIdempotently(
  t: string,
  partition: number,
  offset: string,
  order: Order
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
        `INSERT INTO orders_processed (order_id, status, effect_count)
         VALUES ($1, $2, 1)
         ON CONFLICT (order_id) DO UPDATE SET effect_count = orders_processed.effect_count + 1`,
        [order.orderId, order.status]
      );
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

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  let processedCount = 0;

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: t, partition, message }) => {
      processedCount++;
      const nextOffset = (BigInt(message.offset) + 1n).toString();
      const order = safeParseJson<Order>(message.value?.toString(), "idempotent-consumer");
      if (!order) {
        await consumer.commitOffsets([{ topic: t, partition, offset: nextOffset }]);
        return;
      }

      const outcome = await processIdempotently(t, partition, message.offset, order);
      console.log(`[idempotent-consumer] pid=${process.pid} order=${order.orderId} offset=${message.offset} -> ${outcome}`);

      if (processedCount === crashAfter) {
        console.log(`[CRASH] handled offset=${message.offset} (${outcome}), exiting BEFORE committing offset=${nextOffset}`);
        process.exit(1);
      }

      await consumer.commitOffsets([{ topic: t, partition, offset: nextOffset }]);
    },
  });

  setTimeout(async () => {
    console.log(`[idempotent-consumer] finished run, ${processedCount} message(s) processed, disconnecting`);
    await consumer.disconnect();
    await pool.end();
    process.exit(0);
  }, Number(process.env.RUN_MS ?? 8000));
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
