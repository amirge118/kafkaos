import { kafka } from "../../shared/kafka";

// Consume-transform-produce, tied together atomically: the produced output
// and the consumer offset commit either both become visible, or neither
// does. This is the piece Stage 5 explicitly deferred — at-least-once
// (Stage 5) guarantees no *input* is lost but can duplicate *output*;
// transactions fix the output-duplication problem for this exact pattern
// (the one payment-service/inventory-service/shipping-service all use).
const groupId = process.env.GROUP_ID ?? "transactional-processor";
const transactionalId = process.env.TRANSACTIONAL_ID ?? "kafkaos-transactional-processor-1";
const topic = process.env.TOPIC ?? "orders-crash-demo";
const outputTopic = process.env.OUTPUT_TOPIC ?? "processed-orders-transactional";
const crashAfter = process.env.CRASH_AFTER ? Number(process.env.CRASH_AFTER) : undefined;
const runMs = Number(process.env.RUN_MS ?? 10000);

// transactionalId must be STABLE across restarts of "the same" logical
// producer — that's how the broker recognizes and fences/recovers a dangling
// transaction left behind by a crash, instead of treating a restart as some
// unrelated new producer.
const producer = kafka.producer({ transactionalId, maxInFlightRequests: 1, idempotent: true });
const consumer = kafka.consumer({ groupId });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });

  let processedCount = 0;

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic: t, partition, message }) => {
      processedCount++;
      const key = message.key?.toString() ?? "(none)";

      const transaction = await producer.transaction();

      await transaction.send({
        topic: outputTopic,
        messages: [{ key, value: JSON.stringify({ processedKey: key, sourceOffset: message.offset }) }],
      });

      if (processedCount === crashAfter) {
        console.log(`[CRASH] produced for key=${key} inside an OPEN transaction, exiting BEFORE commit`);
        process.exit(1); // no commitTransaction() — the transaction is left dangling on purpose
      }

      await transaction.sendOffsets({
        consumerGroupId: groupId,
        topics: [{ topic: t, partitions: [{ partition, offset: (Number(message.offset) + 1).toString() }] }],
      });
      await transaction.commit();

      console.log(`[processed] key=${key} offset=${message.offset} -> committed atomically (produce + offset commit)`);
    },
  });

  setTimeout(async () => {
    console.log(`finished run, ${processedCount} message(s) processed, disconnecting`);
    await consumer.disconnect();
    await producer.disconnect();
    process.exit(0);
  }, runMs);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
