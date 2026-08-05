import { Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";

// kafkajs forces acks=-1 and bounded in-flight requests when idempotent:true —
// required for the broker to track (producerId, epoch, sequence) per partition
// and dedupe retries of the *same* in-flight batch.
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
  idempotent: true,
});

async function run() {
  await producer.connect();

  const order = { orderId: "order-idem-1", status: "created" };

  // Two independent, application-level send() calls for "the same" logical
  // order. Idempotence does NOT protect against this — it only dedupes
  // retries the producer's own internals issue for a single send() attempt
  // (e.g. an ack that timed out on the wire, ambiguous outcome). From the
  // broker's point of view these are two separate, unrelated batches.
  const r1 = await producer.send({
    topic: "orders-semantics",
    messages: [{ key: order.orderId, value: JSON.stringify(order) }],
  });
  console.log(`send #1 -> partition ${r1[0].partition} offset ${r1[0].baseOffset}`);

  const r2 = await producer.send({
    topic: "orders-semantics",
    messages: [{ key: order.orderId, value: JSON.stringify(order) }],
  });
  console.log(`send #2 (application-level "retry") -> partition ${r2[0].partition} offset ${r2[0].baseOffset}`);

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
