import { randomUUID, createHash } from "crypto";
import { Partitioners } from "kafkajs";
import { kafka } from "../shared/kafka";
import { uploadBlob, BUCKET } from "./blob-store";

const TOPIC = process.env.TOPIC ?? "attachment-events";
const COUNT = Number(process.env.COUNT ?? 20);
// Deliberately allowed to exceed Kafka's message.max.bytes (1,048,588B on
// this cluster, confirmed for real in NOTES.md Stage 20) — the entire
// point of this pattern is that the attachment's size becomes Kafka's
// problem never again, once it's the blob store's problem instead.
const SIZE = Number(process.env.SIZE ?? 2_000_000);

function makeAttachment(size: number): Buffer {
  // Repeated-but-not-trivial content (not all zero bytes) — realistic
  // enough for a checksum-integrity check to mean something.
  const chunk = Buffer.from(`attachment-content-${randomUUID()}-`);
  const buf = Buffer.alloc(size);
  for (let i = 0; i < size; i += chunk.length) {
    chunk.copy(buf, i, 0, Math.min(chunk.length, size - i));
  }
  return buf;
}

async function run() {
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();

  let totalReferenceBytes = 0;
  const start = Date.now();

  for (let i = 0; i < COUNT; i++) {
    const blob = makeAttachment(SIZE);
    const checksum = createHash("sha256").update(blob).digest("hex");
    const key = `attachments/${randomUUID()}.bin`;

    // The large payload goes to the blob store — Kafka never sees it.
    await uploadBlob(key, blob, "application/octet-stream");

    // Kafka only ever carries this: a small, fixed-size reference.
    const reference = {
      eventId: randomUUID(),
      bucket: BUCKET,
      key,
      sizeBytes: blob.length,
      checksum,
      contentType: "application/octet-stream",
    };
    const value = JSON.stringify(reference);
    totalReferenceBytes += Buffer.byteLength(value);

    await producer.send({
      topic: TOPIC,
      messages: [{ key: reference.eventId, value }],
    });

    if ((i + 1) % 10 === 0) console.log(`  ...${i + 1}/${COUNT} attachments uploaded + referenced`);
  }

  const elapsedSec = (Date.now() - start) / 1000;
  console.log(`--- claim-check-producer: ${COUNT} attachments, ${SIZE.toLocaleString()}B each ---`);
  console.log(
    `elapsed: ${elapsedSec.toFixed(2)}s | ${(COUNT / elapsedSec).toFixed(1)} attachments/sec | ` +
      `avg Kafka message size: ${(totalReferenceBytes / COUNT).toFixed(0)}B (the blob itself never touched Kafka)`
  );

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
