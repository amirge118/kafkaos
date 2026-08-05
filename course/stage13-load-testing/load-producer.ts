import { CompressionCodecs, CompressionTypes, Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";
import { ZstdCodec } from "./zstd-codec";
import { Lz4Codec } from "./lz4-codec";
import { codecStats, resetCodecStats } from "./codec-stats";

CompressionCodecs[CompressionTypes.ZSTD] = ZstdCodec;
CompressionCodecs[CompressionTypes.LZ4] = Lz4Codec;

const TOPIC = process.env.TOPIC ?? "load-test";
const COUNT = Number(process.env.COUNT ?? 100_000);
const SIZE = Number(process.env.SIZE ?? 256);
// Messages accumulated into a single producer.send() call — kafkajs has no
// client-side auto-batching across separate send() calls (unlike the Java
// client's batch.size), so this is our own accumulation buffer standing in
// for it.
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 1);
// Max ms to hold a partial batch open before flushing anyway — our stand-in
// for linger.ms. Only matters when messages arrive slower than BATCH_SIZE
// fills up on its own (see RATE below).
const LINGER_MS = Number(process.env.LINGER_MS ?? 0);
// Messages/sec to pace enqueueing at, instead of firing as fast as
// possible. 0 = unlimited (max-throughput mode, the default).
const RATE = Number(process.env.RATE ?? 0);
const COMPRESSION = (process.env.COMPRESSION ?? "none") as "none" | "gzip" | "lz4" | "zstd";
const ACKS = Number(process.env.ACKS ?? 1) as -1 | 0 | 1;

const compressionType = {
  none: CompressionTypes.None,
  gzip: CompressionTypes.GZIP,
  lz4: CompressionTypes.LZ4,
  zstd: CompressionTypes.ZSTD,
}[COMPRESSION];

// A realistic, compressible payload (repeated JSON structure + a padded
// filler field), not random bytes — random bytes would be incompressible
// and make every compression codec look identically useless, which isn't
// representative of real Kafka traffic (JSON/logs/text).
function makePayload(i: number, size: number): string {
  const base = {
    orderId: `order-${i}`,
    customerId: `cust-${i % 5000}`,
    items: [
      { sku: "SKU-1234", qty: 2 },
      { sku: "SKU-5678", qty: 1 },
    ],
    total: 99.95,
    status: "created",
    notes: "",
  };
  const withoutNotes = JSON.stringify(base).length;
  const padLength = Math.max(0, size - withoutNotes);
  base.notes = "standard delivery, leave at front door if no answer ".repeat(
    Math.ceil(padLength / 48)
  ).slice(0, padLength);
  return JSON.stringify(base);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function run() {
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  resetCodecStats();

  let requestCount = 0;
  let bytesSent = 0;
  let buffer: { key: string; value: string }[] = [];
  let bufferOpenedAt = 0;

  async function flush() {
    if (buffer.length === 0) return;
    const messages = buffer;
    buffer = [];
    requestCount++;
    for (const m of messages) bytesSent += Buffer.byteLength(m.value);
    await producer.send({
      topic: TOPIC,
      acks: ACKS,
      compression: compressionType,
      messages,
    });
  }

  const start = Date.now();

  for (let i = 0; i < COUNT; i++) {
    if (buffer.length === 0) bufferOpenedAt = Date.now();
    buffer.push({ key: `order-${i}`, value: makePayload(i, SIZE) });

    const batchFull = buffer.length >= BATCH_SIZE;
    const lingerExpired = LINGER_MS > 0 && Date.now() - bufferOpenedAt >= LINGER_MS;
    if (batchFull || lingerExpired) {
      await flush();
    }

    if (RATE > 0) {
      await sleep(1000 / RATE);
    }
  }
  await flush();

  const elapsedMs = Date.now() - start;
  const elapsedSec = elapsedMs / 1000;
  const mb = bytesSent / (1024 * 1024);

  console.log(`--- load-producer: COUNT=${COUNT} SIZE=${SIZE}B BATCH_SIZE=${BATCH_SIZE} LINGER_MS=${LINGER_MS} RATE=${RATE || "unlimited"} COMPRESSION=${COMPRESSION} ACKS=${ACKS} ---`);
  console.log(`sent ${COUNT} messages in ${requestCount} produce requests (${(COUNT / requestCount).toFixed(1)} msgs/request)`);
  console.log(`elapsed: ${elapsedMs}ms | ${(COUNT / elapsedSec).toFixed(0)} msgs/sec | ${(mb / elapsedSec).toFixed(2)} MB/sec (uncompressed app-level bytes)`);
  if (codecStats.rawBytes > 0) {
    const ratio = codecStats.rawBytes / codecStats.compressedBytes;
    console.log(`compression: ${codecStats.rawBytes} -> ${codecStats.compressedBytes} bytes (${ratio.toFixed(2)}x ratio), ${codecStats.compressMs}ms total CPU time compressing`);
  }

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
