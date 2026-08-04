import { CompressionCodecs, CompressionTypes } from "kafkajs";
import { kafka } from "../shared/kafka";
import { ZstdCodec } from "./zstd-codec";
import { Lz4Codec } from "./lz4-codec";

// Must match the producer's codec registration — kafkajs needs these
// registered to decode fetched messages compressed with ZSTD/LZ4, not just
// to produce them (learned the hard way: this crashed the consumer with
// "ZSTD compression not implemented" until added — see NOTES.md Stage 13).
CompressionCodecs[CompressionTypes.ZSTD] = ZstdCodec;
CompressionCodecs[CompressionTypes.LZ4] = Lz4Codec;

const TOPIC = process.env.TOPIC ?? "load-test";
// How many messages to wait for before reporting. Defaults to draining
// whatever's on the topic right now via an idle timeout instead of a fixed
// count, since a fresh consumer group doesn't know the producer's COUNT.
const EXPECT = Number(process.env.EXPECT ?? 0);
// Stop once no message has arrived for this long (catches the "drained the
// backlog" case when EXPECT isn't set). Default is generous because a fresh
// consumer group's initial join/rebalance alone routinely takes ~3s before
// any message can arrive at all (see NOTES.md Stage 13).
const IDLE_TIMEOUT_MS = Number(process.env.IDLE_TIMEOUT_MS ?? 10000);

async function run() {
  const groupId = `load-test-consumer-${Date.now()}`;
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  let count = 0;
  let bytes = 0;
  let firstMessageAt = 0;
  let lastMessageAt = 0;
  let idleTimer: NodeJS.Timeout;

  await new Promise<void>((resolve) => {
    function resetIdleTimer() {
      clearTimeout(idleTimer);
      idleTimer = setTimeout(() => resolve(), IDLE_TIMEOUT_MS);
    }
    resetIdleTimer();

    consumer
      .run({
        eachMessage: async ({ message }) => {
          const now = Date.now();
          if (count === 0) firstMessageAt = now;
          lastMessageAt = now;
          count++;
          bytes += message.value ? message.value.length : 0;
          resetIdleTimer();
          if (EXPECT > 0 && count >= EXPECT) {
            clearTimeout(idleTimer);
            resolve();
          }
        },
      })
      .catch((err) => console.error(err));
  });

  const elapsedMs = Math.max(1, lastMessageAt - firstMessageAt);
  const elapsedSec = elapsedMs / 1000;
  const mb = bytes / (1024 * 1024);

  console.log(`--- load-consumer: received ${count} messages from "${TOPIC}" ---`);
  console.log(`elapsed (first->last message): ${elapsedMs}ms | ${(count / elapsedSec).toFixed(0)} msgs/sec | ${(mb / elapsedSec).toFixed(2)} MB/sec`);

  await consumer.disconnect();
  process.exit(0);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
