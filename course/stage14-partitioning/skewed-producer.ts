import { Partitioners } from "kafkajs";
import { kafka } from "../../shared/kafka";

const TOPIC = process.env.TOPIC ?? "hot-key-test";
const COUNT = Number(process.env.COUNT ?? 50_000);
// Fraction of traffic that belongs to one single hot key (e.g. one very
// active customer/tenant/device) — everything else is spread thinly across
// many distinct keys, the realistic shape of most skewed real-world traffic.
const HOT_KEY_SHARE = Number(process.env.HOT_KEY_SHARE ?? 0.8);
const HOT_KEY = process.env.HOT_KEY ?? "customer-VIP";
const NORMAL_KEY_CARDINALITY = Number(process.env.NORMAL_KEY_CARDINALITY ?? 5000);
// The fix under test: split the one hot key into N sub-keys
// (`customer-VIP#0` .. `customer-VIP#N-1`), distributed round-robin, so the
// default (murmur2-hash) partitioner spreads them across up to N different
// partitions instead of hashing them all to one. 0 = disabled (baseline).
const SALT_BUCKETS = Number(process.env.SALT_BUCKETS ?? 0);
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 500);

function keyFor(i: number): string {
  if (Math.random() < HOT_KEY_SHARE) {
    return SALT_BUCKETS > 0 ? `${HOT_KEY}#${i % SALT_BUCKETS}` : HOT_KEY;
  }
  return `customer-${i % NORMAL_KEY_CARDINALITY}`;
}

async function run() {
  const producer = kafka.producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();

  let buffer: { key: string; value: string }[] = [];
  let requestCount = 0;

  async function flush() {
    if (buffer.length === 0) return;
    const messages = buffer;
    buffer = [];
    requestCount++;
    await producer.send({ topic: TOPIC, messages });
  }

  for (let i = 0; i < COUNT; i++) {
    buffer.push({ key: keyFor(i), value: JSON.stringify({ i, ts: Date.now() }) });
    if (buffer.length >= BATCH_SIZE) await flush();
  }
  await flush();

  console.log(
    `--- skewed-producer: sent ${COUNT} messages to "${TOPIC}" in ${requestCount} requests ` +
      `(HOT_KEY_SHARE=${HOT_KEY_SHARE}, SALT_BUCKETS=${SALT_BUCKETS || "disabled"}) ---`
  );

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
