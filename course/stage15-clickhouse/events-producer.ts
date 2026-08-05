import { randomUUID } from "crypto";
import { kafka } from "../../shared/kafka";

const TOPIC = process.env.TOPIC ?? "analytics-events";
const COUNT = Number(process.env.COUNT ?? 2_000_000);
const BATCH_SIZE = Number(process.env.BATCH_SIZE ?? 5000);
// Spread event_time across a real window in the past instead of everything
// being "now" — makes the time-based/hourly aggregation queries meaningful
// instead of every row landing in the same bucket.
const WINDOW_DAYS = Number(process.env.WINDOW_DAYS ?? 7);

const EVENT_TYPES = ["view", "add_to_cart", "purchase"] as const;
// Not uniform — funnel drop-off is realistic: most traffic is views, a
// fraction adds to cart, a smaller fraction actually purchases.
const EVENT_TYPE_WEIGHTS = [0.7, 0.22, 0.08];

const CATEGORIES = ["electronics", "books", "home", "clothing", "toys", "sports"];
const COUNTRIES = ["US", "IL", "DE", "FR", "IN", "BR", "GB", "CA"];
const PRODUCT_CARDINALITY = 20_000;

function weightedPick<T>(items: readonly T[], weights: number[]): T {
  const r = Math.random();
  let acc = 0;
  for (let i = 0; i < items.length; i++) {
    acc += weights[i];
    if (r < acc) return items[i];
  }
  return items[items.length - 1];
}

function makeEvent() {
  const eventType = weightedPick(EVENT_TYPES, EVENT_TYPE_WEIGHTS);
  const category = CATEGORIES[Math.floor(Math.random() * CATEGORIES.length)];
  const amount = eventType === "purchase" ? Math.round((5 + Math.random() * 495) * 100) / 100 : 0;
  const eventTime = Date.now() - Math.floor(Math.random() * WINDOW_DAYS * 24 * 60 * 60 * 1000);

  return {
    event_id: randomUUID(),
    event_time: eventTime,
    event_type: eventType,
    user_id: `user-${Math.floor(Math.random() * 500_000)}`,
    product_id: `prod-${Math.floor(Math.random() * PRODUCT_CARDINALITY)}`,
    category,
    country: COUNTRIES[Math.floor(Math.random() * COUNTRIES.length)],
    amount,
  };
}

async function run() {
  const producer = kafka.producer();
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

  const start = Date.now();
  for (let i = 0; i < COUNT; i++) {
    const event = makeEvent();
    buffer.push({ key: event.user_id, value: JSON.stringify(event) });
    if (buffer.length >= BATCH_SIZE) await flush();
    if (i > 0 && i % 200_000 === 0) {
      console.log(`  ...${i.toLocaleString()} / ${COUNT.toLocaleString()} sent`);
    }
  }
  await flush();

  const elapsedSec = (Date.now() - start) / 1000;
  console.log(`--- events-producer: sent ${COUNT.toLocaleString()} events to "${TOPIC}" in ${requestCount} requests ---`);
  console.log(`elapsed: ${elapsedSec.toFixed(1)}s | ${(COUNT / elapsedSec).toFixed(0)} events/sec`);

  await producer.disconnect();
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
