import { safeParseJson } from "../../shared/util";

// Real, confirmed-by-inspection quirk of this specific pipeline: the
// outbox `payload` column is JSONB, written via our own JSON.stringify()
// (so Postgres stores it as a JSON *string* value, not a native JSON
// object via a typed parameter). Debezium's EventRouter SMT then treats
// that column's value as a Connect STRING type, and JsonConverter
// (schemas.enable=false) serializes a STRING value by JSON-encoding it —
// so the real Kafka message value is a JSON string literal containing
// another JSON string, not a plain object. Confirmed directly: the raw
// bytes on `orders` decode once to a string, and only decode a second
// time to the actual `{ type, order, traceContext }` object. Every
// consumer downstream of a Debezium-produced topic needs this, not just
// a single ad-hoc parse.
export function parseOutboxMessage<T>(raw: string | undefined, context: string): T | null {
  const once = safeParseJson<unknown>(raw, context);
  if (once === null) return null;
  if (typeof once === "string") {
    return safeParseJson<T>(once, context);
  }
  // Tolerate a future config change that stops double-encoding (e.g. if
  // the payload column type or converter changes) without silently
  // breaking every consumer.
  return once as T;
}
