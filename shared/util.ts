// A malformed message ("poison pill") should never be allowed to crash a
// consumer outright — that stalls every message behind it too. Parse
// defensively, log, and skip instead.
export function safeParseJson<T>(raw: string | undefined, context: string): T | null {
  if (raw === undefined) return null;
  try {
    return JSON.parse(raw) as T;
  } catch (err) {
    console.error(`[${context}] skipping unparseable message: ${JSON.stringify(raw)} (${(err as Error).message})`);
    return null;
  }
}
