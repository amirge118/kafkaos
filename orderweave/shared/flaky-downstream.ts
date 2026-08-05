// Unchanged from Stage 22 — the exact same deterministic failure simulator
// (a payment gateway / third-party API stand-in), reused verbatim so the
// same real, already-proven retry/DLQ behavior carries over unmodified:
//   "order-transient-fail-N" -> fails the first N-1 calls, succeeds on the Nth
//   "order-permanent-fail"   -> always fails (exhausts every retry, for real)
//   anything else            -> succeeds immediately
const attemptCounts = new Map<string, number>();

export class DownstreamError extends Error {}

export async function callFlakyDownstream(orderId: string): Promise<void> {
  const attempt = (attemptCounts.get(orderId) ?? 0) + 1;
  attemptCounts.set(orderId, attempt);

  if (orderId.includes("permanent-fail")) {
    throw new DownstreamError(`downstream rejected ${orderId} (attempt ${attempt}) — simulated permanent outage`);
  }

  const match = orderId.match(/transient-fail-(\d+)/);
  if (match) {
    const requiredAttempts = Number(match[1]);
    if (attempt < requiredAttempts) {
      throw new DownstreamError(`downstream timeout for ${orderId} (attempt ${attempt}/${requiredAttempts})`);
    }
  }
  // Otherwise: succeeds.
}
