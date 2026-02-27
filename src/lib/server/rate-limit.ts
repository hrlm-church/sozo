/**
 * In-memory sliding window rate limiter.
 * For production multi-instance deployment, swap with @upstash/ratelimit + Redis.
 */

interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetMs: number;
}

const windows = new Map<string, number[]>();

/**
 * Check and consume a rate limit token for the given key.
 * Returns whether the request is allowed and how many tokens remain.
 */
export function checkRateLimit(
  key: string,
  limit: number,
  windowMs = 60_000,
): RateLimitResult {
  const now = Date.now();
  const windowStart = now - windowMs;

  let timestamps = windows.get(key) ?? [];
  timestamps = timestamps.filter((t) => t > windowStart);

  if (timestamps.length >= limit) {
    return {
      allowed: false,
      remaining: 0,
      resetMs: Math.max(0, timestamps[0] + windowMs - now),
    };
  }

  timestamps.push(now);
  windows.set(key, timestamps);

  // Periodic cleanup of stale keys (~1% chance per call)
  if (Math.random() < 0.01 && windows.size > 1000) {
    for (const [k, v] of windows) {
      if (v.every((t) => t < windowStart)) windows.delete(k);
    }
  }

  return {
    allowed: true,
    remaining: limit - timestamps.length,
    resetMs: windowMs,
  };
}

/** Preset rate limit configurations */
export const RATE_LIMITS = {
  /** LLM chat: 10 requests per minute per user */
  chat: { limit: 10, windowMs: 60_000 },
  /** General API reads: 60 requests per minute per user */
  api: { limit: 60, windowMs: 60_000 },
  /** Write operations: 20 requests per minute per user */
  write: { limit: 20, windowMs: 60_000 },
} as const;

/** Build rate limit response headers */
export function rateLimitHeaders(result: RateLimitResult, limit: number): Record<string, string> {
  return {
    "X-RateLimit-Limit": String(limit),
    "X-RateLimit-Remaining": String(result.remaining),
    "X-RateLimit-Reset": String(Math.ceil(result.resetMs / 1000)),
  };
}
