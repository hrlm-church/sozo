/**
 * Retry with exponential backoff and jitter for transient errors.
 */

export interface RetryOptions {
  /** Maximum number of attempts (default: 3) */
  maxAttempts?: number;
  /** Base delay in ms (default: 200) */
  baseDelayMs?: number;
  /** Maximum delay in ms (default: 5000) */
  maxDelayMs?: number;
  /** Predicate to determine if an error is retryable (default: isTransientSqlError) */
  isRetryable?: (error: unknown) => boolean;
  /** Callback on each retry (for logging) */
  onRetry?: (attempt: number, error: unknown, delayMs: number) => void;
}

/**
 * Known transient SQL Server error codes/messages.
 * These are safe to retry because they indicate temporary infrastructure issues.
 */
const TRANSIENT_SQL_PATTERNS = [
  /ECONNRESET/i,
  /ECONNREFUSED/i,
  /ETIMEOUT/i,
  /ESOCKET/i,
  /Connection lost/i,
  /Connection was reset/i,
  /connection is closed/i,
  /deadlocked/i,
  /lock request time out/i,
  /resource busy/i,
  /server is not ready/i,
  /Too many requests/i,
  /The service is currently busy/i,
  /transport-level error/i,
  /A network-related or instance-specific error/i,
  /Login failed for user/i, // transient Azure SQL login issues
];

/** SQL Server transient error numbers */
const TRANSIENT_SQL_CODES = new Set([
  -1,      // General network error
  -2,      // Timeout
  233,     // Connection broken
  1205,    // Deadlock
  10053,   // Transport error
  10054,   // Connection reset
  10060,   // Timeout
  40197,   // Service error processing request
  40501,   // Service busy
  40544,   // Database reached size quota
  40549,   // Long-running transaction
  40613,   // Database unavailable
  49918,   // Not enough resources
  49919,   // Cannot process create/update request
  49920,   // Too many operations
]);

export function isTransientSqlError(error: unknown): boolean {
  if (!error) return false;

  // Check error number (mssql provides this)
  if (typeof error === "object" && error !== null) {
    const errObj = error as Record<string, unknown>;
    if (typeof errObj.number === "number" && TRANSIENT_SQL_CODES.has(errObj.number)) {
      return true;
    }
    if (typeof errObj.code === "string" && TRANSIENT_SQL_PATTERNS.some((p) => p.test(errObj.code as string))) {
      return true;
    }
  }

  // Check error message
  const msg = error instanceof Error ? error.message : String(error);
  return TRANSIENT_SQL_PATTERNS.some((p) => p.test(msg));
}

/**
 * Calculate delay with exponential backoff + jitter.
 * delay = min(maxDelay, baseDelay * 2^attempt) + random jitter
 */
function calculateDelay(attempt: number, baseDelayMs: number, maxDelayMs: number): number {
  const exponential = Math.min(maxDelayMs, baseDelayMs * Math.pow(2, attempt));
  const jitter = Math.random() * baseDelayMs; // 0 to baseDelay jitter
  return exponential + jitter;
}

/**
 * Execute an async function with retry on transient errors.
 */
export async function withRetry<T>(
  fn: () => Promise<T>,
  opts: RetryOptions = {},
): Promise<T> {
  const {
    maxAttempts = 3,
    baseDelayMs = 200,
    maxDelayMs = 5_000,
    isRetryable = isTransientSqlError,
    onRetry,
  } = opts;

  let lastError: unknown;

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;

      // Don't retry the last attempt or non-retryable errors
      if (attempt >= maxAttempts - 1 || !isRetryable(error)) {
        throw error;
      }

      const delay = calculateDelay(attempt, baseDelayMs, maxDelayMs);
      onRetry?.(attempt + 1, error, delay);
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
  }

  // Should never reach here, but TypeScript needs this
  throw lastError;
}
