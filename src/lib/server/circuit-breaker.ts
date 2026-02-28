/**
 * Circuit breaker for AI provider calls.
 *
 * States:
 *   CLOSED   — normal operation, requests pass through
 *   OPEN     — provider is failing, skip immediately
 *   HALF_OPEN — cooldown expired, allow one test request
 *
 * Transitions:
 *   CLOSED → OPEN     when failureCount >= threshold within the window
 *   OPEN → HALF_OPEN  when cooldownMs elapses
 *   HALF_OPEN → CLOSED on success
 *   HALF_OPEN → OPEN   on failure (reset cooldown)
 */

export interface CircuitBreakerOptions {
  /** Number of failures to trigger open state (default: 3) */
  failureThreshold?: number;
  /** Time window for counting failures in ms (default: 60_000) */
  failureWindowMs?: number;
  /** How long to stay open before trying again in ms (default: 30_000) */
  cooldownMs?: number;
}

type CircuitState = "CLOSED" | "OPEN" | "HALF_OPEN";

interface CircuitEntry {
  state: CircuitState;
  failures: number[];        // timestamps of recent failures
  openedAt: number | null;   // when the circuit opened
  lastFailureMsg: string;
}

const DEFAULT_OPTIONS: Required<CircuitBreakerOptions> = {
  failureThreshold: 3,
  failureWindowMs: 60_000,
  cooldownMs: 30_000,
};

const circuits = new Map<string, CircuitEntry>();

function getCircuit(name: string): CircuitEntry {
  let entry = circuits.get(name);
  if (!entry) {
    entry = { state: "CLOSED", failures: [], openedAt: null, lastFailureMsg: "" };
    circuits.set(name, entry);
  }
  return entry;
}

/**
 * Check if a provider is available (circuit not open).
 * Returns true if the request should proceed, false if it should be skipped.
 */
export function isAvailable(
  name: string,
  opts: CircuitBreakerOptions = {},
): boolean {
  const { cooldownMs } = { ...DEFAULT_OPTIONS, ...opts };
  const circuit = getCircuit(name);

  if (circuit.state === "CLOSED") return true;

  if (circuit.state === "OPEN") {
    // Check if cooldown has expired → transition to HALF_OPEN
    if (circuit.openedAt && Date.now() - circuit.openedAt >= cooldownMs) {
      circuit.state = "HALF_OPEN";
      return true; // allow one test request
    }
    return false; // still cooling down
  }

  // HALF_OPEN — allow the test request
  return true;
}

/**
 * Record a successful call. Resets the circuit to CLOSED.
 */
export function recordSuccess(name: string): void {
  const circuit = getCircuit(name);
  circuit.state = "CLOSED";
  circuit.failures = [];
  circuit.openedAt = null;
  circuit.lastFailureMsg = "";
}

/**
 * Record a failed call. May trip the circuit to OPEN.
 */
export function recordFailure(
  name: string,
  errorMsg: string,
  opts: CircuitBreakerOptions = {},
): void {
  const { failureThreshold, failureWindowMs, cooldownMs } = {
    ...DEFAULT_OPTIONS,
    ...opts,
  };
  const circuit = getCircuit(name);
  const now = Date.now();

  circuit.lastFailureMsg = errorMsg;

  if (circuit.state === "HALF_OPEN") {
    // Test request failed — reopen
    circuit.state = "OPEN";
    circuit.openedAt = now;
    return;
  }

  // CLOSED state — add failure and check threshold
  circuit.failures.push(now);
  // Trim old failures outside the window
  circuit.failures = circuit.failures.filter(
    (t) => now - t < failureWindowMs,
  );

  if (circuit.failures.length >= failureThreshold) {
    circuit.state = "OPEN";
    circuit.openedAt = now;
  }
}

/**
 * Get the current state of a circuit (for monitoring/logging).
 */
export function getCircuitState(name: string): {
  state: CircuitState;
  failures: number;
  lastError: string;
} {
  const circuit = getCircuit(name);
  return {
    state: circuit.state,
    failures: circuit.failures.length,
    lastError: circuit.lastFailureMsg,
  };
}

/**
 * Check if ALL circuits in a list are open (all providers down).
 */
export function allCircuitsOpen(
  names: string[],
  opts: CircuitBreakerOptions = {},
): boolean {
  return names.length > 0 && names.every((n) => !isAvailable(n, opts));
}

/**
 * Reset a circuit (for testing or manual recovery).
 */
export function resetCircuit(name: string): void {
  circuits.delete(name);
}

/**
 * Reset all circuits.
 */
export function resetAllCircuits(): void {
  circuits.clear();
}
