import { describe, it, expect, beforeEach } from "vitest";
import {
  isAvailable,
  recordSuccess,
  recordFailure,
  getCircuitState,
  allCircuitsOpen,
  resetAllCircuits,
} from "./circuit-breaker";

const OPTS = {
  failureThreshold: 3,
  failureWindowMs: 60_000,
  cooldownMs: 100, // short cooldown for tests
};

describe("circuit-breaker", () => {
  beforeEach(() => {
    resetAllCircuits();
  });

  it("starts in CLOSED state and allows requests", () => {
    expect(isAvailable("openai", OPTS)).toBe(true);
    expect(getCircuitState("openai").state).toBe("CLOSED");
  });

  it("stays CLOSED below failure threshold", () => {
    recordFailure("openai", "timeout", OPTS);
    recordFailure("openai", "timeout", OPTS);
    expect(isAvailable("openai", OPTS)).toBe(true);
    expect(getCircuitState("openai").state).toBe("CLOSED");
  });

  it("opens after reaching failure threshold", () => {
    recordFailure("openai", "err1", OPTS);
    recordFailure("openai", "err2", OPTS);
    recordFailure("openai", "err3", OPTS);
    expect(isAvailable("openai", OPTS)).toBe(false);
    expect(getCircuitState("openai").state).toBe("OPEN");
    expect(getCircuitState("openai").lastError).toBe("err3");
  });

  it("blocks requests while OPEN", () => {
    for (let i = 0; i < 3; i++) recordFailure("openai", "err", OPTS);
    expect(isAvailable("openai", OPTS)).toBe(false);
    expect(isAvailable("openai", OPTS)).toBe(false);
  });

  it("transitions to HALF_OPEN after cooldown", async () => {
    for (let i = 0; i < 3; i++) recordFailure("openai", "err", OPTS);
    expect(isAvailable("openai", OPTS)).toBe(false);

    // Wait for cooldown
    await new Promise((r) => setTimeout(r, 150));
    expect(isAvailable("openai", OPTS)).toBe(true);
    expect(getCircuitState("openai").state).toBe("HALF_OPEN");
  });

  it("closes on success in HALF_OPEN state", async () => {
    for (let i = 0; i < 3; i++) recordFailure("openai", "err", OPTS);
    await new Promise((r) => setTimeout(r, 150));
    isAvailable("openai", OPTS); // trigger HALF_OPEN

    recordSuccess("openai");
    expect(getCircuitState("openai").state).toBe("CLOSED");
    expect(getCircuitState("openai").failures).toBe(0);
  });

  it("re-opens on failure in HALF_OPEN state", async () => {
    for (let i = 0; i < 3; i++) recordFailure("openai", "err", OPTS);
    await new Promise((r) => setTimeout(r, 150));
    isAvailable("openai", OPTS); // trigger HALF_OPEN

    recordFailure("openai", "still failing", OPTS);
    expect(getCircuitState("openai").state).toBe("OPEN");
  });

  it("resets on success during normal operation", () => {
    recordFailure("openai", "err", OPTS);
    recordFailure("openai", "err", OPTS);
    recordSuccess("openai");
    expect(getCircuitState("openai").failures).toBe(0);
  });

  it("isolates circuits by name", () => {
    for (let i = 0; i < 3; i++) recordFailure("openai", "err", OPTS);
    expect(isAvailable("openai", OPTS)).toBe(false);
    expect(isAvailable("azure", OPTS)).toBe(true);
  });

  it("allCircuitsOpen returns true only when all are open", () => {
    for (let i = 0; i < 3; i++) {
      recordFailure("openai", "err", OPTS);
      recordFailure("azure", "err", OPTS);
    }
    expect(allCircuitsOpen(["openai", "azure"], OPTS)).toBe(true);
    expect(allCircuitsOpen(["openai", "claude"], OPTS)).toBe(false);
  });

  it("allCircuitsOpen returns false for empty list", () => {
    expect(allCircuitsOpen([], OPTS)).toBe(false);
  });

  it("expires old failures outside the window", () => {
    const shortWindow = { ...OPTS, failureWindowMs: 50 };
    recordFailure("openai", "err", shortWindow);
    recordFailure("openai", "err", shortWindow);

    // Wait for failures to expire
    return new Promise<void>((resolve) => {
      setTimeout(() => {
        recordFailure("openai", "err", shortWindow);
        // Only 1 recent failure — should still be CLOSED
        expect(getCircuitState("openai").state).toBe("CLOSED");
        resolve();
      }, 100);
    });
  });
});
