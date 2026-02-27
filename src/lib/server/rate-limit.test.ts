import { describe, it, expect, beforeEach, vi } from "vitest";
import { checkRateLimit, rateLimitHeaders, RATE_LIMITS } from "./rate-limit";

describe("checkRateLimit", () => {
  beforeEach(() => {
    // Reset the module to clear the in-memory windows map between tests
    vi.resetModules();
  });

  it("allows requests under the limit", async () => {
    const { checkRateLimit: rl } = await import("./rate-limit");
    const key = `test-allow-${Date.now()}`;
    const result = rl(key, 5, 60_000);
    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(4);
  });

  it("decrements remaining count on each call", async () => {
    const { checkRateLimit: rl } = await import("./rate-limit");
    const key = `test-decrement-${Date.now()}`;
    rl(key, 5, 60_000);
    rl(key, 5, 60_000);
    const r3 = rl(key, 5, 60_000);
    expect(r3.allowed).toBe(true);
    expect(r3.remaining).toBe(2);
  });

  it("blocks requests at the limit", async () => {
    const { checkRateLimit: rl } = await import("./rate-limit");
    const key = `test-block-${Date.now()}`;
    for (let i = 0; i < 3; i++) rl(key, 3, 60_000);
    const blocked = rl(key, 3, 60_000);
    expect(blocked.allowed).toBe(false);
    expect(blocked.remaining).toBe(0);
    expect(blocked.resetMs).toBeGreaterThan(0);
  });

  it("expires old timestamps outside the window", async () => {
    const { checkRateLimit: rl } = await import("./rate-limit");
    const key = `test-expire-${Date.now()}`;
    // Fill up the limit
    for (let i = 0; i < 3; i++) rl(key, 3, 10); // 10ms window

    // Wait for window to expire
    await new Promise((r) => setTimeout(r, 20));

    const result = rl(key, 3, 10);
    expect(result.allowed).toBe(true);
    expect(result.remaining).toBe(2);
  });

  it("isolates different keys", async () => {
    const { checkRateLimit: rl } = await import("./rate-limit");
    const keyA = `test-isolate-a-${Date.now()}`;
    const keyB = `test-isolate-b-${Date.now()}`;

    // Exhaust key A
    for (let i = 0; i < 3; i++) rl(keyA, 3, 60_000);
    expect(rl(keyA, 3, 60_000).allowed).toBe(false);

    // Key B should still be available
    expect(rl(keyB, 3, 60_000).allowed).toBe(true);
  });
});

describe("rateLimitHeaders", () => {
  it("returns correct header values", () => {
    const headers = rateLimitHeaders(
      { allowed: true, remaining: 7, resetMs: 45_000 },
      10,
    );
    expect(headers["X-RateLimit-Limit"]).toBe("10");
    expect(headers["X-RateLimit-Remaining"]).toBe("7");
    expect(headers["X-RateLimit-Reset"]).toBe("45");
  });
});

describe("RATE_LIMITS presets", () => {
  it("has chat, api, and write presets", () => {
    expect(RATE_LIMITS.chat).toEqual({ limit: 10, windowMs: 60_000 });
    expect(RATE_LIMITS.api).toEqual({ limit: 60, windowMs: 60_000 });
    expect(RATE_LIMITS.write).toEqual({ limit: 20, windowMs: 60_000 });
  });
});
