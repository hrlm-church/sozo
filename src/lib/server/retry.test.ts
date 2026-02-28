import { describe, it, expect, vi } from "vitest";
import { withRetry, isTransientSqlError } from "./retry";

describe("isTransientSqlError", () => {
  it("detects ECONNRESET", () => {
    expect(isTransientSqlError(new Error("read ECONNRESET"))).toBe(true);
  });

  it("detects deadlock", () => {
    expect(isTransientSqlError(new Error("Transaction was deadlocked"))).toBe(true);
  });

  it("detects timeout", () => {
    expect(isTransientSqlError(new Error("connect ETIMEOUT"))).toBe(true);
  });

  it("detects connection closed", () => {
    expect(isTransientSqlError(new Error("connection is closed"))).toBe(true);
  });

  it("detects SQL error codes", () => {
    expect(isTransientSqlError({ number: 1205, message: "deadlock" })).toBe(true);
    expect(isTransientSqlError({ number: 40613, message: "db unavailable" })).toBe(true);
    expect(isTransientSqlError({ number: 40501, message: "service busy" })).toBe(true);
  });

  it("detects Azure SQL transient codes", () => {
    expect(isTransientSqlError({ number: 49918, message: "not enough resources" })).toBe(true);
  });

  it("returns false for non-transient errors", () => {
    expect(isTransientSqlError(new Error("Invalid column name 'foo'"))).toBe(false);
    expect(isTransientSqlError(new Error("Syntax error near 'SELECT'"))).toBe(false);
    expect(isTransientSqlError(null)).toBe(false);
  });

  it("returns false for unknown error numbers", () => {
    expect(isTransientSqlError({ number: 207, message: "invalid column" })).toBe(false);
  });
});

describe("withRetry", () => {
  it("returns immediately on success", async () => {
    const fn = vi.fn().mockResolvedValue("ok");
    const result = await withRetry(fn);
    expect(result).toBe("ok");
    expect(fn).toHaveBeenCalledTimes(1);
  });

  it("retries on transient error and succeeds", async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(new Error("read ECONNRESET"))
      .mockResolvedValue("recovered");

    const onRetry = vi.fn();
    const result = await withRetry(fn, {
      maxAttempts: 3,
      baseDelayMs: 10,
      onRetry,
    });

    expect(result).toBe("recovered");
    expect(fn).toHaveBeenCalledTimes(2);
    expect(onRetry).toHaveBeenCalledTimes(1);
    expect(onRetry).toHaveBeenCalledWith(1, expect.any(Error), expect.any(Number));
  });

  it("throws after max retries exhausted", async () => {
    const fn = vi.fn().mockRejectedValue(new Error("connection is closed"));
    const onRetry = vi.fn();

    await expect(
      withRetry(fn, { maxAttempts: 3, baseDelayMs: 10, onRetry }),
    ).rejects.toThrow("connection is closed");

    expect(fn).toHaveBeenCalledTimes(3);
    expect(onRetry).toHaveBeenCalledTimes(2); // retries between attempts 1-2 and 2-3
  });

  it("does not retry non-transient errors", async () => {
    const fn = vi.fn().mockRejectedValue(new Error("Syntax error"));
    const onRetry = vi.fn();

    await expect(
      withRetry(fn, { maxAttempts: 3, baseDelayMs: 10, onRetry }),
    ).rejects.toThrow("Syntax error");

    expect(fn).toHaveBeenCalledTimes(1);
    expect(onRetry).not.toHaveBeenCalled();
  });

  it("respects custom isRetryable predicate", async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(new Error("custom retryable"))
      .mockResolvedValue("ok");

    const result = await withRetry(fn, {
      maxAttempts: 3,
      baseDelayMs: 10,
      isRetryable: (err) => err instanceof Error && err.message.includes("custom retryable"),
    });

    expect(result).toBe("ok");
    expect(fn).toHaveBeenCalledTimes(2);
  });

  it("applies exponential backoff", async () => {
    const fn = vi.fn()
      .mockRejectedValueOnce(new Error("ECONNRESET"))
      .mockRejectedValueOnce(new Error("ECONNRESET"))
      .mockResolvedValue("ok");

    const delays: number[] = [];
    await withRetry(fn, {
      maxAttempts: 3,
      baseDelayMs: 100,
      maxDelayMs: 5000,
      onRetry: (_attempt, _err, delay) => delays.push(delay),
    });

    // First retry: ~100-200ms (100 * 2^0 + jitter)
    // Second retry: ~200-300ms (100 * 2^1 + jitter)
    expect(delays[0]).toBeGreaterThanOrEqual(100);
    expect(delays[0]).toBeLessThan(300);
    expect(delays[1]).toBeGreaterThanOrEqual(200);
    expect(delays[1]).toBeLessThan(500);
  });
});
