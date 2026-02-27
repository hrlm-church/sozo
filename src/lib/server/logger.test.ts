import { describe, it, expect, vi, beforeEach } from "vitest";
import { createLogger, generateRequestId } from "./logger";

describe("createLogger", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("emits JSON with timestamp, level, and message", () => {
    const spy = vi.spyOn(console, "log").mockImplementation(() => {});
    const log = createLogger();
    log.info("test message");

    expect(spy).toHaveBeenCalledOnce();
    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed.level).toBe("info");
    expect(parsed.message).toBe("test message");
    expect(parsed.timestamp).toBeDefined();
  });

  it("includes bound context (requestId, userId)", () => {
    const spy = vi.spyOn(console, "log").mockImplementation(() => {});
    const log = createLogger({ requestId: "abc123", userId: "user@test.com" });
    log.info("with context");

    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed.requestId).toBe("abc123");
    expect(parsed.userId).toBe("user@test.com");
  });

  it("merges extra metadata", () => {
    const spy = vi.spyOn(console, "log").mockImplementation(() => {});
    const log = createLogger();
    log.info("event", { route: "/api/chat", durationMs: 150 });

    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed.route).toBe("/api/chat");
    expect(parsed.durationMs).toBe(150);
  });

  it("uses console.error for error level", () => {
    const spy = vi.spyOn(console, "error").mockImplementation(() => {});
    const log = createLogger();
    log.error("something broke");

    expect(spy).toHaveBeenCalledOnce();
    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed.level).toBe("error");
  });

  it("uses console.warn for warn level", () => {
    const spy = vi.spyOn(console, "warn").mockImplementation(() => {});
    const log = createLogger();
    log.warn("heads up");

    expect(spy).toHaveBeenCalledOnce();
    const parsed = JSON.parse(spy.mock.calls[0][0] as string);
    expect(parsed.level).toBe("warn");
  });
});

describe("generateRequestId", () => {
  it("returns an 8-character string", () => {
    const id = generateRequestId();
    expect(id).toHaveLength(8);
  });

  it("generates unique IDs", () => {
    const ids = new Set(Array.from({ length: 100 }, () => generateRequestId()));
    expect(ids.size).toBe(100);
  });
});
