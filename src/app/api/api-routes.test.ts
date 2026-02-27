/**
 * Tests for API route handlers: auth enforcement, Zod validation, parameterized SQL.
 * Each route is tested for: (1) 401 without auth, (2) 400 on bad input, (3) 200 on valid input.
 */
import { describe, it, expect, vi, beforeEach } from "vitest";

// ── Shared mocks ────────────────────────────────────────────────────
const mockExecuteSqlSafe = vi.fn();
const mockWithTransaction = vi.fn();
let mockSessionEmail: string | null = "test@example.com";

vi.mock("@/lib/server/session", () => ({
  getSessionEmail: vi.fn(() => Promise.resolve(mockSessionEmail)),
}));

vi.mock("@/lib/server/sql-client", () => ({
  executeSqlSafe: (...args: unknown[]) => mockExecuteSqlSafe(...args),
  withTransaction: (...args: unknown[]) => mockWithTransaction(...args),
}));

function makeRequest(body: unknown): Request {
  return new Request("http://localhost:3000/api/test", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

// ── Conversation Save ───────────────────────────────────────────────
describe("POST /api/conversation/save", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionEmail = "test@example.com";
    mockWithTransaction.mockImplementation(async (fn: Function) => fn(vi.fn().mockResolvedValue([])));
  });

  it("returns 401 when not authenticated", async () => {
    mockSessionEmail = null;
    const { POST } = await import("./conversation/save/route");
    const res = await POST(makeRequest({ conversationId: "c1", messages: [{ id: "m1", role: "user", content: "hi" }] }));
    expect(res.status).toBe(401);
  });

  it("returns 400 for missing conversationId", async () => {
    const { POST } = await import("./conversation/save/route");
    const res = await POST(makeRequest({ messages: [{ id: "m1", role: "user", content: "hi" }] }));
    expect(res.status).toBe(400);
  });

  it("returns 400 for empty messages array", async () => {
    const { POST } = await import("./conversation/save/route");
    const res = await POST(makeRequest({ conversationId: "c1", messages: [] }));
    expect(res.status).toBe(400);
  });

  it("returns 200 with valid payload", async () => {
    const { POST } = await import("./conversation/save/route");
    const res = await POST(
      makeRequest({
        conversationId: "c1",
        title: "Test Chat",
        messages: [{ id: "m1", role: "user", content: "hello" }],
      }),
    );
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json.saved).toBe(true);
    expect(json.id).toBe("c1");
  });

  it("uses transaction for atomic save", async () => {
    const { POST } = await import("./conversation/save/route");
    await POST(
      makeRequest({
        conversationId: "c1",
        messages: [{ id: "m1", role: "user", content: "hello" }],
      }),
    );
    expect(mockWithTransaction).toHaveBeenCalledOnce();
  });
});

// ── Conversation List ───────────────────────────────────────────────
describe("GET /api/conversation/list", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionEmail = "test@example.com";
    mockExecuteSqlSafe.mockResolvedValue({ ok: true, rows: [{ id: "c1", title: "Chat" }] });
  });

  it("returns 401 when not authenticated", async () => {
    mockSessionEmail = null;
    const { GET } = await import("./conversation/list/route");
    const res = await GET();
    expect(res.status).toBe(401);
  });

  it("returns conversations on success", async () => {
    const { GET } = await import("./conversation/list/route");
    const res = await GET();
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json.conversations).toHaveLength(1);
  });

  it("passes email as parameter (not interpolated)", async () => {
    const { GET } = await import("./conversation/list/route");
    await GET();
    expect(mockExecuteSqlSafe).toHaveBeenCalledWith(
      expect.stringContaining("@email"),
      expect.objectContaining({ email: "test@example.com" }),
    );
  });
});

// ── Feedback ────────────────────────────────────────────────────────
describe("POST /api/feedback", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionEmail = "test@example.com";
    mockExecuteSqlSafe.mockResolvedValue({ ok: true, rows: [] });
  });

  it("returns 401 when not authenticated", async () => {
    mockSessionEmail = null;
    const { POST } = await import("./feedback/route");
    const res = await POST(makeRequest({ messageId: "m1", rating: 1 }));
    expect(res.status).toBe(401);
  });

  it("returns 400 for invalid rating", async () => {
    const { POST } = await import("./feedback/route");
    const res = await POST(makeRequest({ messageId: "m1", rating: 5 }));
    expect(res.status).toBe(400);
  });

  it("returns 400 for missing messageId", async () => {
    const { POST } = await import("./feedback/route");
    const res = await POST(makeRequest({ rating: 1 }));
    expect(res.status).toBe(400);
  });

  it("saves feedback with valid payload", async () => {
    const { POST } = await import("./feedback/route");
    const res = await POST(
      makeRequest({ messageId: "m1", rating: -1, conversationId: "c1" }),
    );
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json.saved).toBe(true);
  });

  it("uses parameterized SQL for insert", async () => {
    const { POST } = await import("./feedback/route");
    await POST(makeRequest({ messageId: "m1", rating: 1 }));
    expect(mockExecuteSqlSafe).toHaveBeenCalledWith(
      expect.stringContaining("@msgId"),
      expect.objectContaining({ msgId: "m1", rating: 1 }),
    );
  });
});

// ── Insights ────────────────────────────────────────────────────────
describe("POST /api/insights", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionEmail = "test@example.com";
    mockExecuteSqlSafe.mockResolvedValue({ ok: true, rows: [] });
  });

  it("returns 401 when not authenticated", async () => {
    mockSessionEmail = null;
    const { POST } = await import("./insights/route");
    const res = await POST(makeRequest({ text: "Some insight text here" }));
    expect(res.status).toBe(401);
  });

  it("returns 400 for text shorter than 10 chars", async () => {
    const { POST } = await import("./insights/route");
    const res = await POST(makeRequest({ text: "short" }));
    expect(res.status).toBe(400);
  });

  it("saves insight with valid payload", async () => {
    const { POST } = await import("./insights/route");
    const res = await POST(
      makeRequest({ text: "Donor retention dropped 15% this quarter", category: "giving" }),
    );
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json.saved).toBe(true);
  });
});

describe("GET /api/insights", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionEmail = "test@example.com";
    mockExecuteSqlSafe.mockResolvedValue({
      ok: true,
      rows: [{ id: "i1", insight_text: "test insight", category: "giving" }],
    });
  });

  it("returns 401 when not authenticated", async () => {
    mockSessionEmail = null;
    const { GET } = await import("./insights/route");
    const res = await GET();
    expect(res.status).toBe(401);
  });

  it("returns insights on success", async () => {
    const { GET } = await import("./insights/route");
    const res = await GET();
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json.insights).toHaveLength(1);
  });
});

// ── Dashboard Save ──────────────────────────────────────────────────
describe("POST /api/dashboard/save", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionEmail = "test@example.com";
    mockWithTransaction.mockImplementation(async (fn: Function) => fn(vi.fn().mockResolvedValue([])));
  });

  it("returns 401 when not authenticated", async () => {
    mockSessionEmail = null;
    const { POST } = await import("./dashboard/save/route");
    const res = await POST(
      makeRequest({ name: "My Dashboard", widgets: [], layouts: [] }),
    );
    expect(res.status).toBe(401);
  });

  it("returns 400 for missing name", async () => {
    const { POST } = await import("./dashboard/save/route");
    const res = await POST(makeRequest({ widgets: [], layouts: [] }));
    expect(res.status).toBe(400);
  });

  it("saves dashboard with valid payload", async () => {
    const { POST } = await import("./dashboard/save/route");
    const res = await POST(
      makeRequest({
        name: "My Dashboard",
        widgets: [
          {
            id: "w1",
            type: "stat_grid",
            title: "Overview",
            config: {},
          },
        ],
        layouts: [{ i: "w1", x: 0, y: 0, w: 6, h: 4 }],
      }),
    );
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json.saved).toBe(true);
  });

  it("uses transaction for atomic save", async () => {
    const { POST } = await import("./dashboard/save/route");
    await POST(
      makeRequest({ name: "Test", widgets: [], layouts: [] }),
    );
    expect(mockWithTransaction).toHaveBeenCalledOnce();
  });
});
