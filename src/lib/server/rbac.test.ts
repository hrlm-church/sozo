import { describe, it, expect, vi, beforeEach } from "vitest";

// ── Mocks ───────────────────────────────────────────────────────────
const mockExecuteSqlSafe = vi.fn();
let mockSessionEmail: string | null = "test@example.com";

vi.mock("@/lib/server/session", () => ({
  getSessionEmail: vi.fn(() => Promise.resolve(mockSessionEmail)),
}));

vi.mock("@/lib/server/sql-client", () => ({
  executeSqlSafe: (...args: unknown[]) => mockExecuteSqlSafe(...args),
}));

// ── Tests ───────────────────────────────────────────────────────────
describe("hasMinRole", () => {
  it("admin meets all roles", async () => {
    const { hasMinRole } = await import("./user-context");
    expect(hasMinRole("admin", "viewer")).toBe(true);
    expect(hasMinRole("admin", "analyst")).toBe(true);
    expect(hasMinRole("admin", "admin")).toBe(true);
  });

  it("analyst meets analyst and viewer", async () => {
    const { hasMinRole } = await import("./user-context");
    expect(hasMinRole("analyst", "viewer")).toBe(true);
    expect(hasMinRole("analyst", "analyst")).toBe(true);
    expect(hasMinRole("analyst", "admin")).toBe(false);
  });

  it("viewer only meets viewer", async () => {
    const { hasMinRole } = await import("./user-context");
    expect(hasMinRole("viewer", "viewer")).toBe(true);
    expect(hasMinRole("viewer", "analyst")).toBe(false);
    expect(hasMinRole("viewer", "admin")).toBe(false);
  });
});

describe("getUserContext", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionEmail = "test@example.com";
  });

  it("returns null when not authenticated", async () => {
    mockSessionEmail = null;
    const { getUserContext, clearContextCache } = await import("./user-context");
    clearContextCache();
    const ctx = await getUserContext();
    expect(ctx).toBeNull();
  });

  it("returns null when user has no org membership", async () => {
    mockExecuteSqlSafe.mockResolvedValueOnce({ ok: true, rows: [] });
    const { getUserContext, clearContextCache } = await import("./user-context");
    clearContextCache();
    const ctx = await getUserContext();
    expect(ctx).toBeNull();
  });

  it("returns user context with role and org", async () => {
    mockExecuteSqlSafe.mockResolvedValueOnce({
      ok: true,
      rows: [{ org_id: "org-1", role: "analyst", org_name: "Test Org", org_slug: "test-org" }],
    });
    const { getUserContext, clearContextCache } = await import("./user-context");
    clearContextCache();
    const ctx = await getUserContext();
    expect(ctx).toEqual({
      email: "test@example.com",
      orgId: "org-1",
      orgName: "Test Org",
      orgSlug: "test-org",
      role: "analyst",
    });
  });

  it("uses parameterized query for email lookup", async () => {
    mockExecuteSqlSafe.mockResolvedValueOnce({ ok: true, rows: [] });
    const { getUserContext, clearContextCache } = await import("./user-context");
    clearContextCache();
    await getUserContext();
    expect(mockExecuteSqlSafe).toHaveBeenCalledWith(
      expect.stringContaining("@email"),
      expect.objectContaining({ email: "test@example.com" }),
    );
  });
});

describe("withRoleCheck", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSessionEmail = "test@example.com";
  });

  it("returns 401 when not authenticated", async () => {
    mockSessionEmail = null;
    const { withRoleCheck } = await import("./role-guard");
    const { clearContextCache } = await import("./user-context");
    clearContextCache();

    const handler = withRoleCheck("viewer", async (_req, ctx) => {
      return new Response(JSON.stringify({ email: ctx.email }));
    });

    const res = await handler(new Request("http://localhost/api/test"));
    expect(res.status).toBe(401);
  });

  it("returns 403 when role is insufficient", async () => {
    mockExecuteSqlSafe.mockResolvedValueOnce({
      ok: true,
      rows: [{ org_id: "org-1", role: "viewer", org_name: "Test Org", org_slug: "test-org" }],
    });
    const { withRoleCheck } = await import("./role-guard");
    const { clearContextCache } = await import("./user-context");
    clearContextCache();

    const handler = withRoleCheck("admin", async (_req, ctx) => {
      return new Response(JSON.stringify({ email: ctx.email }));
    });

    const res = await handler(new Request("http://localhost/api/test"));
    expect(res.status).toBe(403);
    const json = await res.json();
    expect(json.required).toBe("admin");
    expect(json.current).toBe("viewer");
  });

  it("passes context to handler when role is sufficient", async () => {
    mockExecuteSqlSafe.mockResolvedValueOnce({
      ok: true,
      rows: [{ org_id: "org-1", role: "admin", org_name: "Test Org", org_slug: "test-org" }],
    });
    const { withRoleCheck } = await import("./role-guard");
    const { clearContextCache } = await import("./user-context");
    clearContextCache();

    const handler = withRoleCheck("analyst", async (_req, ctx) => {
      return new Response(JSON.stringify({ email: ctx.email, role: ctx.role }));
    });

    const res = await handler(new Request("http://localhost/api/test"));
    expect(res.status).toBe(200);
    const json = await res.json();
    expect(json.email).toBe("test@example.com");
    expect(json.role).toBe("admin");
  });
});
