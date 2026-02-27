import { describe, it, expect, vi, beforeEach } from "vitest";

// ── Mock mssql module ───────────────────────────────────────────────
const mockQuery = vi.fn();
const mockInput = vi.fn();
const mockBegin = vi.fn();
const mockCommit = vi.fn();
const mockRollback = vi.fn();

const mockRequest = () => ({
  input: mockInput,
  query: mockQuery,
  timeout: 0,
});

const mockPool = {
  connected: true,
  request: mockRequest,
  on: vi.fn(),
};

vi.mock("mssql", () => {
  // Use real function constructors so `new` works
  function Transaction() {
    return { begin: mockBegin, commit: mockCommit, rollback: mockRollback };
  }
  function Request() {
    return { input: mockInput, query: mockQuery, timeout: 0 };
  }

  return {
    default: {
      connect: vi.fn().mockResolvedValue(mockPool),
      Transaction,
      Request,
      NVarChar: vi.fn((max?: unknown) => `NVarChar(${max})`),
      Int: "Int",
      Float: "Float",
      Bit: "Bit",
      MAX: "MAX",
    },
  };
});

// Mock env to avoid real env var requirements
vi.mock("@/lib/server/env", () => ({
  getServerEnv: () => ({
    sqlHost: "test.database.windows.net",
    sqlDb: "testdb",
    sqlUser: "testuser",
    sqlPassword: "testpass",
  }),
  looksConfigured: (v?: string) => Boolean(v && !v.includes("az keyvault")),
}));

describe("executeSqlSafe", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockQuery.mockResolvedValue({ recordset: [] });
    mockBegin.mockResolvedValue(undefined);
    mockCommit.mockResolvedValue(undefined);
    mockRollback.mockResolvedValue(undefined);
  });

  it("executes parameterized query and returns rows", async () => {
    const rows = [{ id: "1", name: "Alice" }];
    mockQuery.mockResolvedValueOnce({ recordset: rows });

    const { executeSqlSafe } = await import("./sql-client");
    const result = await executeSqlSafe(
      "SELECT * FROM serving.person_360 WHERE id = @id",
      { id: "1" },
    );

    expect(result.ok).toBe(true);
    expect(result.rows).toEqual(rows);
    expect(mockInput).toHaveBeenCalledWith("id", expect.anything(), "1");
  });

  it("binds string parameters as NVarChar", async () => {
    const { executeSqlSafe } = await import("./sql-client");
    await executeSqlSafe("SELECT 1 WHERE name = @name", { name: "test" });

    expect(mockInput).toHaveBeenCalledWith("name", expect.anything(), "test");
  });

  it("binds integer parameters as Int", async () => {
    const { executeSqlSafe } = await import("./sql-client");
    await executeSqlSafe("SELECT 1 WHERE count = @count", { count: 42 });

    expect(mockInput).toHaveBeenCalledWith("count", "Int", 42);
  });

  it("binds float parameters as Float", async () => {
    const { executeSqlSafe } = await import("./sql-client");
    await executeSqlSafe("SELECT 1 WHERE score = @score", { score: 0.85 });

    expect(mockInput).toHaveBeenCalledWith("score", "Float", 0.85);
  });

  it("binds boolean parameters as Bit", async () => {
    const { executeSqlSafe } = await import("./sql-client");
    await executeSqlSafe("SELECT 1 WHERE active = @active", { active: true });

    expect(mockInput).toHaveBeenCalledWith("active", "Bit", true);
  });

  it("binds null parameters as NVarChar null", async () => {
    const { executeSqlSafe } = await import("./sql-client");
    await executeSqlSafe("SELECT 1 WHERE val = @val", { val: null });

    expect(mockInput).toHaveBeenCalledWith("val", expect.anything(), null);
  });

  it("returns error on SQL failure", async () => {
    mockQuery.mockRejectedValueOnce(new Error("Timeout expired"));

    const { executeSqlSafe } = await import("./sql-client");
    const result = await executeSqlSafe("SELECT 1");

    expect(result.ok).toBe(false);
    expect(result.reason).toContain("Timeout expired");
    expect(result.rows).toEqual([]);
  });

  it("handles missing recordset gracefully", async () => {
    mockQuery.mockResolvedValueOnce({});

    const { executeSqlSafe } = await import("./sql-client");
    const result = await executeSqlSafe("SELECT 1");

    expect(result.ok).toBe(true);
    expect(result.rows).toEqual([]);
  });
});

describe("withTransaction", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockQuery.mockResolvedValue({ recordset: [] });
    mockBegin.mockResolvedValue(undefined);
    mockCommit.mockResolvedValue(undefined);
    mockRollback.mockResolvedValue(undefined);
  });

  it("commits on success", async () => {
    const { withTransaction } = await import("./sql-client");
    await withTransaction(async (exec) => {
      await exec("INSERT INTO t VALUES (@id)", { id: "1" });
    });

    expect(mockBegin).toHaveBeenCalledOnce();
    expect(mockCommit).toHaveBeenCalledOnce();
    expect(mockRollback).not.toHaveBeenCalled();
  });

  it("rolls back on error and rethrows", async () => {
    mockQuery.mockRejectedValueOnce(new Error("Constraint violation"));

    const { withTransaction } = await import("./sql-client");
    await expect(
      withTransaction(async (exec) => {
        await exec("INSERT INTO t VALUES (@id)", { id: "bad" });
      }),
    ).rejects.toThrow("Constraint violation");

    expect(mockBegin).toHaveBeenCalledOnce();
    expect(mockRollback).toHaveBeenCalledOnce();
    expect(mockCommit).not.toHaveBeenCalled();
  });

  it("returns value from callback", async () => {
    const { withTransaction } = await import("./sql-client");
    const result = await withTransaction(async () => "done");

    expect(result).toBe("done");
  });

  it("binds parameters in transaction exec calls", async () => {
    const { withTransaction } = await import("./sql-client");
    await withTransaction(async (exec) => {
      await exec("UPDATE t SET name = @name WHERE id = @id", {
        name: "Alice",
        id: "1",
      });
    });

    expect(mockInput).toHaveBeenCalledWith("name", expect.anything(), "Alice");
    expect(mockInput).toHaveBeenCalledWith("id", expect.anything(), "1");
  });
});
