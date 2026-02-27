import { describe, it, expect } from "vitest";
import { guardSql, ALLOWED_TABLES } from "./sql-guard";

describe("guardSql", () => {
  // ── Basic SELECT queries ──────────────────────────────────────────
  it("allows a simple SELECT", () => {
    const r = guardSql("SELECT TOP 10 * FROM serving.person_360");
    expect(r.ok).toBe(true);
  });

  it("allows SELECT without TOP and injects TOP", () => {
    const r = guardSql("SELECT * FROM serving.person_360");
    expect(r.ok).toBe(true);
    expect(r.sanitized).toContain("TOP (500)");
  });

  it("preserves existing TOP value", () => {
    const r = guardSql("SELECT TOP 20 * FROM serving.person_360");
    expect(r.ok).toBe(true);
    expect(r.sanitized).not.toContain("TOP (500)");
    expect(r.sanitized).toContain("TOP 20");
  });

  it("preserves TOP with parentheses", () => {
    const r = guardSql("SELECT TOP(50) * FROM serving.person_360");
    expect(r.ok).toBe(true);
    expect(r.sanitized).not.toContain("TOP (500)");
  });

  it("allows OFFSET/FETCH pagination without injecting TOP", () => {
    const r = guardSql(
      "SELECT * FROM serving.person_360 ORDER BY display_name OFFSET 0 ROWS FETCH NEXT 25 ROWS ONLY",
    );
    expect(r.ok).toBe(true);
    expect(r.sanitized).not.toContain("TOP (500)");
  });

  // ── CTE (WITH) queries ────────────────────────────────────────────
  it("allows CTE queries starting with WITH", () => {
    const r = guardSql(
      "WITH cte AS (SELECT id FROM serving.person_360) SELECT * FROM cte",
    );
    expect(r.ok).toBe(true);
  });

  it("injects TOP into CTE final SELECT when missing", () => {
    const r = guardSql(
      "WITH cte AS (SELECT id FROM serving.person_360) SELECT * FROM cte",
    );
    expect(r.ok).toBe(true);
    expect(r.sanitized).toContain("TOP (500)");
    // TOP should be in the outer SELECT, not the CTE inner one
    const lastSelectIdx = r.sanitized!.lastIndexOf("SELECT");
    const topIdx = r.sanitized!.lastIndexOf("TOP (500)");
    expect(topIdx).toBeGreaterThan(r.sanitized!.indexOf(")")); // after CTE
  });

  // ── Blocked patterns ──────────────────────────────────────────────
  it("blocks DROP TABLE", () => {
    const r = guardSql("DROP TABLE serving.person_360");
    expect(r.ok).toBe(false);
    expect(r.reason).toContain("Blocked");
  });

  it("blocks INSERT", () => {
    const r = guardSql("INSERT INTO serving.person_360 VALUES (1)");
    expect(r.ok).toBe(false);
  });

  it("blocks UPDATE", () => {
    const r = guardSql("UPDATE serving.person_360 SET name = 'x'");
    expect(r.ok).toBe(false);
  });

  it("blocks DELETE", () => {
    const r = guardSql("DELETE FROM serving.person_360");
    expect(r.ok).toBe(false);
  });

  it("blocks ALTER TABLE", () => {
    const r = guardSql("ALTER TABLE serving.person_360 ADD col INT");
    expect(r.ok).toBe(false);
  });

  it("blocks EXEC/EXECUTE", () => {
    const r = guardSql("EXEC sp_executesql N'SELECT 1'");
    expect(r.ok).toBe(false);
  });

  it("blocks xp_ system procedures", () => {
    const r = guardSql("SELECT * FROM xp_cmdshell('dir')");
    expect(r.ok).toBe(false);
  });

  it("blocks SQL comment injection (--)", () => {
    const r = guardSql("SELECT * FROM serving.person_360 -- DROP TABLE x");
    expect(r.ok).toBe(false);
  });

  it("blocks block comment injection (/*)", () => {
    const r = guardSql("SELECT /* malicious */ * FROM serving.person_360");
    expect(r.ok).toBe(false);
  });

  it("blocks SELECT INTO", () => {
    const r = guardSql("SELECT * INTO #temp FROM serving.person_360");
    expect(r.ok).toBe(false);
  });

  it("blocks GRANT/REVOKE/DENY", () => {
    expect(guardSql("GRANT SELECT ON serving.person_360 TO public").ok).toBe(false);
    expect(guardSql("REVOKE SELECT ON serving.person_360 FROM public").ok).toBe(false);
    expect(guardSql("DENY SELECT ON serving.person_360 TO public").ok).toBe(false);
  });

  it("blocks chained injection after semicolon", () => {
    const r = guardSql("SELECT 1; DROP TABLE serving.person_360");
    expect(r.ok).toBe(false);
  });

  // ── Must start with SELECT or WITH ────────────────────────────────
  it("rejects queries not starting with SELECT or WITH", () => {
    const r = guardSql("DECLARE @x INT = 1; SELECT @x");
    expect(r.ok).toBe(false);
    expect(r.reason).toContain("Only SELECT and WITH");
  });

  // ── Trailing semicolons stripped ──────────────────────────────────
  it("strips trailing semicolons", () => {
    const r = guardSql("SELECT TOP 10 * FROM serving.person_360;;;");
    expect(r.ok).toBe(true);
    expect(r.sanitized).not.toMatch(/;+\s*$/);
  });

  // ── ALLOWED_TABLES ────────────────────────────────────────────────
  it("contains expected serving layer tables", () => {
    expect(ALLOWED_TABLES.has("serving.person_360")).toBe(true);
    expect(ALLOWED_TABLES.has("serving.donation_detail")).toBe(true);
  });

  it("contains sozo app tables", () => {
    expect(ALLOWED_TABLES.has("sozo.conversation")).toBe(true);
    expect(ALLOWED_TABLES.has("sozo.knowledge")).toBe(true);
  });

  it("contains intel layer tables", () => {
    expect(ALLOWED_TABLES.has("intel.metric_definition")).toBe(true);
    expect(ALLOWED_TABLES.has("intel.vw_donor_health")).toBe(true);
  });

  it("does not contain system tables", () => {
    expect(ALLOWED_TABLES.has("sys.tables")).toBe(false);
    expect(ALLOWED_TABLES.has("master.dbo.sysobjects")).toBe(false);
  });
});
