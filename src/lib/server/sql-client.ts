import sql from "mssql";
import { getServerEnv, looksConfigured } from "@/lib/server/env";

type SqlRow = Record<string, unknown>;
type ParamValue = string | number | boolean | null;

let _pool: sql.ConnectionPool | null = null;
let _poolPromise: Promise<sql.ConnectionPool> | null = null;

function getPoolConfig(): sql.config {
  const env = getServerEnv();
  return {
    server: env.sqlHost,
    database: env.sqlDb,
    user: env.sqlUser,
    password: env.sqlPassword,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
    connectionTimeout: 15000,
    requestTimeout: 30000,
  };
}

/** Get or create the singleton connection pool */
async function getPool(): Promise<sql.ConnectionPool> {
  if (_pool?.connected) return _pool;

  if (!_poolPromise) {
    _poolPromise = sql.connect(getPoolConfig()).then((pool) => {
      _pool = pool;
      pool.on("error", () => {
        _pool = null;
        _poolPromise = null;
      });
      return pool;
    });
  }

  return _poolPromise;
}

/** Bind typed parameters to an mssql Request */
function bindParams(request: sql.Request, params: Record<string, ParamValue>) {
  for (const [name, value] of Object.entries(params)) {
    if (value === null) {
      request.input(name, sql.NVarChar, null);
    } else if (typeof value === "number") {
      request.input(name, Number.isInteger(value) ? sql.Int : sql.Float, value);
    } else if (typeof value === "boolean") {
      request.input(name, sql.Bit, value);
    } else {
      request.input(name, sql.NVarChar(sql.MAX), value);
    }
  }
}

export const isSqlConfigured = () => {
  const env = getServerEnv();
  return looksConfigured(env.sqlUser) && looksConfigured(env.sqlPassword);
};

/**
 * Execute raw SQL (no parameterization).
 * ONLY use for: (1) LLM-generated queries that go through sql-guard,
 * (2) internal static queries with no user input.
 * For anything with user-supplied values, use executeSqlSafe().
 */
export const executeSql = async (
  sqlText: string,
  timeoutMs?: number,
): Promise<{ ok: boolean; reason?: string; rows: SqlRow[] }> => {
  if (!isSqlConfigured()) {
    return {
      ok: false,
      reason: "SOZO_SQL_USER and SOZO_SQL_PASSWORD are required.",
      rows: [],
    };
  }

  try {
    const pool = await getPool();
    const request = pool.request();
    if (timeoutMs) (request as unknown as { timeout: number }).timeout = timeoutMs;
    const result = await request.query(sqlText);
    return { ok: true, rows: result.recordset ?? [] };
  } catch (error) {
    return {
      ok: false,
      reason: error instanceof Error ? error.message : "Azure SQL query failed",
      rows: [],
    };
  }
};

/**
 * Execute parameterized SQL — safe from SQL injection.
 * Use @paramName in SQL text and pass { paramName: value } in params.
 */
export const executeSqlSafe = async (
  sqlText: string,
  params: Record<string, ParamValue> = {},
  timeoutMs?: number,
): Promise<{ ok: boolean; reason?: string; rows: SqlRow[] }> => {
  if (!isSqlConfigured()) {
    return {
      ok: false,
      reason: "SOZO_SQL_USER and SOZO_SQL_PASSWORD are required.",
      rows: [],
    };
  }

  try {
    const pool = await getPool();
    const request = pool.request();
    if (timeoutMs) (request as unknown as { timeout: number }).timeout = timeoutMs;
    bindParams(request, params);
    const result = await request.query(sqlText);
    return { ok: true, rows: result.recordset ?? [] };
  } catch (error) {
    return {
      ok: false,
      reason: error instanceof Error ? error.message : "Azure SQL query failed",
      rows: [],
    };
  }
};

/**
 * Execute multiple statements atomically in a transaction.
 * The `exec` callback throws on SQL error (triggering automatic rollback).
 */
export const withTransaction = async <T>(
  fn: (exec: (sqlText: string, params?: Record<string, ParamValue>, timeoutMs?: number) => Promise<SqlRow[]>) => Promise<T>,
): Promise<T> => {
  const pool = await getPool();
  const tx = new sql.Transaction(pool);
  await tx.begin();

  const exec = async (
    sqlText: string,
    params: Record<string, ParamValue> = {},
    timeoutMs?: number,
  ): Promise<SqlRow[]> => {
    const request = new sql.Request(tx);
    if (timeoutMs) (request as unknown as { timeout: number }).timeout = timeoutMs;
    bindParams(request, params);
    const result = await request.query(sqlText);
    return result.recordset ?? [];
  };

  try {
    const result = await fn(exec);
    await tx.commit();
    return result;
  } catch (err) {
    try {
      await tx.rollback();
    } catch {
      /* already rolled back */
    }
    throw err;
  }
};
