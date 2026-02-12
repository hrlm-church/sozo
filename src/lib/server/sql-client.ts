import sql from "mssql";
import { getServerEnv, looksConfigured } from "@/lib/server/env";

type SqlRow = Record<string, unknown>;

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
    requestTimeout: 15000,
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

export const isSqlConfigured = () => {
  const env = getServerEnv();
  return looksConfigured(env.sqlUser) && looksConfigured(env.sqlPassword);
};

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
