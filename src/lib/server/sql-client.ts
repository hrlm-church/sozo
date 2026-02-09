import { getServerEnv, looksConfigured } from "@/lib/server/env";

type SqlRow = Record<string, unknown>;

type SqlDriver = {
  connect: (config: Record<string, unknown>) => Promise<{
    request: () => { query: (sql: string) => Promise<{ recordset: SqlRow[] }> };
    close: () => Promise<void>;
  }>;
};

const safeLoadMssql = async (): Promise<SqlDriver | null> => {
  try {
    const dynamicImport = new Function("name", "return import(name)") as (
      name: string,
    ) => Promise<unknown>;
    const imported = (await dynamicImport("mssql")) as {
      default?: unknown;
      connect?: SqlDriver["connect"];
    };

    const driver = (imported.default ?? imported) as SqlDriver;
    if (typeof driver.connect !== "function") {
      return null;
    }
    return driver;
  } catch {
    return null;
  }
};

export const isSqlConfigured = () => {
  const env = getServerEnv();
  return looksConfigured(env.sqlUser) && looksConfigured(env.sqlPassword);
};

export const executeSql = async (sqlText: string): Promise<{ ok: boolean; reason?: string; rows: SqlRow[] }> => {
  const env = getServerEnv();

  if (!isSqlConfigured()) {
    return {
      ok: false,
      reason: "SOZO_SQL_USER and SOZO_SQL_PASSWORD are required for SQL query execution.",
      rows: [],
    };
  }

  const mssql = await safeLoadMssql();
  if (!mssql) {
    return {
      ok: false,
      reason: "SQL driver unavailable in runtime (missing 'mssql' package).",
      rows: [],
    };
  }

  let pool: Awaited<ReturnType<typeof mssql.connect>> | null = null;
  try {
    pool = await mssql.connect({
      server: env.sqlHost,
      database: env.sqlDb,
      user: env.sqlUser,
      password: env.sqlPassword,
      options: {
        encrypt: true,
      },
      pool: {
        max: 2,
        min: 0,
        idleTimeoutMillis: 5000,
      },
    });

    const result = await pool.request().query(sqlText);
    return {
      ok: true,
      rows: result.recordset ?? [],
    };
  } catch (error) {
    return {
      ok: false,
      reason: error instanceof Error ? error.message : "Azure SQL query failed",
      rows: [],
    };
  } finally {
    if (pool) {
      await pool.close();
    }
  }
};
