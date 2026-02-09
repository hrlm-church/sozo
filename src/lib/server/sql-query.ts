import { getServerEnv } from "@/lib/server/env";
import type { Citation, TableArtifact } from "@/lib/server/dashboard-summary";
import { executeSql } from "@/lib/server/sql-client";

interface SqlResult {
  ok: boolean;
  reason?: string;
  query?: string;
  citations: Citation[];
  table?: TableArtifact;
}

const queryForPrompt = (prompt: string) => {
  const normalized = prompt.toLowerCase();

  if (/(column|field|schema)/.test(normalized)) {
    return `
SELECT TOP (25)
  TABLE_SCHEMA,
  TABLE_NAME,
  COLUMN_NAME,
  DATA_TYPE
FROM INFORMATION_SCHEMA.COLUMNS
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
`.trim();
  }

  if (/(risk|mismatch|household)/.test(normalized)) {
    return `
SELECT TOP (25)
  report_date,
  household_name,
  signal,
  risk_score,
  high_risk,
  updated_at
FROM dbo.household_risk_daily
ORDER BY report_date DESC, risk_score DESC;
`.trim();
  }

  if (/(profile|linkage|exception|migration)/.test(normalized)) {
    return `
SELECT TOP (25)
  report_date,
  profiles_linked,
  migration_exceptions
FROM dbo.profile_linkage_daily
ORDER BY report_date DESC;
`.trim();
  }

  return `
SELECT TOP (25)
  report_date,
  household_name,
  signal,
  risk_score
FROM dbo.household_risk_daily
ORDER BY report_date DESC, risk_score DESC;
`.trim();
};

export const runSqlQuery = async (prompt: string): Promise<SqlResult> => {
  const env = getServerEnv();
  const sqlText = queryForPrompt(prompt);
  const result = await executeSql(sqlText);
  if (!result.ok) {
    return {
      ok: false,
      reason: result.reason,
      query: sqlText,
      citations: [],
    };
  }

  const columns = result.rows.length > 0 ? Object.keys(result.rows[0]) : ["Result"];
  const rows = result.rows.map((row) => columns.map((column) => String(row[column] ?? "")));

  const table: TableArtifact = {
    id: "sql-results",
    title: "Azure SQL Results",
    columns,
    rows,
  };

  return {
    ok: true,
    query: sqlText,
    citations: [
      {
        title: "Azure SQL Query",
        source: `${env.sqlHost}/${env.sqlDb}`,
        snippet: sqlText,
      },
    ],
    table,
  };
};
