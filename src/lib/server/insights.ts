import { executeSql } from "@/lib/server/sql-client";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

// ── Data Insights (ephemeral, 30-day TTL) ──────────────────────────

/** Save an analytical finding to the database */
export async function saveInsight(
  text: string,
  category: string,
  confidence: number,
  sourceQuery?: string,
  ownerEmail?: string,
): Promise<{ ok: boolean; id?: string; error?: string }> {
  try {
    const owner = ownerEmail || "system@sozo.local";
    const id = crypto.randomUUID();
    const conf = Math.min(Math.max(confidence, 0), 1);

    await executeSql(`
      INSERT INTO sozo.insight (id, insight_text, category, confidence, source_query, owner_email, expires_at)
      VALUES (
        '${esc(id)}',
        N'${esc(text.slice(0, 1000))}',
        N'${esc(category.slice(0, 100))}',
        ${conf},
        ${sourceQuery ? `N'${esc(sourceQuery.slice(0, 4000))}'` : "NULL"},
        N'${esc(owner)}',
        DATEADD(day, 30, SYSUTCDATETIME())
      )
    `);
    return { ok: true, id };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to save insight" };
  }
}

/** Get recent non-expired insights for the system prompt */
export async function getRecentInsights(limit: number = 20, ownerEmail?: string): Promise<string> {
  try {
    const ownerFilter = ownerEmail
      ? `AND owner_email = N'${esc(ownerEmail)}'`
      : "";
    const result = await executeSql(`
      SELECT TOP (${Math.min(limit, 50)}) insight_text, category, confidence,
             FORMAT(created_at, 'MMM d') AS saved_on
      FROM sozo.insight
      WHERE (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
        ${ownerFilter}
      ORDER BY confidence DESC, created_at DESC
    `);
    if (!result.ok || result.rows.length === 0) return "";

    const lines = result.rows.map((r: Record<string, unknown>) => {
      const cat = (r.category as string || "general").toUpperCase();
      const text = r.insight_text as string;
      return `- [${cat}] ${text}`;
    });
    return lines.join("\n");
  } catch {
    return "";
  }
}

// ── User Memory (persistent, curated document per user) ────────────

let memoryTableReady = false;

/** Ensure sozo.user_memory table exists */
async function ensureMemoryTable(): Promise<void> {
  if (memoryTableReady) return;
  try {
    await executeSql(`
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'user_memory' AND schema_id = SCHEMA_ID('sozo'))
      CREATE TABLE sozo.user_memory (
        owner_email NVARCHAR(255) NOT NULL PRIMARY KEY,
        memory_text NVARCHAR(MAX) NOT NULL DEFAULT N'',
        updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    memoryTableReady = true;
  } catch {
    // Table likely already exists
    memoryTableReady = true;
  }
}

/** Get the user's curated memory document */
export async function getUserMemory(ownerEmail: string): Promise<string> {
  try {
    await ensureMemoryTable();
    const result = await executeSql(`
      SELECT memory_text FROM sozo.user_memory
      WHERE owner_email = N'${esc(ownerEmail)}'
    `);
    if (!result.ok || result.rows.length === 0) return "";
    return (result.rows[0].memory_text as string) || "";
  } catch {
    return "";
  }
}

/** Save/update the user's curated memory document (upsert) */
export async function saveUserMemory(
  ownerEmail: string,
  memoryText: string,
): Promise<{ ok: boolean; error?: string }> {
  try {
    await ensureMemoryTable();
    const trimmed = memoryText.slice(0, 4000); // Cap at 4000 chars
    await executeSql(`
      MERGE sozo.user_memory AS t
      USING (SELECT N'${esc(ownerEmail)}' AS owner_email) AS s
      ON t.owner_email = s.owner_email
      WHEN MATCHED THEN
        UPDATE SET memory_text = N'${esc(trimmed)}', updated_at = SYSUTCDATETIME()
      WHEN NOT MATCHED THEN
        INSERT (owner_email, memory_text, updated_at)
        VALUES (N'${esc(ownerEmail)}', N'${esc(trimmed)}', SYSUTCDATETIME());
    `);
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to save memory" };
  }
}
