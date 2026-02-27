import { executeSqlSafe } from "@/lib/server/sql-client";

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

    await executeSqlSafe(
      `INSERT INTO sozo.insight (id, insight_text, category, confidence, source_query, owner_email, expires_at)
       VALUES (@id, @text, @category, @conf, @sourceQuery, @email, DATEADD(day, 30, SYSUTCDATETIME()))`,
      {
        id,
        text: text.slice(0, 1000),
        category: category.slice(0, 100),
        conf,
        sourceQuery: sourceQuery?.slice(0, 4000) ?? null,
        email: owner,
      },
    );
    return { ok: true, id };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to save insight" };
  }
}

/** Get recent non-expired insights for the system prompt */
export async function getRecentInsights(limit: number = 20, ownerEmail?: string): Promise<string> {
  try {
    const safeLimit = Math.min(limit, 50);

    const result = ownerEmail
      ? await executeSqlSafe(
          `SELECT TOP (@limit) insight_text, category, confidence,
                  FORMAT(created_at, 'MMM d') AS saved_on
           FROM sozo.insight
           WHERE (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
             AND owner_email = @email
           ORDER BY confidence DESC, created_at DESC`,
          { limit: safeLimit, email: ownerEmail },
        )
      : await executeSqlSafe(
          `SELECT TOP (@limit) insight_text, category, confidence,
                  FORMAT(created_at, 'MMM d') AS saved_on
           FROM sozo.insight
           WHERE (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
           ORDER BY confidence DESC, created_at DESC`,
          { limit: safeLimit },
        );

    if (!result.ok || result.rows.length === 0) return "";

    const lines = result.rows.map((r: Record<string, unknown>) => {
      const cat = ((r.category as string) || "general").toUpperCase();
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
    await executeSqlSafe(
      `IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'user_memory' AND schema_id = SCHEMA_ID('sozo'))
       CREATE TABLE sozo.user_memory (
         owner_email NVARCHAR(255) NOT NULL PRIMARY KEY,
         memory_text NVARCHAR(MAX) NOT NULL DEFAULT N'',
         updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
       )`,
    );
    memoryTableReady = true;
  } catch {
    memoryTableReady = true;
  }
}

/** Get the user's curated memory document */
export async function getUserMemory(ownerEmail: string): Promise<string> {
  try {
    await ensureMemoryTable();
    const result = await executeSqlSafe(
      `SELECT memory_text FROM sozo.user_memory WHERE owner_email = @email`,
      { email: ownerEmail },
    );
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
    const trimmed = memoryText.slice(0, 4000);
    await executeSqlSafe(
      `IF EXISTS (SELECT 1 FROM sozo.user_memory WHERE owner_email = @email)
       BEGIN
         UPDATE sozo.user_memory
         SET memory_text = @text, updated_at = SYSUTCDATETIME()
         WHERE owner_email = @email
       END
       ELSE
       BEGIN
         INSERT INTO sozo.user_memory (owner_email, memory_text, updated_at)
         VALUES (@email, @text, SYSUTCDATETIME())
       END`,
      { email: ownerEmail, text: trimmed },
    );
    return { ok: true };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to save memory" };
  }
}
