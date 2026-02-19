import { executeSql } from "@/lib/server/sql-client";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

/** Save an insight to the database */
export async function saveInsight(
  text: string,
  category: string,
  confidence: number,
  sourceQuery?: string,
): Promise<{ ok: boolean; id?: string; error?: string }> {
  try {
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
        N'system@sozo.local',
        DATEADD(day, 30, SYSUTCDATETIME())
      )
    `);
    return { ok: true, id };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to save insight" };
  }
}

/** Get recent non-expired insights for the system prompt */
export async function getRecentInsights(limit: number = 20): Promise<string> {
  try {
    const result = await executeSql(`
      SELECT TOP (${Math.min(limit, 50)}) insight_text, category, confidence,
             FORMAT(created_at, 'MMM d') AS saved_on
      FROM sozo.insight
      WHERE (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
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
