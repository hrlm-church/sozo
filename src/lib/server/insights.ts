import { executeSql } from "@/lib/server/sql-client";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

/** Save an insight to the database — per-user ownership */
export async function saveInsight(
  text: string,
  category: string,
  confidence: number,
  sourceQuery?: string,
  ownerEmail?: string,
): Promise<{ ok: boolean; id?: string; error?: string }> {
  try {
    const owner = ownerEmail || "system@sozo.local";

    // Persistent categories: dedup by matching first 50 chars, update instead of duplicate
    const dedupCategories = ["user_interest", "correction", "learning"];
    if (dedupCategories.includes(category)) {
      const prefix = text.slice(0, 50);
      const existing = await executeSql(`
        SELECT TOP (1) id FROM sozo.insight
        WHERE owner_email = N'${esc(owner)}'
          AND category = N'${esc(category)}'
          AND LEFT(insight_text, 50) = N'${esc(prefix)}'
      `);
      if (existing.ok && existing.rows.length > 0) {
        const existingId = existing.rows[0].id as string;
        await executeSql(`
          UPDATE sozo.insight
          SET insight_text = N'${esc(text.slice(0, 1000))}',
              confidence = ${Math.min(Math.max(confidence, 0), 1)},
              source_query = ${sourceQuery ? `N'${esc(sourceQuery.slice(0, 4000))}'` : "NULL"},
              created_at = SYSUTCDATETIME()
          WHERE id = '${esc(existingId)}'
        `);
        return { ok: true, id: existingId };
      }
    }

    const id = crypto.randomUUID();
    const conf = Math.min(Math.max(confidence, 0), 1);
    // Persistent categories never expire — they represent learned knowledge
    const persistentCategories = ["user_interest", "correction", "learning"];
    const expiresAt = persistentCategories.includes(category)
      ? "NULL"
      : "DATEADD(day, 30, SYSUTCDATETIME())";

    await executeSql(`
      INSERT INTO sozo.insight (id, insight_text, category, confidence, source_query, owner_email, expires_at)
      VALUES (
        '${esc(id)}',
        N'${esc(text.slice(0, 1000))}',
        N'${esc(category.slice(0, 100))}',
        ${conf},
        ${sourceQuery ? `N'${esc(sourceQuery.slice(0, 4000))}'` : "NULL"},
        N'${esc(owner)}',
        ${expiresAt}
      )
    `);
    return { ok: true, id };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "Failed to save insight" };
  }
}

/** Get recent non-expired insights for the system prompt — filtered per-user */
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

/** Get user context from saved persistent memories — injected into system prompt */
export async function getUserContext(ownerEmail: string): Promise<string> {
  try {
    const result = await executeSql(`
      SELECT TOP (30) insight_text, category, FORMAT(created_at, 'MMM d') AS saved_on
      FROM sozo.insight
      WHERE owner_email = N'${esc(ownerEmail)}'
        AND category IN (N'user_interest', N'correction', N'learning')
        AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
      ORDER BY created_at DESC
    `);
    if (!result.ok || result.rows.length === 0) return "";

    const sections: Record<string, string[]> = {};
    for (const r of result.rows) {
      const cat = r.category as string;
      const text = r.insight_text as string;
      if (!sections[cat]) sections[cat] = [];
      sections[cat].push(`- ${text}`);
    }

    const parts: string[] = [];
    if (sections.correction?.length) {
      parts.push(`**Corrections (things you got wrong before — don't repeat):**\n${sections.correction.join("\n")}`);
    }
    if (sections.user_interest?.length) {
      parts.push(`**What this user cares about:**\n${sections.user_interest.join("\n")}`);
    }
    if (sections.learning?.length) {
      parts.push(`**Learned from past conversations:**\n${sections.learning.join("\n")}`);
    }
    return parts.join("\n\n");
  } catch {
    return "";
  }
}
