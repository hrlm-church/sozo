/**
 * Knowledge base and conversation summary CRUD for the continuous learning system.
 */
import { executeSql } from "@/lib/server/sql-client";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

// ─── Knowledge CRUD ──────────────────────────────────────────────────

export interface KnowledgeItem {
  id: string;
  category: string;
  content: string;
  confidence: number;
  created_at: string;
}

/**
 * Load all active knowledge items for a user, corrections first.
 */
export async function getActiveKnowledge(
  ownerEmail: string,
  limit = 30,
): Promise<KnowledgeItem[]> {
  const result = await executeSql(`
    SELECT TOP (${limit}) id, category, content, confidence, created_at
    FROM sozo.knowledge
    WHERE owner_email = N'${esc(ownerEmail)}' AND is_active = 1
    ORDER BY
      CASE category
        WHEN 'correction' THEN 1
        WHEN 'preference' THEN 2
        WHEN 'persona' THEN 3
        WHEN 'fact' THEN 4
        WHEN 'pattern' THEN 5
      END,
      confidence DESC,
      created_at DESC
  `);
  if (!result.ok) return [];
  return result.rows as unknown as KnowledgeItem[];
}

/**
 * Save a single knowledge item. Optionally supersedes an older item.
 */
export async function saveKnowledge(
  ownerEmail: string,
  category: string,
  content: string,
  confidence: number,
  supersedesId?: string,
  sourceConvId?: string,
): Promise<{ ok: boolean; id?: string; error?: string }> {
  const id = crypto.randomUUID();

  // Deactivate superseded item if specified
  if (supersedesId) {
    await executeSql(`
      UPDATE sozo.knowledge
      SET is_active = 0, updated_at = SYSUTCDATETIME()
      WHERE id = '${esc(supersedesId)}' AND owner_email = N'${esc(ownerEmail)}'
    `);
  }

  const result = await executeSql(`
    INSERT INTO sozo.knowledge (id, owner_email, category, content, source_conv_id, confidence, supersedes_id)
    VALUES (
      '${esc(id)}',
      N'${esc(ownerEmail)}',
      N'${esc(category)}',
      N'${esc(content.slice(0, 2000))}',
      ${sourceConvId ? `'${esc(sourceConvId)}'` : "NULL"},
      ${Math.max(0, Math.min(1, confidence))},
      ${supersedesId ? `'${esc(supersedesId)}'` : "NULL"}
    )
  `);

  if (!result.ok) return { ok: false, error: result.reason };
  return { ok: true, id };
}

// ─── Conversation Summary CRUD ───────────────────────────────────────

/**
 * Save or update a conversation summary.
 */
export async function saveConversationSummary(
  conversationId: string,
  ownerEmail: string,
  title: string,
  summaryText: string,
  topics: string[],
  queryPatterns: string[],
  messageCount: number,
): Promise<{ ok: boolean; error?: string }> {
  const topicsJson = JSON.stringify(topics);
  const patternsJson = JSON.stringify(queryPatterns);

  const result = await executeSql(`
    IF EXISTS (SELECT 1 FROM sozo.conversation_summary WHERE id = '${esc(conversationId)}')
    BEGIN
      UPDATE sozo.conversation_summary
      SET summary_text = N'${esc(summaryText)}',
          title = N'${esc(title.slice(0, 256))}',
          topics = N'${esc(topicsJson)}',
          query_patterns = N'${esc(patternsJson)}',
          message_count = ${messageCount},
          updated_at = SYSUTCDATETIME()
      WHERE id = '${esc(conversationId)}'
    END
    ELSE
    BEGIN
      INSERT INTO sozo.conversation_summary (id, owner_email, title, summary_text, topics, query_patterns, message_count)
      VALUES (
        '${esc(conversationId)}',
        N'${esc(ownerEmail)}',
        N'${esc(title.slice(0, 256))}',
        N'${esc(summaryText)}',
        N'${esc(topicsJson)}',
        N'${esc(patternsJson)}',
        ${messageCount}
      )
    END
  `);

  if (!result.ok) return { ok: false, error: result.reason };
  return { ok: true };
}

// ─── Formatting for System Prompt ────────────────────────────────────

/**
 * Format knowledge items into a compact prompt section.
 */
export function formatKnowledgeForPrompt(items: KnowledgeItem[]): string {
  if (items.length === 0) return "";

  const groups: Record<string, string[]> = {};
  for (const item of items) {
    const label = item.category.charAt(0).toUpperCase() + item.category.slice(1) + "s";
    if (!groups[label]) groups[label] = [];
    groups[label].push(`- ${item.content}`);
  }

  return Object.entries(groups)
    .map(([label, lines]) => `### ${label}\n${lines.join("\n")}`)
    .join("\n\n");
}

export interface MemoryResult {
  conversation_id: string;
  title: string;
  content: string;
  topics: string;
  created_at: string;
  score: number;
}

/**
 * Format memory search results into a compact prompt section.
 */
export function formatMemoriesForPrompt(memories: MemoryResult[]): string {
  if (memories.length === 0) return "";

  return memories
    .map((m) => {
      const date = new Date(m.created_at).toLocaleDateString("en-US", {
        month: "short",
        day: "numeric",
      });
      return `- [${date}] "${m.title}": ${m.content}`;
    })
    .join("\n");
}
