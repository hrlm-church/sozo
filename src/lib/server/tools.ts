import { tool } from "ai";
import { z } from "zod";
import { guardSql, QUERY_TIMEOUT_MS } from "@/lib/server/sql-guard";
import { executeSql } from "@/lib/server/sql-client";
import { hybridSearch } from "@/lib/server/search-client";

import { saveInsight } from "@/lib/server/insights";
import { saveKnowledge } from "@/lib/server/memory";
import { executeSql as execSqlDirect } from "@/lib/server/sql-client";
import type { Widget, WidgetType, WidgetConfig } from "@/types/widget";

/**
 * All LLM tools for the chat API.
 *
 * Each call creates a fresh closure so query results can be shared
 * between query_data → show_widget within one request without forcing
 * the LLM to re-emit every row as tool-call arguments (saves tokens,
 * avoids truncation on large result sets).
 */
// Normalize SQL for map key: collapse whitespace, trim, strip trailing semicolons
function sqlKey(s: string): string {
  return s.replace(/\s+/g, " ").trim().replace(/;+$/, "");
}

export function getChatTools(ownerEmail?: string) {
  // Shared state: query results keyed by normalized SQL so show_widget can
  // reference the correct dataset when multiple queries run in one step.
  const queryResultMap = new Map<string, Record<string, unknown>[]>();
  let lastQueryRows: Record<string, unknown>[] = [];
  let lastQuerySql: string | undefined;

  return {
    query_data: tool({
      description:
        "Execute a read-only SQL SELECT query against Azure SQL. " +
        "Returns up to 500 rows. You see a preview (first 15 rows); full data is stored for show_widget. " +
        "Only SELECT and WITH (CTE) queries are allowed. " +
        "The query results are automatically available to show_widget — " +
        "you do NOT need to pass the data rows again.",
      inputSchema: z.object({
        sql: z.string().describe("The SQL SELECT query to execute"),
        purpose: z
          .string()
          .describe("Brief description of what this query answers"),
      }),
      execute: async ({ sql: rawSql, purpose }) => {
        const guard = guardSql(rawSql);
        if (!guard.ok) {
          return { ok: false as const, error: guard.reason, data: [] as Record<string, unknown>[], sql: rawSql };
        }
        const result = await executeSql(guard.sanitized!, QUERY_TIMEOUT_MS);
        if (!result.ok) {
          return {
            ok: false as const,
            error: result.reason,
            data: [] as Record<string, unknown>[],
            sql: guard.sanitized,
          };
        }
        // Store for show_widget to use (keyed by normalized raw + sanitized SQL)
        lastQueryRows = result.rows;
        lastQuerySql = guard.sanitized;
        queryResultMap.set(sqlKey(rawSql), result.rows);
        if (guard.sanitized) queryResultMap.set(sqlKey(guard.sanitized), result.rows);
        // Return preview to model (saves tokens), full data stays in map for widgets
        const PREVIEW_LIMIT = 15;
        const preview = result.rows.length > PREVIEW_LIMIT
          ? result.rows.slice(0, PREVIEW_LIMIT)
          : result.rows;
        return {
          ok: true as const,
          rowCount: result.rows.length,
          data: preview,
          sql: guard.sanitized,
          purpose,
          ...(result.rows.length > PREVIEW_LIMIT && { note: `Showing ${PREVIEW_LIMIT} of ${result.rows.length} rows. Full data available in widget.` }),
        };
      },
    }),

    search_data: tool({
      description:
        "Semantic search across all ministry person profiles using AI embeddings. " +
        "Use this for qualitative/discovery questions: finding people by behavior patterns, " +
        "searching across tags/events/notes, or discovering relationships. " +
        "Returns the most relevant person profiles ranked by semantic similarity. " +
        "Use query_data (SQL) for quantitative questions (counts, sums, trends, rankings). " +
        "Use this tool for questions like 'find donors interested in Bible studies', " +
        "'who are our most multi-channel supporters', 'contacts similar to [name]'.",
      inputSchema: z.object({
        query: z.string().describe("Natural language search query describing what you're looking for"),
        top: z.number().optional().default(10).describe("Number of results to return (1-50)"),
        filter: z.string().optional().describe("Optional OData filter (e.g. \"lifecycle_stage eq 'active'\", \"giving_total gt 1000\")"),
      }),
      execute: async ({ query, top, filter }) => {
        const result = await hybridSearch(query, Math.min(top ?? 10, 50), filter);
        if (!result.ok) {
          return { ok: false as const, error: result.error, results: [] };
        }
        // Store results for show_widget to use
        lastQueryRows = result.results.map((r) => ({
          display_name: r.display_name,
          email: r.email,
          location: r.location,
          lifecycle_stage: r.lifecycle_stage,
          giving_total: r.giving_total,
          order_count: r.order_count,
          event_count: r.event_count,
          has_subscription: r.has_subscription,
          relevance: Math.round(r.score * 100) / 100,
          profile: r.content.slice(0, 500),
        }));
        lastQuerySql = `[Semantic Search: "${query}"]`;
        return {
          ok: true as const,
          totalCount: result.totalCount,
          results: result.results.map((r) => ({
            display_name: r.display_name,
            email: r.email,
            location: r.location,
            lifecycle_stage: r.lifecycle_stage,
            giving_total: r.giving_total,
            order_count: r.order_count,
            event_count: r.event_count,
            has_subscription: r.has_subscription,
            profile_summary: r.content.slice(0, 800),
          })),
        };
      },
    }),

    show_widget: tool({
      description:
        "Display a visual widget (chart, KPI, table, etc.) to the user. " +
        "Call query_data first to get data, then call this to render it. " +
        "Data rows are automatically inherited from the last query_data call — " +
        "you do NOT need to pass data rows. Just pass type, title, and config. " +
        "The widget will appear inline in the chat response.",
      inputSchema: z.object({
        type: z
          .enum([
            "kpi",
            "bar_chart",
            "line_chart",
            "area_chart",
            "donut_chart",
            "table",
            "drill_down_table",
            "funnel",
            "stat_grid",
            "text",
          ])
          .describe("The type of widget to display"),
        title: z.string().describe("Widget title"),
        data: z
          .array(z.record(z.string(), z.unknown()))
          .optional()
          .default([])
          .describe("Data rows (optional — automatically uses last query_data result if empty)"),
        config: z
          .object({
            categoryKey: z.string().optional(),
            valueKeys: z.array(z.string()).optional(),
            seriesKey: z.string().optional().describe("Column to split into separate series (auto-pivots long-format data). E.g. seriesKey='display_name' turns rows into one line/bar per person."),
            groupKey: z.string().optional().describe("Column to group rows by for drill_down_table. Click a group to expand detail rows."),
            summaryColumns: z.array(z.string()).optional().describe("Columns to show in summary row of drill_down_table."),
            detailColumns: z.array(z.string()).optional().describe("Columns to show in expanded detail rows of drill_down_table."),
            valueLabels: z.record(z.string(), z.string()).optional(),
            colors: z.record(z.string(), z.string()).optional(),
            value: z.union([z.string(), z.number()]).optional(),
            delta: z.union([z.string(), z.number()]).optional(),
            trend: z.enum(["up", "down", "flat"]).optional(),
            unit: z.string().optional(),
            markdown: z.string().optional(),
            stats: z
              .array(
                z.object({
                  label: z.string(),
                  value: z.union([z.string(), z.number()]),
                  unit: z.string().optional(),
                  trend: z.enum(["up", "down", "flat"]).optional(),
                }).passthrough(),
              )
              .optional(),
            numberFormat: z.enum(["currency", "percent", "number"]).optional(),
          })
          .passthrough()
          .describe("Widget display configuration"),
        sql: z
          .string()
          .optional()
          .describe("The SQL query that produced this data (for reference)"),
      }),
      execute: async ({ type, title, data, config, sql: sqlQuery }) => {
        // Use provided data if non-empty, then try matching by normalized SQL key, then fall back
        const widgetSql = sqlQuery ?? lastQuerySql;
        const normalizedKey = widgetSql ? sqlKey(widgetSql) : undefined;
        const widgetData = (data && data.length > 0)
          ? data
          : (normalizedKey && queryResultMap.has(normalizedKey))
            ? queryResultMap.get(normalizedKey)!
            : lastQueryRows;

        const widget: Widget = {
          id: crypto.randomUUID(),
          type: type as WidgetType,
          title,
          data: widgetData as Record<string, unknown>[],
          config: config as WidgetConfig,
          sql: widgetSql,
          createdAt: new Date().toISOString(),
        };
        return { widget };
      },
    }),

    save_insight: tool({
      description:
        "Save a specific data finding from your analysis. " +
        "Use when a query reveals something notable — a trend, risk, anomaly, or opportunity. " +
        "These expire after 30 days. For permanent knowledge (corrections, user preferences, learnings), use update_memory instead.",
      inputSchema: z.object({
        text: z
          .string()
          .describe("The insight text — a concise, actionable finding (1-2 sentences max)"),
        category: z
          .enum(["giving", "commerce", "events", "subscriptions", "engagement", "wealth", "risk", "opportunity", "general"])
          .describe("Category for the insight"),
        confidence: z
          .number()
          .min(0)
          .max(1)
          .default(0.8)
          .describe("How confident you are in this insight (0-1)"),
        source_query: z
          .string()
          .optional()
          .describe("The SQL query or search that produced this insight"),
      }),
      execute: async ({ text, category, confidence, source_query }) => {
        const result = await saveInsight(text, category, confidence, source_query, ownerEmail);
        return result;
      },
    }),

    save_knowledge: tool({
      description:
        "Save a specific piece of knowledge you've learned. Use when the user " +
        "corrects you, expresses a preference, or when you discover a reusable pattern. " +
        "Each call saves ONE atomic fact. Do NOT try to save everything at once. " +
        "For data findings from queries, use save_insight instead.",
      inputSchema: z.object({
        category: z
          .enum(["correction", "preference", "pattern", "fact", "persona"])
          .describe(
            "correction: user corrected you. preference: how user likes data presented. " +
            "pattern: reusable data pattern. fact: organizational fact. persona: about this user.",
          ),
        content: z
          .string()
          .max(500)
          .describe("The specific thing to remember. One clear sentence."),
        confidence: z
          .number()
          .min(0)
          .max(1)
          .default(0.9)
          .describe("How confident (1.0 = user explicitly stated, 0.7 = inferred)"),
        supersedes: z
          .string()
          .optional()
          .describe("If correcting previous knowledge, the ID of the old item"),
      }),
      execute: async ({ category, content, confidence, supersedes }) => {
        if (!ownerEmail) return { ok: false, error: "No user session" };
        const result = await saveKnowledge(ownerEmail, category, content, confidence, supersedes);
        return result;
      },
    }),

    create_action: tool({
      description:
        "Create an action item in the user's action queue. Use this when your analysis " +
        "reveals something the user should DO — a person to call, thank, re-engage, or review. " +
        "Actions appear in the /actions page sorted by priority. " +
        "Create actions proactively when you spot opportunities or risks in the data.",
      inputSchema: z.object({
        title: z.string().max(500).describe("Clear, actionable title (e.g., 'Call Kay Barker — 427 days silent, $160K lifetime')"),
        description: z.string().max(2000).optional().describe("Additional context about why this action matters"),
        action_type: z
          .enum(["call", "email", "thank", "reengage", "review", "general"])
          .default("general")
          .describe("Type of action"),
        priority_score: z
          .number()
          .min(0)
          .max(100)
          .default(50)
          .describe("Priority 0-100 (higher = more urgent). 90+ for critical revenue at risk, 70-89 for important, 50-69 moderate"),
        person_name: z.string().optional().describe("Name of the person this action relates to"),
        due_date: z.string().optional().describe("Suggested due date (YYYY-MM-DD)"),
      }),
      execute: async ({ title, description, action_type, priority_score, person_name, due_date }) => {
        if (!ownerEmail) return { ok: false, error: "No user session" };
        const id = crypto.randomUUID();
        const esc = (s: string) => s.replace(/'/g, "''");
        const result = await execSqlDirect(`
          INSERT INTO sozo.action (id, owner_email, title, description, action_type, priority_score, person_name, source, due_date)
          VALUES (
            '${id}',
            N'${esc(ownerEmail)}',
            N'${esc(title.slice(0, 500))}',
            ${description ? `N'${esc(description.slice(0, 2000))}'` : "NULL"},
            N'${esc(action_type)}',
            ${priority_score},
            ${person_name ? `N'${esc(person_name.slice(0, 256))}'` : "NULL"},
            'ai',
            ${due_date ? `'${esc(due_date)}'` : "NULL"}
          )
        `);
        if (!result.ok) return { ok: false, error: result.reason };
        return { ok: true, id, message: `Action created: "${title}"` };
      },
    }),

    draft_email: tool({
      description:
        "Draft a personalized outreach email for the user to review. " +
        "Use person context from your analysis to write a relevant, warm email. " +
        "Returns a draft — NEVER auto-sends. The user reviews and sends manually. " +
        "Great for: thank you notes, re-engagement, upgrade asks, event invitations.",
      inputSchema: z.object({
        purpose: z
          .enum(["thank_you", "reengagement", "upgrade_ask", "event_invite", "general"])
          .describe("The purpose of the email"),
        person_name: z.string().describe("Recipient's name"),
        person_email: z.string().optional().describe("Recipient's email (if known)"),
        context: z.string().describe("Key context about this person (giving history, relationship, what prompted this outreach)"),
        tone: z
          .enum(["warm", "professional", "casual", "grateful"])
          .default("warm")
          .describe("Email tone"),
      }),
      execute: async ({ purpose, person_name, person_email, context, tone }) => {
        // Draft the email using a template approach (no API call — the LLM itself
        // generates the draft in its response based on our instructions)
        const purposeLabels: Record<string, string> = {
          thank_you: "Thank You",
          reengagement: "Re-engagement",
          upgrade_ask: "Giving Upgrade",
          event_invite: "Event Invitation",
          general: "Outreach",
        };

        return {
          ok: true,
          draft: {
            to: person_email ?? `[${person_name}'s email]`,
            subject: `[Draft ${purposeLabels[purpose]}]`,
            purpose,
            person_name,
            context,
            tone,
            instructions: `Generate a ${tone} ${purposeLabels[purpose]} email to ${person_name} based on this context: ${context}. Keep it personal, brief (3-4 paragraphs max), and include a specific call to action. Sign from the ministry team.`,
          },
          message: `Email draft prepared for ${person_name}. Review the draft below and customize before sending.`,
        };
      },
    }),
  };
}
