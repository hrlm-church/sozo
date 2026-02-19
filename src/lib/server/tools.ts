import { tool } from "ai";
import { z } from "zod";
import { guardSql, QUERY_TIMEOUT_MS } from "@/lib/server/sql-guard";
import { executeSql } from "@/lib/server/sql-client";
import { hybridSearch } from "@/lib/server/search-client";
import type { Widget, WidgetType, WidgetConfig } from "@/types/widget";

/**
 * All LLM tools for the chat API.
 *
 * Each call creates a fresh closure so query results can be shared
 * between query_data → show_widget within one request without forcing
 * the LLM to re-emit every row as tool-call arguments (saves tokens,
 * avoids truncation on large result sets).
 */
export function getChatTools() {
  // Shared state: last successful query result, keyed by sql string
  let lastQueryRows: Record<string, unknown>[] = [];
  let lastQuerySql: string | undefined;

  return {
    query_data: tool({
      description:
        "Execute a read-only SQL SELECT query against Azure SQL. " +
        "Returns up to 500 rows. Use this to fetch data before showing a widget. " +
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
        // Store for show_widget to use
        lastQueryRows = result.rows;
        lastQuerySql = guard.sanitized;
        return {
          ok: true as const,
          rowCount: result.rows.length,
          data: result.rows,
          sql: guard.sanitized,
          purpose,
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
        // Use provided data if non-empty, otherwise fall back to last query result
        const widgetData = (data && data.length > 0) ? data : lastQueryRows;
        const widgetSql = sqlQuery ?? lastQuerySql;

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
  };
}
