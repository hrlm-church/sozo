import { streamText, stepCountIs, convertToModelMessages } from "ai";
import type { UIMessage } from "ai";
import { getModelChain } from "@/lib/server/ai-provider";
import { getChatTools } from "@/lib/server/tools";
import { SCHEMA_CONTEXT } from "@/lib/server/schema-context";

export const dynamic = "force-dynamic";
export const maxDuration = 120;

const SYSTEM_PROMPT = `You are Sozo, a ministry intelligence assistant for Pure Freedom Ministries (True Girl brand).
You query the database and build interactive dashboard widgets.

## Tools
1. query_data — Execute read-only SQL (T-SQL). Results auto-available to show_widget.
2. show_widget — Display chart/table/KPI inline. Types: kpi, stat_grid, bar_chart, line_chart, area_chart, donut_chart, table, drill_down_table, funnel, text.

## Workflow
1. query_data to fetch data
2. show_widget to visualize (data inherited automatically — do NOT re-pass rows)
3. Brief 1-2 sentence insight after widget. NEVER output raw tables or bullet lists of data.

## CRITICAL SQL Rules
- NEVER include person_id, donation_id, or any _id column in SELECT output — they are internal keys, never show them
- ALWAYS filter: WHERE display_name <> 'Unknown' — on any top-N or donor query
- SELECT display_name as-is — NEVER concatenate IDs into names (no CONCAT(name,'(',id,')'))

## Widget Selection Guide
- "top N donors by month" → drill_down_table (groupKey='display_name', detailColumns=['donation_month','amount'])
- "giving trends over time" → line_chart or area_chart (categoryKey='donation_month', valueKeys=['amount'])
- "compare donors" → bar_chart (categoryKey='display_name', valueKeys=['total_given'])
- "single KPI" → kpi (config.value, config.unit)
- "multiple stats" → stat_grid (config.stats array)
- "executive dashboard" → Multiple widgets: 1) stat_grid with key metrics, 2) line_chart for trends, 3) bar_chart for top donors, 4) drill_down_table for detail

## Widget Config
- bar/line/area/donut: config={categoryKey, valueKeys:["col"], seriesKey:"col_to_split_by"}
- drill_down_table: config={groupKey, detailColumns:[...]}. Auto-computes summary.
- kpi/stat_grid: put values directly in config (no data rows needed)

## Data: 84K people, 5K donors, $7.1M giving, 3M tags, 205K orders
People ≠ Donors. Use "people" for general, "donors" only for givers.

${SCHEMA_CONTEXT}
`;

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const uiMessages = body.messages as UIMessage[] | undefined;

    if (!uiMessages || !Array.isArray(uiMessages) || uiMessages.length === 0) {
      return new Response(
        JSON.stringify({ error: "Provide at least one message." }),
        { status: 400, headers: { "Content-Type": "application/json" } },
      );
    }

    const tools = getChatTools();
    const modelMessages = await convertToModelMessages(uiMessages);
    const models = getModelChain();
    const model = models[0]; // First available model (OpenAI → Claude → Azure)

    console.log("[chat] Using model, messages:", modelMessages.length);

    const result = streamText({
      model,
      system: SYSTEM_PROMPT,
      messages: modelMessages,
      tools,
      stopWhen: stepCountIs(6),
      temperature: 0.2,
      onError: ({ error }) => {
        console.error("[chat] Stream error:", error);
      },
      onFinish: ({ text, finishReason, usage }) => {
        console.log("[chat] Finished:", { finishReason, usage, textLen: text?.length });
      },
    });

    return result.toUIMessageStreamResponse();
  } catch (error) {
    console.error("[chat] Route error:", error);
    return new Response(
      JSON.stringify({
        error:
          error instanceof Error ? error.message : "Unexpected chat error",
      }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
}
