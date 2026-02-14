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

## Widget Config
- bar/line/area/donut: config={categoryKey, valueKeys:["col"], seriesKey:"col_to_split_by"}
- drill_down_table: config={groupKey:"display_name", detailColumns:["month","amount"]}. Auto-computes summary. Use for "top donors by month" questions.
- kpi/stat_grid: put values directly in config (no data rows needed)
- Colors: "#0693e3","#9b51e0","#17c6b8","#f59e0b","#f43f5e","#10b981","#ec4899","#f97316"

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

    // Try each model in priority order (Claude → OpenAI → Azure)
    let lastError: unknown = null;
    for (const model of models) {
      try {
        console.log("[chat] Trying model:", String(model) ?? "unknown");

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
            console.log("[chat] Finished:", { model: String(model), finishReason, usage, textLen: text?.length });
          },
        });

        return result.toUIMessageStreamResponse();
      } catch (err) {
        lastError = err;
        const msg = err instanceof Error ? err.message : String(err);
        console.warn(`[chat] Model failed (${String(model)}): ${msg.substring(0, 200)}`);
        // Continue to next model
      }
    }

    // All models failed
    throw lastError ?? new Error("All AI providers failed");
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
