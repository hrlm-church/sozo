import { NextResponse } from "next/server";
import { getDashboardSummary } from "@/lib/server/dashboard-summary";
import { ChatMessage, runAzureOpenAiChat } from "@/lib/server/azure-openai";
import { runSearchQuery } from "@/lib/server/search-query";
import { runSqlQuery } from "@/lib/server/sql-query";

export const dynamic = "force-dynamic";

type ToolRoute = "dashboard_summary" | "search" | "sql";

interface ChatRequestBody {
  messages?: ChatMessage[];
  householdId?: string;
  personId?: string;
}

const selectRoute = (latestUserMessage: string): { tool: ToolRoute; reason: string } => {
  const normalized = latestUserMessage.toLowerCase();

  if (/\bsql\b/.test(normalized)) {
    return {
      tool: "sql",
      reason: "Explicit SQL keyword detected",
    };
  }

  if (
    /(dashboard|summary|kpi|metric|trend|risk|utilization|payment|household)/.test(
      normalized,
    )
  ) {
    return {
      tool: "dashboard_summary",
      reason: "Dashboard and KPI language detected",
    };
  }

  if (/(search|index|citation|document|source)/.test(normalized)) {
    return {
      tool: "search",
      reason: "Search-oriented language detected",
    };
  }

  return {
    tool: "sql",
    reason: "Defaulting to SQL route for structured data requests",
  };
};

const fallbackAnswer = (tool: ToolRoute, latestPrompt: string, householdId?: string, personId?: string) => {
  const scope = [personId ? `person=${personId}` : null, householdId ? `household=${householdId}` : null]
    .filter(Boolean)
    .join(", ");

  const scopeText = scope ? ` Scope: ${scope}.` : "";

  if (tool === "dashboard_summary") {
    return `Dashboard summary prepared for: \"${latestPrompt}\".${scopeText}`;
  }
  if (tool === "search") {
    return `Azure Search results prepared for: \"${latestPrompt}\".${scopeText}`;
  }
  return `Azure SQL results prepared for: \"${latestPrompt}\".${scopeText}`;
};

export async function POST(request: Request) {
  try {
    const body = (await request.json()) as ChatRequestBody;
    const messages = body.messages ?? [];
    const latestUserMessage = [...messages]
      .reverse()
      .find((message) => message.role === "user")?.content;

    if (!latestUserMessage) {
      return NextResponse.json(
        {
          error: "Provide at least one user message in messages[].",
        },
        { status: 400 },
      );
    }

    const route = selectRoute(latestUserMessage);
    const summary = await getDashboardSummary();

    const searchResult = route.tool === "search" ? await runSearchQuery(latestUserMessage) : null;
    const sqlResult = route.tool === "sql" ? await runSqlQuery(latestUserMessage) : null;

    const routeCitations =
      route.tool === "dashboard_summary"
        ? summary.citations
        : route.tool === "search"
          ? searchResult?.citations ?? []
          : sqlResult?.citations ?? [];

    const routeTables =
      route.tool === "dashboard_summary"
        ? summary.tables
        : route.tool === "search"
          ? searchResult?.table
            ? [searchResult.table]
            : []
          : sqlResult?.table
            ? [sqlResult.table]
            : [];

    const systemPrompt: ChatMessage = {
      role: "system",
      content:
        "You are Sozo, a person/household-centric assistant. Keep answers concise and factual. If tool results are provided and marked ok=true, you must answer from those results and must not say you cannot access/search/query data. If tool results are not available, clearly state the specific missing dependency.",
    };

    const contextPrompt: ChatMessage = {
      role: "system",
      content: JSON.stringify(
        {
          toolRoute: route,
          dashboardSummary:
            route.tool === "dashboard_summary"
              ? {
                  metrics: summary.metrics,
                  citations: summary.citations,
                }
              : undefined,
          searchResult:
            route.tool === "search"
              ? {
                  ok: searchResult?.ok ?? false,
                  reason: searchResult?.reason,
                  indexName: searchResult?.indexName,
                  citations: searchResult?.citations ?? [],
                  table: searchResult?.table,
                }
              : undefined,
          sqlResult:
            route.tool === "sql"
              ? {
                  ok: sqlResult?.ok ?? false,
                  reason: sqlResult?.reason,
                  query: sqlResult?.query,
                  citations: sqlResult?.citations ?? [],
                  table: sqlResult?.table,
                }
              : undefined,
          requestScope: {
            personId: body.personId,
            householdId: body.householdId,
          },
        },
        null,
        2,
      ),
    };

    const modelResult = await runAzureOpenAiChat([systemPrompt, contextPrompt, ...messages]);

    return NextResponse.json({
      answer:
        modelResult.ok && modelResult.content
          ? modelResult.content
          : fallbackAnswer(route.tool, latestUserMessage, body.householdId, body.personId),
      citations: routeCitations,
      artifacts: {
        charts: route.tool === "dashboard_summary" ? summary.charts : [],
        tables: routeTables,
      },
      route,
      meta: {
        usedModel: modelResult.ok,
        model: modelResult.model,
        modelError: modelResult.ok ? undefined : modelResult.error,
        toolStatus:
          route.tool === "search"
            ? { ok: searchResult?.ok ?? false, reason: searchResult?.reason }
            : route.tool === "sql"
              ? { ok: sqlResult?.ok ?? false, reason: sqlResult?.reason }
              : { ok: true },
      },
      timestamp: new Date().toISOString(),
    });
  } catch (error) {
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Unexpected chat error",
      },
      { status: 500 },
    );
  }
}
