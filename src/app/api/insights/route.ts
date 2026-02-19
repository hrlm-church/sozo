import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

interface InsightRequest {
  text: string;
  category?: string;
  confidence?: number;
  sourceQuery?: string;
}

/** Save a new insight */
export async function POST(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = (await request.json()) as InsightRequest;
    const { text, category, confidence, sourceQuery } = body;

    if (!text || text.length < 10) {
      return NextResponse.json({ error: "Insight text required (min 10 chars)" }, { status: 400 });
    }

    const id = crypto.randomUUID();
    const conf = Math.min(Math.max(confidence ?? 0.8, 0), 1);

    await executeSql(`
      INSERT INTO sozo.insight (id, insight_text, category, confidence, source_query, owner_email, expires_at)
      VALUES (
        '${esc(id)}',
        N'${esc(text.slice(0, 1000))}',
        ${category ? `N'${esc(category.slice(0, 100))}'` : "NULL"},
        ${conf},
        ${sourceQuery ? `N'${esc(sourceQuery.slice(0, 4000))}'` : "NULL"},
        N'${esc(ownerEmail)}',
        DATEADD(day, 30, SYSUTCDATETIME())
      )
    `);

    return NextResponse.json({ id, saved: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Insight save failed" },
      { status: 500 },
    );
  }
}

/** Get recent insights for the current user (non-expired) */
export async function GET() {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const result = await executeSql(`
      SELECT TOP (50) id, insight_text, category, confidence, created_at
      FROM sozo.insight
      WHERE owner_email = N'${esc(ownerEmail)}'
        AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
      ORDER BY created_at DESC
    `);
    return NextResponse.json({ insights: result.ok ? result.rows : [] });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Insight load failed" },
      { status: 500 },
    );
  }
}
