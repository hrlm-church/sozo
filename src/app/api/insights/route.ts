import { NextResponse } from "next/server";
import { executeSqlSafe } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import { withAuditLog } from "@/lib/server/audit";
import { z } from "zod";

export const dynamic = "force-dynamic";

const InsightSchema = z.object({
  text: z.string().min(10).max(1000),
  category: z.string().max(100).optional(),
  confidence: z.number().min(0).max(1).optional(),
  sourceQuery: z.string().max(4000).optional(),
});

/** Save a new insight */
export const POST = withAuditLog("/api/insights", async function POST(request: Request) {
  try {
    const ownerEmail = await getSessionEmail();
    if (!ownerEmail) {
      return NextResponse.json({ error: "Authentication required" }, { status: 401 });
    }

    const body = await request.json();
    const parsed = InsightSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: "Insight text required (min 10 chars)", details: parsed.error.issues },
        { status: 400 },
      );
    }

    const { text, category, confidence, sourceQuery } = parsed.data;
    const id = crypto.randomUUID();
    const conf = Math.min(Math.max(confidence ?? 0.8, 0), 1);

    await executeSqlSafe(
      `INSERT INTO sozo.insight (id, insight_text, category, confidence, source_query, owner_email, expires_at)
       VALUES (@id, @text, @category, @conf, @sourceQuery, @email, DATEADD(day, 30, SYSUTCDATETIME()))`,
      {
        id,
        text,
        category: category ?? null,
        conf,
        sourceQuery: sourceQuery ?? null,
        email: ownerEmail,
      },
    );

    return NextResponse.json({ id, saved: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Insight save failed" },
      { status: 500 },
    );
  }
});

/** Get recent non-expired insights for the current user */
export const GET = withAuditLog("/api/insights", async function GET() {
  try {
    const ownerEmail = await getSessionEmail();
    if (!ownerEmail) {
      return NextResponse.json({ error: "Authentication required" }, { status: 401 });
    }

    const result = await executeSqlSafe(
      `SELECT TOP (50) id, insight_text, category, confidence, created_at
       FROM sozo.insight
       WHERE owner_email = @email
         AND (expires_at IS NULL OR expires_at > SYSUTCDATETIME())
       ORDER BY created_at DESC`,
      { email: ownerEmail },
    );
    return NextResponse.json({ insights: result.ok ? result.rows : [] });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Insight load failed" },
      { status: 500 },
    );
  }
});
