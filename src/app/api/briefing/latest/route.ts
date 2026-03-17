/**
 * GET /api/briefing/latest
 *
 * Returns the most recent briefing for the current user.
 */

import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "system@sozo.local";

    const result = await executeSql(`
      SELECT TOP (1) id, owner_email, briefing_date, content_json, metrics_json, action_count, created_at
      FROM sozo.briefing
      WHERE owner_email = N'${ownerEmail.replace(/'/g, "''")}'
      ORDER BY briefing_date DESC, created_at DESC
    `, 15000);

    if (!result.ok || result.rows.length === 0) {
      return NextResponse.json({ briefing: null });
    }

    const row = result.rows[0] as Record<string, unknown>;
    return NextResponse.json({
      briefing: {
        id: row.id,
        date: row.briefing_date,
        content: JSON.parse(String(row.content_json)),
        metrics: row.metrics_json ? JSON.parse(String(row.metrics_json)) : null,
        action_count: row.action_count,
        created_at: row.created_at,
      },
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to load briefing" },
      { status: 500 },
    );
  }
}
