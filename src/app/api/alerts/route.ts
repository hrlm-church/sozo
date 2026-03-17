/**
 * /api/alerts — Alert management.
 *
 * GET   — list alerts (with unread count)
 * PATCH — mark alerts as read
 */

import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

export async function GET(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const url = new URL(request.url);
    const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "20", 10), 100);
    const unreadOnly = url.searchParams.get("unread") === "1";

    const where = `WHERE owner_email = N'${esc(ownerEmail)}'${unreadOnly ? " AND is_read = 0" : ""}`;

    const [alertsResult, countResult] = await Promise.all([
      executeSql(`
        SELECT TOP (${limit})
          id, alert_type, severity, title, body, person_id, person_name, is_read, created_at
        FROM sozo.alert
        ${where}
        ORDER BY created_at DESC
      `, 15000),
      executeSql(`
        SELECT COUNT(*) AS unread_count
        FROM sozo.alert
        WHERE owner_email = N'${esc(ownerEmail)}' AND is_read = 0
      `, 15000),
    ]);

    return NextResponse.json({
      alerts: alertsResult.ok ? alertsResult.rows : [],
      unread_count: countResult.ok && countResult.rows.length > 0
        ? (countResult.rows[0] as Record<string, unknown>).unread_count
        : 0,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to load alerts" },
      { status: 500 },
    );
  }
}

export async function PATCH(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = await request.json();

    if (body.mark_all_read) {
      await executeSql(`
        UPDATE sozo.alert SET is_read = 1
        WHERE owner_email = N'${esc(ownerEmail)}' AND is_read = 0
      `);
      return NextResponse.json({ ok: true });
    }

    if (body.id) {
      await executeSql(`
        UPDATE sozo.alert SET is_read = 1
        WHERE id = '${esc(String(body.id))}' AND owner_email = N'${esc(ownerEmail)}'
      `);
      return NextResponse.json({ ok: true });
    }

    return NextResponse.json({ error: "Provide id or mark_all_read" }, { status: 400 });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to update alert" },
      { status: 500 },
    );
  }
}
