import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import type { Widget, WidgetLayout, WidgetConfig } from "@/types/widget";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const { searchParams } = new URL(request.url);
    const id = searchParams.get("id");

    // List dashboards for current user
    if (!id) {
      const result = await executeSql(`
        SELECT id, name, owner_email, created_at, updated_at
        FROM dashboard.saved_dashboard
        WHERE owner_email = N'${esc(ownerEmail)}'
        ORDER BY updated_at DESC
      `);
      if (!result.ok) {
        return NextResponse.json({ error: result.reason }, { status: 500 });
      }
      return NextResponse.json({ dashboards: result.rows });
    }

    // Load specific dashboard (scoped to current user)
    const dashResult = await executeSql(`
      SELECT id, name, owner_email, created_at, updated_at
      FROM dashboard.saved_dashboard
      WHERE id = '${esc(id)}' AND owner_email = N'${esc(ownerEmail)}'
    `);
    if (!dashResult.ok || dashResult.rows.length === 0) {
      return NextResponse.json({ error: "Dashboard not found" }, { status: 404 });
    }

    const dashboard = dashResult.rows[0];

    const widgetResult = await executeSql(`
      SELECT id, type, title, sql_query, config_json, data_json, layout_x, layout_y, layout_w, layout_h
      FROM dashboard.widget
      WHERE dashboard_id = '${esc(id)}'
    `);

    const widgets: Widget[] = [];
    const layouts: WidgetLayout[] = [];

    for (const row of widgetResult.rows) {
      widgets.push({
        id: String(row.id),
        type: String(row.type) as Widget["type"],
        title: String(row.title),
        sql: row.sql_query ? String(row.sql_query) : undefined,
        config: safeJsonParse<WidgetConfig>(row.config_json, {}),
        data: safeJsonParse<Record<string, unknown>[]>(row.data_json, []),
        createdAt: new Date().toISOString(),
      });

      layouts.push({
        i: String(row.id),
        x: Number(row.layout_x) || 0,
        y: Number(row.layout_y) || 0,
        w: Number(row.layout_w) || 6,
        h: Number(row.layout_h) || 4,
      });
    }

    return NextResponse.json({
      id: dashboard.id,
      name: dashboard.name,
      widgets,
      layouts,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Load failed" },
      { status: 500 },
    );
  }
}

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

function safeJsonParse<T>(val: unknown, fallback: T): T {
  if (!val) return fallback;
  try {
    return JSON.parse(String(val)) as T;
  } catch {
    return fallback;
  }
}
