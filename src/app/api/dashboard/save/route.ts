import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import type { Widget, WidgetLayout } from "@/types/widget";

export const dynamic = "force-dynamic";

interface SaveRequest {
  id?: string | null;
  name: string;
  widgets: Widget[];
  layouts: WidgetLayout[];
}

export async function POST(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = (await request.json()) as SaveRequest;
    const { name, widgets, layouts } = body;
    const dashboardId = body.id ?? crypto.randomUUID();

    // Upsert dashboard
    const upsertResult = await executeSql(`
      IF EXISTS (SELECT 1 FROM dashboard.saved_dashboard WHERE id = '${esc(dashboardId)}')
      BEGIN
        UPDATE dashboard.saved_dashboard
        SET name = N'${esc(name)}', updated_at = SYSUTCDATETIME()
        WHERE id = '${esc(dashboardId)}'
      END
      ELSE
      BEGIN
        INSERT INTO dashboard.saved_dashboard (id, name, owner_email)
        VALUES ('${esc(dashboardId)}', N'${esc(name)}', N'${esc(ownerEmail)}')
      END
    `);

    if (!upsertResult.ok) {
      return NextResponse.json({ error: upsertResult.reason }, { status: 500 });
    }

    // Delete existing widgets for this dashboard
    await executeSql(
      `DELETE FROM dashboard.widget WHERE dashboard_id = '${esc(dashboardId)}'`,
    );

    // Insert widgets
    for (const widget of widgets) {
      const layout = layouts.find((l) => l.i === widget.id);
      await executeSql(`
        INSERT INTO dashboard.widget (id, dashboard_id, type, title, sql_query, config_json, data_json, layout_x, layout_y, layout_w, layout_h)
        VALUES (
          '${esc(widget.id)}',
          '${esc(dashboardId)}',
          '${esc(widget.type)}',
          N'${esc(widget.title)}',
          ${widget.sql ? `N'${esc(widget.sql)}'` : "NULL"},
          N'${esc(JSON.stringify(widget.config))}',
          N'${esc(JSON.stringify(widget.data))}',
          ${layout?.x ?? 0},
          ${layout?.y ?? 0},
          ${layout?.w ?? 6},
          ${layout?.h ?? 4}
        )
      `);
    }

    return NextResponse.json({ id: dashboardId, saved: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Save failed" },
      { status: 500 },
    );
  }
}

function esc(val: string): string {
  return val.replace(/'/g, "''");
}
