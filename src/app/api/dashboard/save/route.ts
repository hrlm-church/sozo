import { NextResponse } from "next/server";
import { withTransaction } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import { withAuditLog } from "@/lib/server/audit";
import { z } from "zod";

export const dynamic = "force-dynamic";

const DashboardSaveSchema = z.object({
  id: z.string().max(100).optional().nullable(),
  name: z.string().min(1).max(256),
  widgets: z
    .array(
      z.object({
        id: z.string(),
        type: z.string(),
        title: z.string(),
        sql: z.string().optional(),
        config: z.record(z.string(), z.unknown()),
        data: z.array(z.record(z.string(), z.unknown())).optional().default([]),
        createdAt: z.string().optional(),
      }),
    )
    .max(50),
  layouts: z
    .array(
      z.object({
        i: z.string(),
        x: z.number(),
        y: z.number(),
        w: z.number(),
        h: z.number(),
      }),
    )
    .max(50),
});

export const POST = withAuditLog("/api/dashboard/save", async function POST(request: Request) {
  try {
    const ownerEmail = await getSessionEmail();
    if (!ownerEmail) {
      return NextResponse.json({ error: "Authentication required" }, { status: 401 });
    }

    const body = await request.json();
    const parsed = DashboardSaveSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: "Invalid request", details: parsed.error.issues },
        { status: 400 },
      );
    }

    const { name, widgets, layouts } = parsed.data;
    const dashboardId = parsed.data.id ?? crypto.randomUUID();

    await withTransaction(async (exec) => {
      // Upsert dashboard
      await exec(
        `IF EXISTS (SELECT 1 FROM dashboard.saved_dashboard WHERE id = @id)
         BEGIN
           UPDATE dashboard.saved_dashboard
           SET name = @name, updated_at = SYSUTCDATETIME()
           WHERE id = @id
         END
         ELSE
         BEGIN
           INSERT INTO dashboard.saved_dashboard (id, name, owner_email)
           VALUES (@id, @name, @email)
         END`,
        { id: dashboardId, name, email: ownerEmail },
      );

      // Delete existing widgets
      await exec(
        `DELETE FROM dashboard.widget WHERE dashboard_id = @dashId`,
        { dashId: dashboardId },
      );

      // Insert each widget
      for (const widget of widgets) {
        const layout = layouts.find((l) => l.i === widget.id);
        await exec(
          `INSERT INTO dashboard.widget
           (id, dashboard_id, type, title, sql_query, config_json, data_json,
            layout_x, layout_y, layout_w, layout_h)
           VALUES (@wid, @dashId, @type, @title, @sqlQuery, @config, @data,
                   @x, @y, @w, @h)`,
          {
            wid: widget.id,
            dashId: dashboardId,
            type: widget.type,
            title: widget.title,
            sqlQuery: widget.sql ?? null,
            config: JSON.stringify(widget.config),
            data: JSON.stringify(widget.data ?? []),
            x: layout?.x ?? 0,
            y: layout?.y ?? 0,
            w: layout?.w ?? 6,
            h: layout?.h ?? 4,
          },
        );
      }
    });

    return NextResponse.json({ id: dashboardId, saved: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Save failed" },
      { status: 500 },
    );
  }
});
