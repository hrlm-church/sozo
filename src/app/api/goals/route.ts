/**
 * /api/goals — Goal tracking CRUD.
 *
 * GET    — list goals for current user
 * POST   — create a new goal
 * PATCH  — update a goal
 * DELETE — delete a goal
 */

import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

export async function GET() {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";

    const result = await executeSql(`
      SELECT id, title, goal_type, target_value, current_value, unit, target_date, status, created_at, updated_at
      FROM sozo.goal
      WHERE owner_email = N'${esc(ownerEmail)}'
      ORDER BY
        CASE status WHEN 'active' THEN 1 WHEN 'paused' THEN 2 ELSE 3 END,
        created_at DESC
    `, 15000);

    return NextResponse.json({ goals: result.ok ? result.rows : [] });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to load goals" },
      { status: 500 },
    );
  }
}

export async function POST(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = await request.json();

    const id = crypto.randomUUID();
    const title = esc(String(body.title ?? "Untitled goal").slice(0, 500));
    const goalType = esc(String(body.goal_type ?? "custom").slice(0, 50));
    const targetValue = Number(body.target_value ?? 0);
    const currentValue = Number(body.current_value ?? 0);
    const unit = body.unit ? esc(String(body.unit).slice(0, 20)) : null;
    const metricQuery = body.metric_query ? esc(String(body.metric_query).slice(0, 4000)) : null;
    const targetDate = body.target_date ? esc(String(body.target_date)) : null;

    const result = await executeSql(`
      INSERT INTO sozo.goal (id, owner_email, title, goal_type, target_value, current_value, unit, metric_query, target_date)
      VALUES (
        '${id}',
        N'${esc(ownerEmail)}',
        N'${title}',
        N'${goalType}',
        ${targetValue},
        ${currentValue},
        ${unit ? `N'${unit}'` : "NULL"},
        ${metricQuery ? `N'${metricQuery}'` : "NULL"},
        ${targetDate ? `'${targetDate}'` : "NULL"}
      )
    `);

    if (!result.ok) {
      return NextResponse.json({ error: result.reason }, { status: 500 });
    }

    return NextResponse.json({ ok: true, id });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to create goal" },
      { status: 500 },
    );
  }
}

export async function PATCH(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = await request.json();

    if (!body.id) {
      return NextResponse.json({ error: "id is required" }, { status: 400 });
    }

    const updates: string[] = [];
    if (body.title) updates.push(`title = N'${esc(String(body.title).slice(0, 500))}'`);
    if (body.target_value !== undefined) updates.push(`target_value = ${Number(body.target_value)}`);
    if (body.current_value !== undefined) updates.push(`current_value = ${Number(body.current_value)}`);
    if (body.status) updates.push(`status = N'${esc(String(body.status))}'`);
    if (body.target_date) updates.push(`target_date = '${esc(String(body.target_date))}'`);
    updates.push("updated_at = SYSUTCDATETIME()");

    const result = await executeSql(`
      UPDATE sozo.goal
      SET ${updates.join(", ")}
      WHERE id = '${esc(String(body.id))}' AND owner_email = N'${esc(ownerEmail)}'
    `);

    if (!result.ok) {
      return NextResponse.json({ error: result.reason }, { status: 500 });
    }

    return NextResponse.json({ ok: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to update goal" },
      { status: 500 },
    );
  }
}

export async function DELETE(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const url = new URL(request.url);
    const id = url.searchParams.get("id");

    if (!id) {
      return NextResponse.json({ error: "id is required" }, { status: 400 });
    }

    await executeSql(`
      DELETE FROM sozo.goal
      WHERE id = '${esc(id)}' AND owner_email = N'${esc(ownerEmail)}'
    `);

    return NextResponse.json({ ok: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to delete goal" },
      { status: 500 },
    );
  }
}
