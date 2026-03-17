/**
 * /api/actions — CRUD for the action queue.
 *
 * GET    — list actions for current user (filterable by status, type)
 * POST   — create a new action
 * PATCH  — update action status/outcome
 * DELETE — delete an action
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
    const status = url.searchParams.get("status");
    const type = url.searchParams.get("type");
    const limit = Math.min(parseInt(url.searchParams.get("limit") ?? "50", 10), 200);

    let where = `WHERE owner_email = N'${esc(ownerEmail)}'`;
    if (status) where += ` AND status = N'${esc(status)}'`;
    if (type) where += ` AND action_type = N'${esc(type)}'`;

    const result = await executeSql(`
      SELECT TOP (${limit})
        id, title, description, action_type, priority_score,
        person_id, person_name, status, source, due_date,
        outcome, outcome_value, outcome_date,
        created_at, updated_at
      FROM sozo.action
      ${where}
      ORDER BY
        CASE status WHEN 'pending' THEN 1 WHEN 'in_progress' THEN 2 ELSE 3 END,
        priority_score DESC,
        created_at DESC
    `, 15000);

    return NextResponse.json({ actions: result.ok ? result.rows : [] });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to load actions" },
      { status: 500 },
    );
  }
}

export async function POST(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = await request.json();

    const id = crypto.randomUUID();
    const title = esc(String(body.title ?? "Untitled action").slice(0, 500));
    const description = body.description ? esc(String(body.description).slice(0, 4000)) : null;
    const actionType = esc(String(body.action_type ?? "general").slice(0, 50));
    const priorityScore = Math.max(0, Math.min(100, Number(body.priority_score ?? 50)));
    const personName = body.person_name ? esc(String(body.person_name).slice(0, 256)) : null;
    const personId = body.person_id ? parseInt(body.person_id, 10) : null;
    const source = esc(String(body.source ?? "manual").slice(0, 50));
    const dueDate = body.due_date ? esc(String(body.due_date)) : null;

    const result = await executeSql(`
      INSERT INTO sozo.action (id, owner_email, title, description, action_type, priority_score, person_id, person_name, source, due_date)
      VALUES (
        '${id}',
        N'${esc(ownerEmail)}',
        N'${title}',
        ${description ? `N'${description}'` : "NULL"},
        N'${actionType}',
        ${priorityScore},
        ${personId && !isNaN(personId) ? personId : "NULL"},
        ${personName ? `N'${personName}'` : "NULL"},
        N'${source}',
        ${dueDate ? `'${dueDate}'` : "NULL"}
      )
    `);

    if (!result.ok) {
      return NextResponse.json({ error: result.reason }, { status: 500 });
    }

    return NextResponse.json({ ok: true, id });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to create action" },
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
    if (body.status) updates.push(`status = N'${esc(String(body.status))}'`);
    if (body.outcome !== undefined) updates.push(`outcome = N'${esc(String(body.outcome).slice(0, 500))}'`);
    if (body.outcome_value !== undefined) updates.push(`outcome_value = ${Number(body.outcome_value)}`);
    if (body.outcome_date) updates.push(`outcome_date = '${esc(String(body.outcome_date))}'`);
    if (body.priority_score !== undefined) updates.push(`priority_score = ${Math.max(0, Math.min(100, Number(body.priority_score)))}`);
    updates.push("updated_at = SYSUTCDATETIME()");

    const result = await executeSql(`
      UPDATE sozo.action
      SET ${updates.join(", ")}
      WHERE id = '${esc(String(body.id))}' AND owner_email = N'${esc(ownerEmail)}'
    `);

    if (!result.ok) {
      return NextResponse.json({ error: result.reason }, { status: 500 });
    }

    return NextResponse.json({ ok: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to update action" },
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

    const result = await executeSql(`
      DELETE FROM sozo.action
      WHERE id = '${esc(id)}' AND owner_email = N'${esc(ownerEmail)}'
    `);

    if (!result.ok) {
      return NextResponse.json({ error: result.reason }, { status: 500 });
    }

    return NextResponse.json({ ok: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to delete action" },
      { status: 500 },
    );
  }
}
