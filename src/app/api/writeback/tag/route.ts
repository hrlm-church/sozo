/**
 * POST /api/writeback/tag
 *
 * Apply or remove a tag on a Keap contact.
 * Logs all operations to sozo.audit_log.
 */

import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import { applyTag, removeTag, isKeapConfigured } from "@/lib/server/keap-client";

export const dynamic = "force-dynamic";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

export async function POST(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";

    if (!isKeapConfigured()) {
      return NextResponse.json({ error: "Keap integration not configured" }, { status: 503 });
    }

    const body = await request.json();
    const { contact_id, tag_id, action, person_name } = body;

    if (!contact_id || !tag_id || !action) {
      return NextResponse.json(
        { error: "contact_id, tag_id, and action (apply|remove) are required" },
        { status: 400 },
      );
    }

    const auditId = crypto.randomUUID();
    const payloadJson = JSON.stringify({ contact_id, tag_id, action }).replace(/'/g, "''");

    // Log the attempt
    await executeSql(`
      INSERT INTO sozo.audit_log (id, owner_email, action, target_system, target_id, person_name, payload_json, status)
      VALUES (
        '${auditId}',
        N'${esc(ownerEmail)}',
        N'tag_${esc(String(action))}',
        'keap',
        '${contact_id}',
        ${person_name ? `N'${esc(String(person_name))}'` : "NULL"},
        N'${payloadJson}',
        'pending'
      )
    `);

    // Execute the write-back
    const result = action === "remove"
      ? await removeTag(Number(contact_id), Number(tag_id))
      : await applyTag(Number(contact_id), Number(tag_id));

    // Update audit log with result
    await executeSql(`
      UPDATE sozo.audit_log
      SET status = '${result.ok ? "success" : "failed"}',
          ${result.error ? `error_message = N'${esc(result.error)}',` : ""}
          response_json = N'${JSON.stringify(result).replace(/'/g, "''")}'
      WHERE id = '${auditId}'
    `);

    if (!result.ok) {
      return NextResponse.json({ error: result.error }, { status: 502 });
    }

    return NextResponse.json({ ok: true, audit_id: auditId });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Tag write-back failed" },
      { status: 500 },
    );
  }
}
