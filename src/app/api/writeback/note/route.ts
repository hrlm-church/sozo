/**
 * POST /api/writeback/note
 *
 * Create a note on a Keap contact.
 * Logs all operations to sozo.audit_log.
 */

import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import { createNote, isKeapConfigured } from "@/lib/server/keap-client";

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
    const { contact_id, title, note_body, person_name } = body;

    if (!contact_id || !title || !note_body) {
      return NextResponse.json(
        { error: "contact_id, title, and note_body are required" },
        { status: 400 },
      );
    }

    const auditId = crypto.randomUUID();
    const payloadJson = JSON.stringify({ contact_id, title, note_body: note_body.slice(0, 200) }).replace(/'/g, "''");

    // Log the attempt
    await executeSql(`
      INSERT INTO sozo.audit_log (id, owner_email, action, target_system, target_id, person_name, payload_json, status)
      VALUES (
        '${auditId}',
        N'${esc(ownerEmail)}',
        'note_created',
        'keap',
        '${parseInt(contact_id, 10)}',
        ${person_name ? `N'${esc(String(person_name))}'` : "NULL"},
        N'${payloadJson}',
        'pending'
      )
    `);

    // Execute the write-back
    const result = await createNote(
      Number(contact_id),
      String(title).slice(0, 256),
      String(note_body).slice(0, 4000),
    );

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

    return NextResponse.json({ ok: true, note_id: result.noteId, audit_id: auditId });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Note write-back failed" },
      { status: 500 },
    );
  }
}
