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
    const { searchParams } = new URL(request.url);
    const id = searchParams.get("id");

    if (!id) {
      return NextResponse.json({ error: "id parameter required" }, { status: 400 });
    }

    // Verify ownership
    const convResult = await executeSql(`
      SELECT id, title, owner_email, message_count, created_at, updated_at
      FROM sozo.conversation
      WHERE id = '${esc(id)}' AND owner_email = N'${esc(ownerEmail)}'
    `);

    if (!convResult.ok || !convResult.rows.length) {
      return NextResponse.json({ error: "Conversation not found" }, { status: 404 });
    }

    // Load messages
    const msgResult = await executeSql(`
      SELECT id, role, content_json, created_at
      FROM sozo.conversation_message
      WHERE conversation_id = '${esc(id)}'
      ORDER BY created_at ASC
    `);

    return NextResponse.json({
      conversation: convResult.rows[0],
      messages: (msgResult.ok ? msgResult.rows : []).map((m) => ({
        id: m.id,
        role: m.role,
        content: m.content_json,
        createdAt: m.created_at,
      })),
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Load failed" },
      { status: 500 },
    );
  }
}
