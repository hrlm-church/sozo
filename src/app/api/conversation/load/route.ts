import { NextResponse } from "next/server";
import { executeSqlSafe } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import { withAuditLog } from "@/lib/server/audit";

export const dynamic = "force-dynamic";

export const GET = withAuditLog("/api/conversation/load", async function GET(request: Request) {
  try {
    const ownerEmail = await getSessionEmail();
    if (!ownerEmail) {
      return NextResponse.json({ error: "Authentication required" }, { status: 401 });
    }

    const { searchParams } = new URL(request.url);
    const id = searchParams.get("id");

    if (!id || id.length > 100) {
      return NextResponse.json({ error: "Valid id parameter required" }, { status: 400 });
    }

    // Verify ownership
    const convResult = await executeSqlSafe(
      `SELECT id, title, owner_email, message_count, created_at, updated_at
       FROM sozo.conversation
       WHERE id = @id AND owner_email = @email`,
      { id, email: ownerEmail },
    );

    if (!convResult.ok || !convResult.rows.length) {
      return NextResponse.json({ error: "Conversation not found" }, { status: 404 });
    }

    // Load messages
    const msgResult = await executeSqlSafe(
      `SELECT id, role, content_json, created_at
       FROM sozo.conversation_message
       WHERE conversation_id = @id
       ORDER BY created_at ASC`,
      { id },
    );

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
});
