import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

interface SaveRequest {
  conversationId: string;
  title?: string;
  messages: Array<{
    id: string;
    role: string;
    content: string; // JSON-serialized UIMessage parts
  }>;
}

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

export async function POST(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = (await request.json()) as SaveRequest;
    const { conversationId, title, messages } = body;

    if (!conversationId || !messages?.length) {
      return NextResponse.json({ error: "conversationId and messages required" }, { status: 400 });
    }

    const safeTitle = esc(title || messages.find(m => m.role === "user")?.content?.slice(0, 100) || "New Chat");

    // Upsert conversation
    await executeSql(`
      IF EXISTS (SELECT 1 FROM sozo.conversation WHERE id = '${esc(conversationId)}')
      BEGIN
        UPDATE sozo.conversation
        SET title = N'${safeTitle}', message_count = ${messages.length}, updated_at = SYSUTCDATETIME()
        WHERE id = '${esc(conversationId)}'
      END
      ELSE
      BEGIN
        INSERT INTO sozo.conversation (id, title, owner_email, message_count)
        VALUES ('${esc(conversationId)}', N'${safeTitle}', N'${esc(ownerEmail)}', ${messages.length})
      END
    `);

    // Delete existing messages and re-insert (simpler than diff)
    await executeSql(
      `DELETE FROM sozo.conversation_message WHERE conversation_id = '${esc(conversationId)}'`,
    );

    // Insert messages in batches of 5 (avoid huge queries on low DTU)
    for (let i = 0; i < messages.length; i += 5) {
      const batch = messages.slice(i, i + 5);
      const values = batch
        .map(
          (m) =>
            `('${esc(m.id)}', '${esc(conversationId)}', '${esc(m.role)}', N'${esc(m.content)}')`,
        )
        .join(",\n");
      await executeSql(
        `INSERT INTO sozo.conversation_message (id, conversation_id, role, content_json) VALUES ${values}`,
      );
    }

    return NextResponse.json({ id: conversationId, saved: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Save failed" },
      { status: 500 },
    );
  }
}
