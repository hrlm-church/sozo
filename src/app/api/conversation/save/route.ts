import { NextResponse } from "next/server";
import { withTransaction } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import { withAuditLog } from "@/lib/server/audit";
import { z } from "zod";

export const dynamic = "force-dynamic";

const SaveSchema = z.object({
  conversationId: z.string().min(1).max(100),
  title: z.string().max(256).optional(),
  messages: z
    .array(
      z.object({
        id: z.string().min(1),
        role: z.string().min(1).max(50),
        content: z.string(),
      }),
    )
    .min(1)
    .max(500),
});

export const POST = withAuditLog("/api/conversation/save", async function POST(request: Request) {
  try {
    const ownerEmail = await getSessionEmail();
    if (!ownerEmail) {
      return NextResponse.json({ error: "Authentication required" }, { status: 401 });
    }

    const body = await request.json();
    const parsed = SaveSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: "Invalid request", details: parsed.error.issues },
        { status: 400 },
      );
    }

    const { conversationId, title, messages } = parsed.data;
    const safeTitle = title || messages.find((m) => m.role === "user")?.content?.slice(0, 100) || "New Chat";

    await withTransaction(async (exec) => {
      // Upsert conversation
      await exec(
        `IF EXISTS (SELECT 1 FROM sozo.conversation WHERE id = @id)
         BEGIN
           UPDATE sozo.conversation
           SET title = @title, message_count = @msgCount, updated_at = SYSUTCDATETIME()
           WHERE id = @id
         END
         ELSE
         BEGIN
           INSERT INTO sozo.conversation (id, title, owner_email, message_count)
           VALUES (@id, @title, @email, @msgCount)
         END`,
        { id: conversationId, title: safeTitle, email: ownerEmail, msgCount: messages.length },
      );

      // Delete existing messages
      await exec(
        `DELETE FROM sozo.conversation_message WHERE conversation_id = @convId`,
        { convId: conversationId },
      );

      // Insert messages in batches of 5
      for (let i = 0; i < messages.length; i += 5) {
        const batch = messages.slice(i, i + 5);
        for (let j = 0; j < batch.length; j++) {
          const m = batch[j];
          await exec(
            `INSERT INTO sozo.conversation_message (id, conversation_id, role, content_json)
             VALUES (@mid, @convId, @role, @content)`,
            { mid: m.id, convId: conversationId, role: m.role, content: m.content },
          );
        }
      }
    });

    return NextResponse.json({ id: conversationId, saved: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Save failed" },
      { status: 500 },
    );
  }
});
