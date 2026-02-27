import { NextResponse } from "next/server";
import { executeSqlSafe } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import { withAuditLog } from "@/lib/server/audit";
import { z } from "zod";

export const dynamic = "force-dynamic";

const FeedbackSchema = z.object({
  conversationId: z.string().max(100).optional(),
  messageId: z.string().min(1).max(100),
  rating: z.union([z.literal(1), z.literal(-1)]),
});

export const POST = withAuditLog("/api/feedback", async function POST(request: Request) {
  try {
    const ownerEmail = await getSessionEmail();
    if (!ownerEmail) {
      return NextResponse.json({ error: "Authentication required" }, { status: 401 });
    }

    const body = await request.json();
    const parsed = FeedbackSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: "messageId and rating (1 or -1) required" },
        { status: 400 },
      );
    }

    const { conversationId, messageId, rating } = parsed.data;
    const id = crypto.randomUUID();

    await executeSqlSafe(
      `INSERT INTO sozo.feedback (id, conversation_id, message_id, rating, owner_email)
       VALUES (@id, @convId, @msgId, @rating, @email)`,
      {
        id,
        convId: conversationId ?? null,
        msgId: messageId,
        rating,
        email: ownerEmail,
      },
    );

    // Adjust knowledge confidence based on feedback
    if (conversationId) {
      if (rating === -1) {
        await executeSqlSafe(
          `UPDATE sozo.knowledge
           SET confidence = CASE WHEN confidence - 0.15 < 0.10 THEN 0.10 ELSE confidence - 0.15 END,
               updated_at = SYSUTCDATETIME()
           WHERE source_conv_id = @convId AND is_active = 1`,
          { convId: conversationId },
        ).catch(() => { /* non-critical */ });
      } else if (rating === 1) {
        await executeSqlSafe(
          `UPDATE sozo.knowledge
           SET confidence = CASE WHEN confidence + 0.05 > 1.00 THEN 1.00 ELSE confidence + 0.05 END,
               updated_at = SYSUTCDATETIME()
           WHERE source_conv_id = @convId AND is_active = 1`,
          { convId: conversationId },
        ).catch(() => { /* non-critical */ });
      }
    }

    return NextResponse.json({ id, saved: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Feedback save failed" },
      { status: 500 },
    );
  }
});

export const GET = withAuditLog("/api/feedback", async function GET() {
  try {
    const ownerEmail = await getSessionEmail();
    if (!ownerEmail) {
      return NextResponse.json({ error: "Authentication required" }, { status: 401 });
    }

    const result = await executeSqlSafe(
      `SELECT TOP (100) id, conversation_id, message_id, rating, created_at
       FROM sozo.feedback
       WHERE owner_email = @email
       ORDER BY created_at DESC`,
      { email: ownerEmail },
    );
    return NextResponse.json({ feedback: result.ok ? result.rows : [] });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Feedback load failed" },
      { status: 500 },
    );
  }
});
