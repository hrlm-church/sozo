import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

interface FeedbackRequest {
  conversationId?: string;
  messageId: string;
  rating: number; // 1 = thumbs up, -1 = thumbs down
}

export async function POST(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const body = (await request.json()) as FeedbackRequest;
    const { conversationId, messageId, rating } = body;

    if (!messageId || (rating !== 1 && rating !== -1)) {
      return NextResponse.json({ error: "messageId and rating (1 or -1) required" }, { status: 400 });
    }

    const id = crypto.randomUUID();
    await executeSql(`
      INSERT INTO sozo.feedback (id, conversation_id, message_id, rating, owner_email)
      VALUES (
        '${esc(id)}',
        ${conversationId ? `'${esc(conversationId)}'` : "NULL"},
        '${esc(messageId)}',
        ${rating},
        N'${esc(ownerEmail)}'
      )
    `);

    return NextResponse.json({ id, saved: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Feedback save failed" },
      { status: 500 },
    );
  }
}

export async function GET() {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const result = await executeSql(`
      SELECT TOP (100) id, conversation_id, message_id, rating, created_at
      FROM sozo.feedback
      WHERE owner_email = N'${esc(ownerEmail)}'
      ORDER BY created_at DESC
    `);
    return NextResponse.json({ feedback: result.ok ? result.rows : [] });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Feedback load failed" },
      { status: 500 },
    );
  }
}
