import { NextResponse } from "next/server";
import { executeSqlSafe } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const ownerEmail = await getSessionEmail();
    if (!ownerEmail) {
      return NextResponse.json({ error: "Authentication required" }, { status: 401 });
    }

    const result = await executeSqlSafe(
      `SELECT TOP (50) id, title, message_count, created_at, updated_at
       FROM sozo.conversation
       WHERE owner_email = @email
       ORDER BY updated_at DESC`,
      { email: ownerEmail },
    );

    if (!result.ok) {
      return NextResponse.json({ error: result.reason }, { status: 500 });
    }

    return NextResponse.json({ conversations: result.rows });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "List failed" },
      { status: 500 },
    );
  }
}
