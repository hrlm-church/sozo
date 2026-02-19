import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

export async function GET() {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";

    const result = await executeSql(`
      SELECT TOP (50) id, title, message_count, created_at, updated_at
      FROM sozo.conversation
      WHERE owner_email = N'${esc(ownerEmail)}'
      ORDER BY updated_at DESC
    `);

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
