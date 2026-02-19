import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

function esc(val: string): string {
  return val.replace(/'/g, "''");
}

export async function DELETE(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const { searchParams } = new URL(request.url);
    const id = searchParams.get("id");

    if (!id) {
      return NextResponse.json({ error: "id parameter required" }, { status: 400 });
    }

    // Delete (CASCADE will remove messages too)
    await executeSql(`
      DELETE FROM sozo.conversation
      WHERE id = '${esc(id)}' AND owner_email = N'${esc(ownerEmail)}'
    `);

    return NextResponse.json({ deleted: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Delete failed" },
      { status: 500 },
    );
  }
}
