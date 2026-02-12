import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";

export async function DELETE(request: Request) {
  try {
    const ownerEmail = (await getSessionEmail()) ?? "anonymous@sozo.local";
    const { searchParams } = new URL(request.url);
    const id = searchParams.get("id");

    if (!id) {
      return NextResponse.json({ error: "Dashboard id is required" }, { status: 400 });
    }

    // CASCADE delete handles widgets automatically via FK (scoped to owner)
    const result = await executeSql(
      `DELETE FROM dashboard.saved_dashboard WHERE id = '${id.replace(/'/g, "''")}' AND owner_email = N'${ownerEmail.replace(/'/g, "''")}'`,
    );

    if (!result.ok) {
      return NextResponse.json({ error: result.reason }, { status: 500 });
    }

    return NextResponse.json({ deleted: true });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Delete failed" },
      { status: 500 },
    );
  }
}
