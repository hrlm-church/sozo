/**
 * Person Search API — searches donor_summary by display_name.
 *
 * GET /api/people/search?q=<query>
 * Returns top 20 matches with person_id, display_name, email, lifecycle_stage, total_given.
 */

import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const q = url.searchParams.get("q")?.trim();

    if (!q || q.length < 2) {
      return NextResponse.json({ results: [] });
    }

    // Sanitize for LIKE query — escape special chars and single quotes
    const safeTerm = q.replace(/'/g, "''").replace(/[%_[\]]/g, "[$&]");

    const result = await executeSql(`
      SELECT TOP (20)
        ds.person_id,
        ds.display_name,
        ds.email,
        ds.lifecycle_stage,
        ds.total_given,
        ds.last_gift_date,
        ds.donation_count
      FROM serving.donor_summary ds
      WHERE ds.display_name <> 'Unknown'
        AND (ds.display_name LIKE '%${safeTerm}%' OR ds.email LIKE '%${safeTerm}%')
      ORDER BY ds.total_given DESC
    `, 15000);

    return NextResponse.json({ results: result.ok ? result.rows : [] });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Search failed" },
      { status: 500 },
    );
  }
}
