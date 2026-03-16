import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

const PROGRAMS = ["True Girl", "B2BB", "Dannah Gresh", "Pure Freedom"] as const;

export async function GET() {
  try {
    // Program metrics from donations (by fund name) and events
    const [donationResult, eventResult] = await Promise.all([
      executeSql(
        `SELECT
          ${PROGRAMS.map(
            (p) => `
          COUNT(DISTINCT CASE WHEN fund LIKE '%${p}%' THEN person_id END) AS [${p}_donor_count],
          SUM(CASE WHEN fund LIKE '%${p}%' THEN amount ELSE 0 END) AS [${p}_total_revenue]`,
          ).join(",")}
        FROM serving.donation_detail
        WHERE display_name <> 'Unknown'`,
        30000,
      ),
      executeSql(
        `SELECT
          ${PROGRAMS.map(
            (p) => `
          COUNT(DISTINCT CASE WHEN event_name LIKE '%${p}%' THEN person_id END) AS [${p}_attendee_count]`,
          ).join(",")}
        FROM serving.event_detail
        WHERE display_name <> 'Unknown'`,
        30000,
      ),
    ]);

    for (const r of [donationResult, eventResult]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    const donRow = donationResult.rows[0] ?? {};
    const evtRow = eventResult.rows[0] ?? {};

    const programs = PROGRAMS.map((name) => ({
      program_name: name,
      donor_count: donRow[`${name}_donor_count`] ?? 0,
      total_revenue: donRow[`${name}_total_revenue`] ?? 0,
      attendee_count: evtRow[`${name}_attendee_count`] ?? 0,
    }));

    return NextResponse.json({ programs });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Programs query failed" },
      { status: 500 },
    );
  }
}
