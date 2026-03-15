import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

const PROGRAMS = ["True Girl", "B2BB", "Dannah Gresh", "Pure Freedom"] as const;

export async function GET() {
  try {
    // Build a single query that computes all program metrics via CASE WHEN
    const programTagQuery = `
      SELECT
        ${PROGRAMS.map(
          (p) =>
            `COUNT(DISTINCT CASE WHEN tag_group LIKE '%${p}%' THEN person_id END) AS [${p}_people_count]`,
        ).join(",\n        ")}
      FROM serving.tag_detail
      WHERE display_name <> 'Unknown'`;

    const programDonationQuery = `
      SELECT
        ${PROGRAMS.map(
          (p) => `
        COUNT(DISTINCT CASE WHEN fund LIKE '%${p}%' THEN person_id END) AS [${p}_donor_count],
        SUM(CASE WHEN fund LIKE '%${p}%' THEN amount ELSE 0 END) AS [${p}_total_revenue]`,
        ).join(",")}
      FROM serving.donation_detail
      WHERE display_name <> 'Unknown'`;

    const [tagResult, donationResult] = await Promise.all([
      executeSql(programTagQuery, 30000),
      executeSql(programDonationQuery, 30000),
    ]);

    for (const r of [tagResult, donationResult]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    const tagRow = tagResult.rows[0] ?? {};
    const donRow = donationResult.rows[0] ?? {};

    const programs = PROGRAMS.map((name) => ({
      program_name: name,
      people_count: tagRow[`${name}_people_count`] ?? 0,
      donor_count: donRow[`${name}_donor_count`] ?? 0,
      total_revenue: donRow[`${name}_total_revenue`] ?? 0,
    }));

    return NextResponse.json({ programs });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Programs query failed" },
      { status: 500 },
    );
  }
}
