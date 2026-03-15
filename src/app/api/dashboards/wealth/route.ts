import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [capacityTiers, givingGap, upgradeCandidates, screenedVsTotal] =
      await Promise.all([
        // capacity_tiers: count + sum by giving_capacity_label
        executeSql(
          `SELECT ws.giving_capacity_label,
                  COUNT(*) AS donor_count,
                  SUM(ds.total_given) AS total_given
           FROM serving.wealth_screening ws
           JOIN serving.donor_summary ds ON ws.person_id = ds.person_id
           WHERE ds.display_name <> 'Unknown'
             AND ws.giving_capacity_label IS NOT NULL
           GROUP BY ws.giving_capacity_label
           ORDER BY total_given DESC`,
          30000,
        ),

        // giving_gap: avg capacity vs avg actual giving by tier
        executeSql(
          `SELECT ws.giving_capacity_label,
                  AVG(ws.estimated_annual_capacity) AS avg_capacity,
                  AVG(
                    CASE WHEN DATEDIFF(year, ds.first_gift_date, GETDATE()) > 0
                         THEN ds.total_given / DATEDIFF(year, ds.first_gift_date, GETDATE())
                         ELSE ds.total_given
                    END
                  ) AS avg_annualized_giving,
                  COUNT(*) AS donor_count
           FROM serving.wealth_screening ws
           JOIN serving.donor_summary ds ON ws.person_id = ds.person_id
           WHERE ds.display_name <> 'Unknown'
             AND ws.giving_capacity_label IS NOT NULL
           GROUP BY ws.giving_capacity_label
           ORDER BY avg_capacity DESC`,
          30000,
        ),

        // top_upgrade_candidates: giving far below capacity
        executeSql(
          `SELECT TOP 20
                  ws.person_id,
                  ds.display_name,
                  ws.estimated_annual_capacity,
                  ws.giving_capacity_label,
                  ds.total_given,
                  CASE WHEN DATEDIFF(year, ds.first_gift_date, GETDATE()) > 0
                       THEN ds.total_given / DATEDIFF(year, ds.first_gift_date, GETDATE())
                       ELSE ds.total_given
                  END AS annualized_giving,
                  ws.estimated_annual_capacity -
                    CASE WHEN DATEDIFF(year, ds.first_gift_date, GETDATE()) > 0
                         THEN ds.total_given / DATEDIFF(year, ds.first_gift_date, GETDATE())
                         ELSE ds.total_given
                    END AS giving_gap
           FROM serving.wealth_screening ws
           JOIN serving.donor_summary ds ON ws.person_id = ds.person_id
           WHERE ds.display_name <> 'Unknown'
             AND ws.estimated_annual_capacity IS NOT NULL
           ORDER BY giving_gap DESC`,
          30000,
        ),

        // screened_vs_unscreened
        executeSql(
          `SELECT
             (SELECT COUNT(*) FROM serving.wealth_screening ws
              JOIN serving.donor_summary ds ON ws.person_id = ds.person_id
              WHERE ds.display_name <> 'Unknown') AS screened_count,
             (SELECT COUNT(*) FROM serving.donor_summary
              WHERE display_name <> 'Unknown') AS total_donors`,
          30000,
        ),
      ]);

    for (const r of [capacityTiers, givingGap, upgradeCandidates, screenedVsTotal]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    return NextResponse.json({
      capacity_tiers: capacityTiers.rows,
      giving_gap: givingGap.rows,
      top_upgrade_candidates: upgradeCandidates.rows,
      screened_vs_unscreened: screenedVsTotal.rows[0] ?? null,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Wealth query failed" },
      { status: 500 },
    );
  }
}
