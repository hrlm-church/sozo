import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [funnel, retention, atRisk, lost, giving, avgGift] =
      await Promise.all([
        // lifecycle_funnel
        executeSql(
          `SELECT lifecycle_stage,
                  COUNT(*) AS donor_count,
                  SUM(total_given) AS total_given
           FROM serving.donor_summary
           WHERE display_name <> 'Unknown'
           GROUP BY lifecycle_stage
           ORDER BY donor_count DESC`,
          30000,
        ),

        // retention_trend: donors who gave in month N who also gave in N-1
        executeSql(
          `SELECT cur.donation_month,
                  COUNT(DISTINCT cur.person_id) AS total_donors,
                  COUNT(DISTINCT prev.person_id) AS retained_donors
           FROM serving.donor_monthly cur
           LEFT JOIN serving.donor_monthly prev
             ON cur.person_id = prev.person_id
            AND prev.donation_month = FORMAT(
                  DATEADD(month, -1, CAST(cur.donation_month + '-01' AS DATE)),
                  'yyyy-MM')
           WHERE cur.display_name <> 'Unknown'
             AND cur.donation_month >= FORMAT(DATEADD(month, -12, GETDATE()), 'yyyy-MM')
           GROUP BY cur.donation_month
           ORDER BY cur.donation_month`,
          30000,
        ),

        // at_risk_donors: top 25 critical/high by revenue at risk
        executeSql(
          `SELECT TOP 25
                  ps.person_id,
                  ds.display_name,
                  ps.score_value,
                  ps.score_label,
                  ds.total_given,
                  ds.last_gift_date,
                  ds.days_since_last,
                  ds.lifecycle_stage,
                  dh.annual_revenue_at_risk
           FROM intel.person_score ps
           JOIN serving.donor_summary ds ON ps.person_id = ds.person_id
           LEFT JOIN intel.vw_donor_health dh ON ps.person_id = dh.person_id
           WHERE ps.score_type = 'churn_risk'
             AND ps.score_label IN ('critical', 'high')
             AND ds.display_name <> 'Unknown'
           ORDER BY dh.annual_revenue_at_risk DESC`,
          30000,
        ),

        // lost_recurring summary
        executeSql(
          `SELECT COUNT(*) AS lost_count,
                  SUM(monthly_amount) AS total_monthly_value_lost
           FROM serving.lost_recurring_donors
           WHERE display_name <> 'Unknown'`,
          30000,
        ),

        // giving_trend: last 24 months
        executeSql(
          `SELECT donation_month,
                  SUM(amount) AS total_amount,
                  COUNT(DISTINCT person_id) AS donor_count
           FROM serving.donor_monthly
           WHERE display_name <> 'Unknown'
             AND donation_month >= FORMAT(DATEADD(month, -24, GETDATE()), 'yyyy-MM')
           GROUP BY donation_month
           ORDER BY donation_month`,
          30000,
        ),

        // avg_gift_trend: last 24 months
        executeSql(
          `SELECT donation_month,
                  AVG(amount) AS avg_gift,
                  COUNT(*) AS gift_count
           FROM serving.donor_monthly
           WHERE display_name <> 'Unknown'
             AND donation_month >= FORMAT(DATEADD(month, -24, GETDATE()), 'yyyy-MM')
           GROUP BY donation_month
           ORDER BY donation_month`,
          30000,
        ),
      ]);

    // Check for query failures
    for (const r of [funnel, retention, atRisk, lost, giving, avgGift]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    return NextResponse.json({
      lifecycle_funnel: funnel.rows,
      retention_trend: retention.rows,
      at_risk_donors: atRisk.rows,
      lost_recurring: lost.rows[0] ?? { lost_count: 0, total_monthly_value_lost: 0 },
      giving_trend: giving.rows,
      avg_gift_trend: avgGift.rows,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Donor health query failed" },
      { status: 500 },
    );
  }
}
