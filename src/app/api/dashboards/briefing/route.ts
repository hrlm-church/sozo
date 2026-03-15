import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [kpis, topAlerts, recentMetrics, atRiskPreview] =
      await Promise.all([
        // summary_kpis
        executeSql(
          `SELECT
             (SELECT COUNT(*) FROM serving.donor_summary WHERE display_name <> 'Unknown') AS total_donors,
             (SELECT SUM(total_given) FROM serving.donor_summary WHERE display_name <> 'Unknown') AS total_given,
             (SELECT COUNT(*) FROM serving.subscription_detail
              WHERE source_system = 'subbly' AND subscription_status = 'active'
                AND display_name <> 'Unknown') AS active_subscribers,
             (SELECT COUNT(*) FROM intel.person_score
              WHERE score_type = 'churn_risk' AND score_label IN ('critical', 'high')) AS at_risk_count`,
          30000,
        ),

        // top_alerts: top 5 critical/high insights
        executeSql(
          `SELECT TOP 5
                  insight_id,
                  insight_type,
                  severity,
                  title,
                  summary,
                  metric_key,
                  current_value,
                  baseline_value,
                  delta_pct,
                  created_at
           FROM intel.insight
           WHERE is_active = 1
             AND severity IN ('critical', 'high')
           ORDER BY
             CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 ELSE 2 END,
             created_at DESC`,
          30000,
        ),

        // recent_metrics: latest snapshots
        executeSql(
          `SELECT ms.metric_key,
                  md.display_name,
                  ms.value_decimal,
                  ms.prior_value_decimal,
                  ms.delta_pct,
                  ms.as_of_date,
                  md.unit,
                  md.format_hint
           FROM intel.metric_snapshot ms
           JOIN intel.metric_definition md ON ms.metric_key = md.metric_key
           WHERE md.is_active = 1
           ORDER BY ms.as_of_date DESC`,
          30000,
        ),

        // at_risk_preview: top 5
        executeSql(
          `SELECT TOP 5
                  ps.person_id,
                  ds.display_name,
                  ps.score_value,
                  ps.score_label,
                  ds.total_given,
                  ds.last_gift_date,
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
      ]);

    for (const r of [kpis, topAlerts, recentMetrics, atRiskPreview]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    return NextResponse.json({
      summary_kpis: kpis.rows[0] ?? null,
      top_alerts: topAlerts.rows,
      recent_metrics: recentMetrics.rows,
      at_risk_preview: atRiskPreview.rows,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Briefing query failed" },
      { status: 500 },
    );
  }
}
