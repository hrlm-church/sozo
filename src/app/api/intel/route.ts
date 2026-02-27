/**
 * Intel Feed API — returns intelligence insights, metric snapshots, and risk scores.
 *
 * GET /api/intel — returns the full intelligence briefing
 * GET /api/intel?section=insights — returns only insights
 * GET /api/intel?section=metrics — returns only latest metric snapshots
 * GET /api/intel?section=risks — returns only high-risk donors
 */

import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET(request: Request) {
  try {
    const url = new URL(request.url);
    const section = url.searchParams.get("section"); // insights | metrics | risks | null (all)

    const result: Record<string, unknown> = {};

    // ── Insights (anomalies, risks, opportunities) ──────────────────────
    if (!section || section === "insights") {
      const insightsResult = await executeSql(`
        SELECT TOP (20)
          insight_id, insight_type, severity, title, summary,
          metric_key, as_of_date, current_value, baseline_value, delta_pct,
          status, created_at
        FROM intel.insight
        WHERE is_active = 1 AND status = 'open'
        ORDER BY severity DESC, created_at DESC
      `, 30000);

      result.insights = insightsResult.ok ? insightsResult.rows : [];
    }

    // ── Latest metric snapshots ─────────────────────────────────────────
    if (!section || section === "metrics") {
      const metricsResult = await executeSql(`
        SELECT
          ms.metric_key,
          md.display_name,
          md.unit,
          md.format_hint,
          ms.value_decimal AS value,
          ms.prior_value_decimal AS prior_value,
          ms.delta_pct,
          ms.as_of_date,
          ms.start_date,
          ms.end_date
        FROM intel.metric_snapshot ms
        JOIN intel.metric_definition md ON md.metric_key = ms.metric_key
        WHERE ms.as_of_date = (SELECT MAX(as_of_date) FROM intel.metric_snapshot)
          AND ms.segment_key IS NULL
          AND md.is_active = 1
        ORDER BY md.metric_key
      `, 30000);

      result.metrics = metricsResult.ok ? metricsResult.rows : [];
    }

    // ── High-risk donors ────────────────────────────────────────────────
    if (!section || section === "risks") {
      const risksResult = await executeSql(`
        SELECT TOP (25)
          ps.person_id,
          ds.display_name,
          ds.email,
          ds.total_given,
          ds.last_gift_date,
          ds.days_since_last,
          ds.lifecycle_stage,
          ps.score_value AS risk_score,
          ps.score_label AS risk_level,
          ps.drivers_json,
          dh.annual_revenue_at_risk
        FROM intel.person_score ps
        JOIN serving.donor_summary ds ON ds.person_id = ps.person_id
        LEFT JOIN intel.vw_donor_health dh ON dh.person_id = ps.person_id
        WHERE ps.score_type = 'churn_risk'
          AND ps.as_of_date = (SELECT MAX(as_of_date) FROM intel.person_score WHERE score_type = 'churn_risk')
          AND ps.score_label IN ('critical', 'high')
          AND ds.display_name <> 'Unknown'
          AND ds.total_given > 100
        ORDER BY dh.annual_revenue_at_risk DESC
      `, 30000);

      result.risks = risksResult.ok ? risksResult.rows : [];
    }

    // ── Summary stats ───────────────────────────────────────────────────
    if (!section) {
      const summaryResult = await executeSql(`
        SELECT
          (SELECT COUNT(*) FROM intel.insight WHERE is_active = 1 AND status = 'open') AS open_insights,
          (SELECT COUNT(*) FROM intel.person_score
           WHERE score_type = 'churn_risk'
             AND as_of_date = (SELECT MAX(as_of_date) FROM intel.person_score WHERE score_type = 'churn_risk')
             AND score_label IN ('critical', 'high')) AS high_risk_donors,
          (SELECT COUNT(*) FROM intel.metric_snapshot
           WHERE as_of_date = (SELECT MAX(as_of_date) FROM intel.metric_snapshot)
             AND segment_key IS NULL) AS metrics_computed,
          (SELECT MAX(as_of_date) FROM intel.metric_snapshot) AS last_snapshot_date
      `, 30000);

      result.summary = summaryResult.ok && summaryResult.rows.length > 0 ? summaryResult.rows[0] : {};
    }

    return NextResponse.json(result);
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Intel feed error" },
      { status: 500 },
    );
  }
}
