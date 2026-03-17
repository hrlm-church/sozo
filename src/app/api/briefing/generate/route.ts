/**
 * POST /api/briefing/generate
 *
 * Runs analytical queries against the database, passes results to AI,
 * and saves a structured daily briefing. Called by Azure Logic App timer
 * or manually.
 *
 * Requires API key auth via x-api-key header for cron/external calls.
 */

import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export const dynamic = "force-dynamic";
export const maxDuration = 120;

const BRIEFING_QUERIES = [
  {
    name: "lifecycle_changes",
    label: "Lifecycle Stage Changes (7 days)",
    sql: `SELECT lifecycle_stage, COUNT(*) AS cnt
          FROM serving.donor_summary
          WHERE display_name <> 'Unknown'
          GROUP BY lifecycle_stage
          ORDER BY cnt DESC`,
  },
  {
    name: "giving_this_week",
    label: "Giving This Week vs Last Year",
    sql: `SELECT
            SUM(CASE WHEN donated_at >= DATEADD(DAY, -7, GETDATE()) THEN amount ELSE 0 END) AS this_week,
            SUM(CASE WHEN donated_at >= DATEADD(DAY, -372, GETDATE()) AND donated_at < DATEADD(DAY, -365, GETDATE()) THEN amount ELSE 0 END) AS same_week_last_year,
            COUNT(DISTINCT CASE WHEN donated_at >= DATEADD(DAY, -7, GETDATE()) THEN person_id END) AS donors_this_week
          FROM serving.donation_detail
          WHERE display_name <> 'Unknown'`,
  },
  {
    name: "new_donors",
    label: "New Donors (7 days)",
    sql: `SELECT TOP (10) display_name, total_given, first_gift_date
          FROM serving.donor_summary
          WHERE first_gift_date >= DATEADD(DAY, -7, GETDATE())
            AND display_name <> 'Unknown'
          ORDER BY total_given DESC`,
  },
  {
    name: "largest_gifts",
    label: "Largest Gifts (7 days)",
    sql: `SELECT TOP (10) display_name, amount, fund, donated_at
          FROM serving.donation_detail
          WHERE donated_at >= DATEADD(DAY, -7, GETDATE())
            AND display_name <> 'Unknown'
          ORDER BY amount DESC`,
  },
  {
    name: "expiring_subscriptions",
    label: "Expiring Subscriptions (14 days)",
    sql: `SELECT TOP (10) customer_name, product_name, renewal_date, status
          FROM silver.subbly_subscription
          WHERE renewal_date BETWEEN GETDATE() AND DATEADD(DAY, 14, GETDATE())
            AND status = 'Active'
          ORDER BY renewal_date`,
  },
  {
    name: "capacity_gap",
    label: "Top 10 Under-Givers vs Capacity",
    sql: `SELECT TOP (10) ds.display_name,
            ds.total_given,
            ws.giving_capacity,
            ws.capacity_label,
            ROUND(ws.giving_capacity - ds.total_given, 0) AS gap
          FROM serving.donor_summary ds
          JOIN serving.wealth_screening ws ON ds.person_id = ws.person_id
          WHERE ds.display_name <> 'Unknown'
            AND ws.giving_capacity > ds.total_given * 2
          ORDER BY gap DESC`,
  },
  {
    name: "lost_recurring",
    label: "Recoverable Lost Recurring Donors",
    sql: `SELECT TOP (10) display_name, monthly_amount, annual_value, category
          FROM serving.lost_recurring_donors
          ORDER BY annual_value DESC`,
  },
  {
    name: "churn_risk",
    label: "Churn Risk Summary",
    sql: `SELECT
            SUM(CASE WHEN ps.score_value >= 0.7 THEN 1 ELSE 0 END) AS critical_count,
            SUM(CASE WHEN ps.score_value >= 0.4 AND ps.score_value < 0.7 THEN 1 ELSE 0 END) AS high_count
          FROM intel.person_score ps
          WHERE ps.score_type = 'churn_risk'`,
  },
];

function validateApiKey(request: Request): boolean {
  const apiKey = request.headers.get("x-api-key");
  const expected = process.env.BRIEFING_API_KEY;
  if (!expected) return true; // No key configured = allow (dev mode)
  return apiKey === expected;
}

export async function POST(request: Request) {
  try {
    // Auth: either session or API key
    const ownerEmail = (await getSessionEmail()) ?? null;
    if (!ownerEmail && !validateApiKey(request)) {
      return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
    }

    const email = ownerEmail ?? "system@sozo.local";

    // Run all queries in parallel
    const queryResults: Record<string, unknown> = {};
    const results = await Promise.all(
      BRIEFING_QUERIES.map(async (q) => {
        const result = await executeSql(q.sql, 30000);
        return { name: q.name, label: q.label, ok: result.ok, rows: result.ok ? result.rows : [], error: result.reason };
      }),
    );

    for (const r of results) {
      queryResults[r.name] = { label: r.label, data: r.rows, error: r.error };
    }

    // Build briefing content from query results
    const sections: { title: string; content: string; data?: unknown }[] = [];
    const metrics: Record<string, unknown> = {};

    // Lifecycle summary
    const lifecycle = results.find((r) => r.name === "lifecycle_changes");
    if (lifecycle?.ok && lifecycle.rows.length > 0) {
      const stageMap = Object.fromEntries(lifecycle.rows.map((r: Record<string, unknown>) => [r.lifecycle_stage, r.cnt]));
      metrics.active_donors = stageMap["active"] ?? 0;
      metrics.cooling_donors = stageMap["cooling"] ?? 0;
      metrics.lapsed_donors = stageMap["lapsed"] ?? 0;
      metrics.lost_donors = stageMap["lost"] ?? 0;
      sections.push({
        title: "Donor Lifecycle",
        content: `Active: ${metrics.active_donors}, Cooling: ${metrics.cooling_donors}, Lapsed: ${metrics.lapsed_donors}, Lost: ${metrics.lost_donors}`,
        data: lifecycle.rows,
      });
    }

    // Giving this week
    const giving = results.find((r) => r.name === "giving_this_week");
    if (giving?.ok && giving.rows.length > 0) {
      const g = giving.rows[0] as Record<string, unknown>;
      metrics.giving_this_week = g.this_week;
      metrics.giving_same_week_ly = g.same_week_last_year;
      metrics.donors_this_week = g.donors_this_week;
      sections.push({
        title: "This Week's Giving",
        content: `$${Number(g.this_week ?? 0).toLocaleString()} from ${g.donors_this_week} donors (same week last year: $${Number(g.same_week_last_year ?? 0).toLocaleString()})`,
      });
    }

    // New donors
    const newDonors = results.find((r) => r.name === "new_donors");
    if (newDonors?.ok && newDonors.rows.length > 0) {
      metrics.new_donor_count = newDonors.rows.length;
      sections.push({
        title: "New Donors",
        content: newDonors.rows.map((r: Record<string, unknown>) => `${r.display_name}: $${Number(r.total_given ?? 0).toLocaleString()}`).join(", "),
        data: newDonors.rows,
      });
    }

    // Largest gifts
    const largestGifts = results.find((r) => r.name === "largest_gifts");
    if (largestGifts?.ok && largestGifts.rows.length > 0) {
      sections.push({
        title: "Largest Gifts This Week",
        content: largestGifts.rows.slice(0, 5).map((r: Record<string, unknown>) => `${r.display_name}: $${Number(r.amount ?? 0).toLocaleString()} (${r.fund})`).join(", "),
        data: largestGifts.rows,
      });
    }

    // Churn risk
    const churn = results.find((r) => r.name === "churn_risk");
    if (churn?.ok && churn.rows.length > 0) {
      const c = churn.rows[0] as Record<string, unknown>;
      metrics.churn_critical = c.critical_count;
      metrics.churn_high = c.high_count;
      sections.push({
        title: "Churn Risk",
        content: `${c.critical_count} critical, ${c.high_count} high risk donors`,
      });
    }

    // Lost recurring
    const lostRecurring = results.find((r) => r.name === "lost_recurring");
    if (lostRecurring?.ok && lostRecurring.rows.length > 0) {
      sections.push({
        title: "Top Recovery Opportunities",
        content: lostRecurring.rows.slice(0, 5).map((r: Record<string, unknown>) => `${r.display_name}: $${Number(r.annual_value ?? 0).toLocaleString()}/yr`).join(", "),
        data: lostRecurring.rows,
      });
    }

    // Capacity gap
    const capGap = results.find((r) => r.name === "capacity_gap");
    if (capGap?.ok && capGap.rows.length > 0) {
      sections.push({
        title: "Upgrade Opportunities",
        content: capGap.rows.slice(0, 5).map((r: Record<string, unknown>) => `${r.display_name}: giving $${Number(r.total_given ?? 0).toLocaleString()} vs $${Number(r.giving_capacity ?? 0).toLocaleString()} capacity`).join(", "),
        data: capGap.rows,
      });
    }

    // Generate suggested actions
    const suggestedActions: { title: string; type: string; priority: number; person_name?: string }[] = [];

    // Thank new donors
    if (newDonors?.ok) {
      for (const r of newDonors.rows.slice(0, 3)) {
        const row = r as Record<string, unknown>;
        suggestedActions.push({
          title: `Thank new donor ${row.display_name}`,
          type: "thank",
          priority: 80,
          person_name: String(row.display_name),
        });
      }
    }

    // Re-engage lost recurring
    if (lostRecurring?.ok) {
      for (const r of lostRecurring.rows.slice(0, 2)) {
        const row = r as Record<string, unknown>;
        suggestedActions.push({
          title: `Re-engage ${row.display_name} — $${Number(row.annual_value ?? 0).toLocaleString()}/yr lost`,
          type: "reengage",
          priority: 90,
          person_name: String(row.display_name),
        });
      }
    }

    const briefingContent = {
      date: new Date().toISOString().slice(0, 10),
      summary: `Daily briefing for ${new Date().toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" })}`,
      sections,
      suggested_actions: suggestedActions,
    };

    // Save briefing to database
    const briefingId = crypto.randomUUID();
    const contentJson = JSON.stringify(briefingContent).replace(/'/g, "''");
    const metricsJson = JSON.stringify(metrics).replace(/'/g, "''");

    await executeSql(`
      INSERT INTO sozo.briefing (id, owner_email, briefing_date, content_json, metrics_json, action_count)
      VALUES (
        '${briefingId}',
        N'${email.replace(/'/g, "''")}',
        CAST(SYSUTCDATETIME() AS DATE),
        N'${contentJson}',
        N'${metricsJson}',
        ${suggestedActions.length}
      )
    `);

    // Auto-create actions from briefing
    for (const action of suggestedActions) {
      const actionId = crypto.randomUUID();
      await executeSql(`
        INSERT INTO sozo.action (id, owner_email, title, action_type, priority_score, person_name, source)
        VALUES (
          '${actionId}',
          N'${email.replace(/'/g, "''")}',
          N'${action.title.replace(/'/g, "''")}',
          N'${action.type}',
          ${action.priority},
          ${action.person_name ? `N'${action.person_name.replace(/'/g, "''")}'` : "NULL"},
          'briefing'
        )
      `);
    }

    return NextResponse.json({
      ok: true,
      briefing_id: briefingId,
      sections: sections.length,
      actions_created: suggestedActions.length,
      metrics,
    });
  } catch (error) {
    console.error("[briefing/generate] Error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Briefing generation failed" },
      { status: 500 },
    );
  }
}
