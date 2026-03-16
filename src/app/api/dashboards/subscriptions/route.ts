import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [summary, lostSubscribers, statusBreakdown] = await Promise.all([
      // summary: active, inactive, estimated MRR
      executeSql(
        `SELECT COUNT(CASE WHEN subscription_status = 'active' THEN 1 END) AS active_count,
                COUNT(CASE WHEN subscription_status <> 'active' THEN 1 END) AS inactive_count,
                COUNT(*) AS total_count
         FROM serving.subscription_detail
         WHERE source_system = 'subbly'
           AND display_name <> 'Unknown'`,
        30000,
      ),

      // lost_subscribers: top 20 by value from lost_recurring_donors
      executeSql(
        `SELECT TOP 20
                person_id,
                display_name,
                annual_value,
                monthly_amount,
                last_used_date,
                status,
                category
         FROM serving.lost_recurring_donors
         WHERE display_name <> 'Unknown'
         ORDER BY annual_value DESC`,
        30000,
      ),

      // status_breakdown
      executeSql(
        `SELECT subscription_status,
                COUNT(*) AS subscriber_count
         FROM serving.subscription_detail
         WHERE source_system = 'subbly'
           AND display_name <> 'Unknown'
         GROUP BY subscription_status
         ORDER BY subscriber_count DESC`,
        30000,
      ),
    ]);

    for (const r of [summary, lostSubscribers, statusBreakdown]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    return NextResponse.json({
      summary: summary.rows[0] ?? null,
      lost_subscribers: lostSubscribers.rows,
      status_breakdown: statusBreakdown.rows,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Subscriptions query failed" },
      { status: 500 },
    );
  }
}
