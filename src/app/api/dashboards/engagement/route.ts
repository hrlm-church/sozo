import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [topEngaged, commTrend, unengaged] =
      await Promise.all([
        // top_engaged: top 30 donors by donation_count + event attendance
        executeSql(
          `SELECT TOP 30
                  ds.person_id,
                  ds.display_name,
                  ds.donation_count,
                  ds.total_given,
                  ds.lifecycle_stage,
                  ds.last_gift_date,
                  ds.active_months,
                  ISNULL(ec.event_count, 0) AS event_count
           FROM serving.donor_summary ds
           LEFT JOIN (
             SELECT person_id, COUNT(*) AS event_count
             FROM serving.event_detail
             WHERE display_name <> 'Unknown'
             GROUP BY person_id
           ) ec ON ds.person_id = ec.person_id
           WHERE ds.display_name <> 'Unknown'
           ORDER BY ds.donation_count + ISNULL(ec.event_count, 0) DESC`,
          30000,
        ),

        // communication_trend: monthly count last 12 months
        executeSql(
          `SELECT FORMAT(sent_at, 'yyyy-MM') AS comm_month,
                  channel,
                  COUNT(*) AS message_count
           FROM serving.communication_detail
           WHERE display_name <> 'Unknown'
             AND sent_at >= DATEADD(month, -12, GETDATE())
           GROUP BY FORMAT(sent_at, 'yyyy-MM'), channel
           ORDER BY comm_month`,
          30000,
        ),

        // unengaged_high_value: high giving but inactive
        executeSql(
          `SELECT TOP 30
                  ds.person_id,
                  ds.display_name,
                  ds.total_given,
                  ds.last_gift_date,
                  ds.days_since_last,
                  ds.lifecycle_stage,
                  ds.donation_count
           FROM serving.donor_summary ds
           WHERE ds.display_name <> 'Unknown'
             AND ds.total_given > 1000
             AND ds.days_since_last > 365
           ORDER BY ds.total_given DESC`,
          30000,
        ),
      ]);

    for (const r of [topEngaged, commTrend, unengaged]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    return NextResponse.json({
      top_engaged: topEngaged.rows,
      communication_trend: commTrend.rows,
      unengaged_high_value: unengaged.rows,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Engagement query failed" },
      { status: 500 },
    );
  }
}
