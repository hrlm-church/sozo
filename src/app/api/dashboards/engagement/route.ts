import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [signalGroups, topEngaged, commTrend, unengaged] =
      await Promise.all([
        // signal_groups: count by tag_group
        executeSql(
          `SELECT tag_group,
                  COUNT(*) AS tag_count,
                  COUNT(DISTINCT person_id) AS person_count
           FROM serving.tag_detail
           WHERE display_name <> 'Unknown'
             AND tag_group IS NOT NULL
           GROUP BY tag_group
           ORDER BY person_count DESC`,
          30000,
        ),

        // top_engaged: top 30 by tag count
        executeSql(
          `SELECT TOP 30
                  td.person_id,
                  ds.display_name,
                  COUNT(*) AS tag_count,
                  ds.total_given,
                  ds.lifecycle_stage,
                  ds.last_gift_date
           FROM serving.tag_detail td
           JOIN serving.donor_summary ds ON td.person_id = ds.person_id
           WHERE ds.display_name <> 'Unknown'
           GROUP BY td.person_id, ds.display_name, ds.total_given,
                    ds.lifecycle_stage, ds.last_gift_date
           ORDER BY tag_count DESC`,
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

        // unengaged_high_value: total_given > 1000 but tag count < 5
        executeSql(
          `SELECT ds.person_id,
                  ds.display_name,
                  ds.total_given,
                  ds.last_gift_date,
                  ds.lifecycle_stage,
                  ISNULL(tc.tag_count, 0) AS tag_count
           FROM serving.donor_summary ds
           LEFT JOIN (
             SELECT person_id, COUNT(*) AS tag_count
             FROM serving.tag_detail
             GROUP BY person_id
           ) tc ON ds.person_id = tc.person_id
           WHERE ds.display_name <> 'Unknown'
             AND ds.total_given > 1000
             AND ISNULL(tc.tag_count, 0) < 5
           ORDER BY ds.total_given DESC`,
          30000,
        ),
      ]);

    for (const r of [signalGroups, topEngaged, commTrend, unengaged]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    return NextResponse.json({
      signal_groups: signalGroups.rows,
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
