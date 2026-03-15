import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [kpis, leaderboard, geo, ticketTypes, checkinRate] =
      await Promise.all([
        // summary_kpis
        executeSql(
          `SELECT COUNT(DISTINCT event_name) AS total_events,
                  COUNT(*) AS total_attendees,
                  SUM(price) AS total_revenue,
                  AVG(price) AS avg_ticket_price
           FROM serving.event_detail
           WHERE display_name <> 'Unknown'`,
          30000,
        ),

        // event_leaderboard: top 20 by attendance
        executeSql(
          `SELECT TOP 20
                  event_name,
                  COUNT(*) AS attendee_count,
                  SUM(price) AS total_revenue,
                  SUM(CASE WHEN checked_in = 1 THEN 1 ELSE 0 END) AS checked_in_count
           FROM serving.event_detail
           WHERE display_name <> 'Unknown'
           GROUP BY event_name
           ORDER BY attendee_count DESC`,
          30000,
        ),

        // geo_breakdown: by state
        executeSql(
          `SELECT state,
                  COUNT(*) AS attendee_count
           FROM serving.event_detail
           WHERE display_name <> 'Unknown'
             AND state IS NOT NULL
           GROUP BY state
           ORDER BY attendee_count DESC`,
          30000,
        ),

        // ticket_types
        executeSql(
          `SELECT ticket_type,
                  COUNT(*) AS ticket_count,
                  SUM(price) AS total_revenue
           FROM serving.event_detail
           WHERE display_name <> 'Unknown'
             AND ticket_type IS NOT NULL
           GROUP BY ticket_type
           ORDER BY ticket_count DESC`,
          30000,
        ),

        // checkin_rate
        executeSql(
          `SELECT COUNT(*) AS total_attendees,
                  SUM(CASE WHEN checked_in = 1 THEN 1 ELSE 0 END) AS checked_in_count,
                  CAST(SUM(CASE WHEN checked_in = 1 THEN 1.0 ELSE 0.0 END) / NULLIF(COUNT(*), 0) AS DECIMAL(5,4)) AS checkin_rate
           FROM serving.event_detail
           WHERE display_name <> 'Unknown'`,
          30000,
        ),
      ]);

    for (const r of [kpis, leaderboard, geo, ticketTypes, checkinRate]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    return NextResponse.json({
      summary_kpis: kpis.rows[0] ?? null,
      event_leaderboard: leaderboard.rows,
      geo_breakdown: geo.rows,
      ticket_types: ticketTypes.rows,
      checkin_rate: checkinRate.rows[0] ?? null,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Events query failed" },
      { status: 500 },
    );
  }
}
