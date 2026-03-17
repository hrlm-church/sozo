import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> },
) {
  try {
    const { id } = await params;
    const personId = parseInt(id, 10);
    if (isNaN(personId)) {
      return NextResponse.json({ error: "Invalid person_id" }, { status: 400 });
    }

    // personId is a validated integer — safe for direct interpolation
    const [profile, givingTimeline, tags, events, wealth, riskScore, subscriptions] =
      await Promise.all([
        executeSql(
          `SELECT person_id, display_name, email, lifecycle_stage,
                  total_given, avg_gift, first_gift_date, last_gift_date,
                  days_since_last, donation_count, largest_gift,
                  fund_count, active_months
           FROM serving.donor_summary
           WHERE person_id = ${personId}
             AND display_name <> 'Unknown'`,
          30000,
        ),
        executeSql(
          `SELECT donation_month, gifts, amount, primary_fund
           FROM serving.donor_monthly
           WHERE person_id = ${personId}
             AND display_name <> 'Unknown'
           ORDER BY donation_month`,
          30000,
        ),
        executeSql(
          `SELECT tag_value AS tag_name, tag_group
           FROM serving.tag_detail
           WHERE person_id = ${personId}
             AND display_name <> 'Unknown'
           ORDER BY tag_group, tag_value`,
          30000,
        ),
        executeSql(
          `SELECT event_name, ticket_type, attendee_name, buyer_name,
                  checked_in, price, city, state
           FROM serving.event_detail
           WHERE person_id = ${personId}
             AND display_name <> 'Unknown'
           ORDER BY event_name`,
          30000,
        ),
        executeSql(
          `SELECT capacity_label, giving_capacity
           FROM serving.wealth_screening
           WHERE person_id = ${personId}
             AND display_name <> 'Unknown'`,
          30000,
        ),
        executeSql(
          `SELECT score_value, score_label, drivers_json, as_of_date
           FROM intel.person_score
           WHERE person_id = ${personId}
             AND score_type = 'churn_risk'`,
          30000,
        ),
        executeSql(
          `SELECT subscription_status, source_system
           FROM serving.subscription_detail
           WHERE person_id = ${personId}
             AND display_name <> 'Unknown'`,
          30000,
        ),
      ]);

    for (const r of [profile, givingTimeline, tags, events, wealth, riskScore, subscriptions]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    if (profile.rows.length === 0) {
      return NextResponse.json({ error: "Person not found" }, { status: 404 });
    }

    return NextResponse.json({
      profile: profile.rows[0],
      giving_timeline: givingTimeline.rows,
      tags: tags.rows,
      events: events.rows,
      wealth: wealth.rows[0] ?? null,
      risk_score: riskScore.rows[0] ?? null,
      subscriptions: subscriptions.rows,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Person query failed" },
      { status: 500 },
    );
  }
}
