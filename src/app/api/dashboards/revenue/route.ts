import { NextResponse } from "next/server";
import { executeSql } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export async function GET() {
  try {
    const [revenueByStream, topFunds, paymentMethods, yoy, kpis] =
      await Promise.all([
        // revenue_by_stream: monthly totals by stream (UNION)
        executeSql(
          `SELECT stream, revenue_month, SUM(amount) AS total_amount
           FROM (
             SELECT 'Donations' AS stream,
                    donation_month AS revenue_month,
                    amount
             FROM serving.donation_detail
             WHERE display_name <> 'Unknown'

             UNION ALL

             SELECT 'Commerce' AS stream,
                    order_month AS revenue_month,
                    total_amount AS amount
             FROM serving.order_detail
             WHERE display_name <> 'Unknown'

             UNION ALL

             SELECT 'Events' AS stream,
                    FORMAT(event_detail.event_name, 'yyyy-MM') AS revenue_month,
                    price AS amount
             FROM serving.event_detail
             WHERE display_name <> 'Unknown'
               AND price IS NOT NULL
           ) combined
           GROUP BY stream, revenue_month
           ORDER BY revenue_month, stream`,
          30000,
        ),

        // top_funds: top 10 by total raised
        executeSql(
          `SELECT TOP 10
                  fund,
                  SUM(amount) AS total_raised,
                  COUNT(*) AS donation_count,
                  COUNT(DISTINCT person_id) AS donor_count
           FROM serving.donation_detail
           WHERE display_name <> 'Unknown'
             AND fund IS NOT NULL
           GROUP BY fund
           ORDER BY total_raised DESC`,
          30000,
        ),

        // payment_methods breakdown
        executeSql(
          `SELECT payment_method,
                  COUNT(*) AS transaction_count,
                  SUM(amount) AS total_amount
           FROM serving.donation_detail
           WHERE display_name <> 'Unknown'
             AND payment_method IS NOT NULL
           GROUP BY payment_method
           ORDER BY total_amount DESC`,
          30000,
        ),

        // yoy_comparison: this year vs last year monthly
        executeSql(
          `SELECT donation_month,
                  donation_year,
                  SUM(amount) AS total_amount,
                  COUNT(*) AS transaction_count
           FROM serving.donation_detail
           WHERE display_name <> 'Unknown'
             AND donation_year >= YEAR(GETDATE()) - 1
           GROUP BY donation_month, donation_year
           ORDER BY donation_month`,
          30000,
        ),

        // summary_kpis
        executeSql(
          `SELECT SUM(amount) AS total_revenue,
                  COUNT(DISTINCT person_id) AS donor_count,
                  AVG(amount) AS avg_gift,
                  COUNT(*) AS total_transactions
           FROM serving.donation_detail
           WHERE display_name <> 'Unknown'`,
          30000,
        ),
      ]);

    // Subscription estimate (separate since it's computed differently)
    const subscriptions = await executeSql(
      `SELECT COUNT(*) AS active_count,
              AVG(CAST(0 AS DECIMAL(18,2))) AS avg_monthly_value
       FROM serving.subscription_detail
       WHERE source_system = 'subbly'
         AND subscription_status = 'active'
         AND display_name <> 'Unknown'`,
      30000,
    );

    for (const r of [revenueByStream, topFunds, paymentMethods, yoy, kpis, subscriptions]) {
      if (!r.ok) {
        return NextResponse.json({ error: r.reason }, { status: 500 });
      }
    }

    return NextResponse.json({
      revenue_by_stream: revenueByStream.rows,
      subscriptions_estimate: subscriptions.rows[0] ?? null,
      top_funds: topFunds.rows,
      payment_methods: paymentMethods.rows,
      yoy_comparison: yoy.rows,
      summary_kpis: kpis.rows[0] ?? null,
    });
  } catch (error) {
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Revenue query failed" },
      { status: 500 },
    );
  }
}
