/**
 * Schema context for the LLM system prompt.
 * SERVING VIEWS are pre-joined — the LLM should NEVER need to write JOINs.
 * KEEP THIS CONCISE — every token counts against rate limits.
 */

export const SCHEMA_CONTEXT = `
## Serving Views (pre-joined — NO JOINs needed)
All views have person_id (internal key — NEVER show to users), display_name, email. T-SQL: TOP (N), never LIMIT.

serving.person_360 (84K) — person_id, display_name, first_name, last_name, email, phone, city, state, postal_code, donation_count, lifetime_giving, avg_gift, largest_gift, first_gift_date, last_gift_date, recency_days, order_count, total_spent, tag_count, note_count, comm_count, lifecycle_stage ('prospect','active','cooling','lapsed','lost')

serving.donor_summary (5K) — person_id, display_name, email, donation_count, total_given, avg_gift, largest_gift, first_gift_date, last_gift_date, days_since_last, fund_count, active_months, lifecycle_stage

serving.donor_monthly (62K) — person_id, display_name, donation_month (yyyy-MM), donation_year, gifts, amount, primary_fund

serving.donation_detail (66K, 2014-2025) — donation_id, person_id, display_name, amount, donated_at, donation_month, donation_year, payment_method, fund, appeal, source_system

serving.tag_detail (3M) — person_id, display_name, tag_value, tag_group, applied_at
tag_group: 'Donor Assignment','True Girl','B2BB','Nurture Tags','True Productions','Customer Tags','Box Tracking',NULL

serving.order_detail (205K) — order_id, person_id, display_name, total_amount, order_date, order_month, order_status
serving.payment_detail (135K) — payment_id, person_id, display_name, amount, payment_date, payment_month, payment_method
serving.invoice_detail (205K) — invoice_id, person_id, display_name, invoice_total, invoice_status, issued_at, invoice_month
serving.subscription_detail (6K) — person_id, display_name, product_name, amount, cadence, subscription_status ('Active':41, 'Inactive':6.3K)
serving.household_360 (55K) — household_id, name, member_count, household_giving_total, giving_trend
serving.communication_detail (24K) — person_id, display_name, channel, direction, subject, sent_at

serving.wealth_screening (1.1K) — person_id, keap_id, display_name, email, giving_capacity (dollars), capacity_tier (letter A-L), capacity_min, capacity_max, capacity_label ('Ultra High ($250K+)':29, 'Very High ($100K-$250K)':114, 'High ($25K-$100K)':391, 'Medium ($10K-$25K)':178, 'Standard':397), prospect_rating, quality_score, real_estate_value
USE FOR: "donor capacity", "wealth screening", "giving below capacity", "major gift prospects"
JOIN to donor_summary: WHERE w.person_id = d.person_id to compare giving_capacity vs total_given

serving.lost_recurring_donors (383) — person_id, dd_account_nbr, display_name, monthly_amount, annual_value, frequency, status, going_to_givebutter, category, source_code
USE FOR: "lost recurring donors", "MRR recovery", "Kindful migration losses". Total lost MRR: $17K/month ($206K/year)

serving.stripe_customer (6.8K) — person_id, stripe_customer_id, email, card_holder_name, display_name, total_spend, payment_count, refunded_volume, dispute_losses, created_date
USE FOR: "Stripe customers", "unlinked payments", "payment gaps". $2M total spend, 3,292 linked to identity, 1,946 unlinked with $1.27M in spend

## SQL Rules
- ALWAYS use serving.* views — NEVER JOIN tables manually
- NEVER include person_id, donation_id, or any _id column in SELECT output — they are internal keys
- ALWAYS add: WHERE display_name <> 'Unknown' — when querying donors or top-N lists
- For donors: use donor_summary or donor_monthly, never CTEs on donation_detail
- TOP (N) with parens, DATEADD(YEAR,-2,GETDATE()) for relative dates
- display_name is unique enough for display — just SELECT display_name, never concatenate IDs
- NEVER self-join donor_monthly or donation_detail — self-joins cause row duplication that inflates SUM/COUNT. Use subqueries or window functions instead.
- For drill_down_table: return ONLY the detail columns (display_name, month, amount). The widget auto-computes group totals. NEVER include a pre-computed total column alongside detail rows — it causes double-counting.
- When you need to ORDER BY a period total but SHOW monthly detail, use a subquery for the ranking and a separate query for detail, or use a window function: SUM(amount) OVER (PARTITION BY person_id) as period_total — but ONLY if querying a single table with no JOINs.

## Query Patterns
Top 20 donors (lifetime): SELECT TOP (20) display_name, total_given, donation_count, avg_gift, last_gift_date FROM serving.donor_summary WHERE display_name <> 'Unknown' ORDER BY total_given DESC

Top 20 donors monthly (by lifetime giving): SELECT m.display_name, m.donation_month, m.amount FROM serving.donor_monthly m WHERE m.person_id IN (SELECT TOP (20) person_id FROM serving.donor_summary WHERE display_name <> 'Unknown' ORDER BY total_given DESC) AND m.donation_month >= FORMAT(DATEADD(YEAR,-2,GETDATE()),'yyyy-MM') ORDER BY m.display_name, m.donation_month
→ Use drill_down_table with groupKey='display_name', detailColumns=['donation_month','amount']

Top 20 donors by 2-year consolidated total with monthly detail: SELECT m.display_name, m.donation_month, m.amount FROM serving.donor_monthly m WHERE m.donation_month >= FORMAT(DATEADD(YEAR,-2,GETDATE()),'yyyy-MM') AND m.display_name <> 'Unknown' AND m.person_id IN (SELECT TOP (20) person_id FROM serving.donor_monthly WHERE donation_month >= FORMAT(DATEADD(YEAR,-2,GETDATE()),'yyyy-MM') AND display_name <> 'Unknown' GROUP BY person_id ORDER BY SUM(amount) DESC) ORDER BY (SELECT SUM(x.amount) FROM serving.donor_monthly x WHERE x.person_id = m.person_id AND x.donation_month >= FORMAT(DATEADD(YEAR,-2,GETDATE()),'yyyy-MM')) DESC, m.display_name, m.donation_month
→ Use drill_down_table with groupKey='display_name', detailColumns=['donation_month','amount']
→ The widget will auto-sum the amount column per donor — do NOT add a total column
`.trim();
