/**
 * Schema context for the LLM system prompt.
 * SERVING VIEWS are pre-joined — the LLM should NEVER need to write JOINs.
 * KEEP THIS CONCISE — every token counts against rate limits.
 */

export const SCHEMA_CONTEXT = `
## Serving Views (pre-joined, use these — NO JOINs needed)
All views include person_id, display_name, email. T-SQL: use TOP (N), never LIMIT.

serving.person_360 (84K) — person_id, display_name, first_name, last_name, email, phone, city, state, postal_code, donation_count, lifetime_giving, avg_gift, largest_gift, first_gift_date, last_gift_date, recency_days, order_count, total_spent, tag_count, note_count, comm_count, lifecycle_stage ('prospect','active','cooling','lapsed','lost')

serving.donor_summary (5K) — person_id, display_name, email, donation_count, total_given, avg_gift, largest_gift, first_gift_date, last_gift_date, days_since_last, fund_count, active_months, lifecycle_stage
USE FOR: "top donors", "biggest givers". Example: SELECT TOP (20) * FROM serving.donor_summary ORDER BY total_given DESC

serving.donor_monthly (62K) — person_id, display_name, donation_month (yyyy-MM), donation_year, gifts, amount, primary_fund
USE FOR: "giving by month", "monthly breakdown". Top 20 monthly: SELECT m.* FROM serving.donor_monthly m WHERE m.person_id IN (SELECT TOP (20) person_id FROM serving.donor_summary ORDER BY total_given DESC) ORDER BY m.display_name, m.donation_month

serving.donation_detail (66K, 2014-2025) — donation_id, person_id, display_name, amount, donated_at, donation_month, donation_year, payment_method, fund, appeal, source_system

serving.tag_detail (3M rows) — person_id, display_name, tag_value, tag_group, applied_at
Key tag_group values: 'Donor Assignment'(18K), 'True Girl'(65K), 'B2BB'(12K), 'Nurture Tags'(208K), 'True Productions'(134K), 'Customer Tags'(76K), 'Box Tracking'(12K), NULL(1.8M)

serving.order_detail (205K) — order_id, person_id, display_name, total_amount, order_date, order_month, order_status
serving.payment_detail (135K) — payment_id, person_id, display_name, amount, payment_date, payment_month, payment_method
serving.invoice_detail (205K) — invoice_id, person_id, display_name, invoice_total, invoice_status, issued_at, invoice_month
serving.subscription_detail (6K) — person_id, display_name, product_name, amount, cadence, subscription_status ('Active':46, 'Inactive':6.3K)
serving.household_360 (55K) — household_id, name, member_count, household_giving_total, giving_trend
serving.communication_detail (24K) — person_id, display_name, channel ('EMAIL','Note','CONV','Phone Call'), direction, subject, sent_at

## Key Stats: 84K people, 5K donors, $7.1M giving, 66K donations, 205K orders, 3M tags, 24K comms

## Rules
- ALWAYS use serving.* views — NEVER JOIN silver tables manually
- For donors: use donor_summary or donor_monthly, never CTEs on donation_detail
- TOP (N) with parens, DATEADD(YEAR,-2,GETDATE()) for relative dates
- display_name not unique: GROUP BY person_id or use MAX(display_name)
`.trim();
