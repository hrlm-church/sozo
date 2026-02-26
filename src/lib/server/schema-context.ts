/**
 * Schema context for the LLM system prompt.
 * The LLM queries serving detail views (identity-resolved) + silver tables for flexibility.
 * KEEP THIS CONCISE — every token counts against rate limits.
 */

export const SCHEMA_CONTEXT = `
## Database (Azure SQL, T-SQL — use TOP (N) not LIMIT)

### Person Demographics (89K unique people, 13 sources)
silver.contact (212K) — contact_id, source_system, source_id, first_name, last_name, email_primary, phone_primary, city, state, postal_code, date_of_birth, gender, organization_name, lifecycle_stage, created_at
silver.identity_map (212K) — master_id (unified person key), contact_id, source_system, source_id, is_primary
To get a person's best info: JOIN silver.identity_map im ON im.is_primary = 1 JOIN silver.contact c ON c.contact_id = im.contact_id
All serving views below use person_id = master_id from identity_map. person_id is internal — NEVER include in SELECT output.

### Giving ($6.7M lifetime, 5K donors)
serving.donor_summary (5K) — person_id, display_name, email, donation_count, total_given, avg_gift, largest_gift, first_gift_date, last_gift_date, days_since_last, fund_count, active_months, lifecycle_stage ('active','cooling','lapsed','lost')
serving.donor_monthly (62K) — person_id, display_name, donation_month (yyyy-MM), donation_year, gifts, amount, primary_fund
serving.donation_detail (66K, 2014-2025) — person_id, display_name, amount, donated_at, donation_month, donation_year, payment_method, fund, appeal, source_system

### Commerce (Keap order items classified as 'commerce' via silver.product_classification — excludes donations, subscriptions, events, shipping, tax)
serving.order_detail (205K Keap) — person_id, display_name, total_amount, order_date, order_month, order_status
serving.woo_order_detail (67K WooCommerce) — person_id, display_name, email, order_number, order_date, order_month, order_year, revenue, net_sales, status, product_name, items_sold, coupon, customer_type, city, state
silver.shopify_order (5K) — customer_email, total, line_item_name, line_item_price, paid_at, vendor, billing_city, billing_state (JOIN to identity via email)

### Payments
serving.payment_detail (135K Keap) — person_id, display_name, amount, payment_date, payment_month, payment_method
serving.stripe_charge_detail (163K) — person_id, display_name, email, amount, amount_refunded, status, description, card_brand, card_last4, created_at, charge_month, charge_year, fee, meta_source, meta_from_app

### Subscriptions
serving.subscription_detail (8K) — person_id, display_name, product_name, amount, cadence, subscription_status ('Active','Inactive'), source_system ('keap','subbly')
CRITICAL: Keap subscriptions are STALE. ALWAYS filter WHERE source_system = 'subbly' for active subscriptions.
silver.subbly_subscription (2.4K) — customer_email, customer_name, product_name, status, renewal_date, date_created, date_cancelled, cancellation_reason, orders_count, girl_name

### Events (21K tickets, 53 events)
serving.event_detail (21K) — person_id, display_name, event_name, ticket_type, payment_date, event_month, event_year, order_total, ticket_total, price, checked_in, attendee_name, buyer_name, city, state

### Tags (5.7M assignments)
serving.tag_detail (5.7M) — person_id, display_name, tag_value, tag_group, applied_at, source_system
tag_group values: 'Donor Assignment','True Girl','B2BB','Nurture Tags','Customer Tags','Box Tracking','Mailchimp Audience','Shopify Customer'

### Engagement
serving.communication_detail (24K) — person_id, display_name, channel, direction, subject, sent_at

### Wealth Screening (1.1K screened contacts)
serving.wealth_screening (1.1K) — person_id, display_name, email, giving_capacity (ANNUAL estimate), capacity_label ('Ultra High ($250K+)':29, 'Very High ($100K-$250K)':114, 'High ($25K-$100K)':391, 'Medium ($10K-$25K)':178, 'Standard':397), quality_score
JOIN to donor_summary on person_id. IMPORTANT: giving_capacity is ANNUAL — always compare against annualized giving (total_given / years active), never raw lifetime total_given.

### Special Views
serving.lost_recurring_donors (383) — person_id, display_name, monthly_amount, annual_value, frequency, status, category. Lost MRR: $17K/month ($206K/year).
serving.stripe_customer (6.8K) — person_id, email, display_name, total_spend, payment_count, refunded_volume

## SQL Rules
- NEVER include person_id, donation_id, or any _id column in SELECT output
- ALWAYS add WHERE display_name <> 'Unknown' on top-N or donor queries
- Use serving.* views for identity-resolved queries (they have person_id + display_name)
- Use silver.* tables directly when you need columns not in serving views, or for source-specific analysis
- For person demographics across all 89K people: use silver.contact + silver.identity_map
- For donor rankings/totals: use donor_summary (pre-aggregated). For monthly trends: use donor_monthly.
- NEVER self-join donor_monthly or donation_detail — use subqueries or window functions
- Commerce totals (order_detail.total_amount, person_360.total_spent) ONLY include items classified as 'commerce' in silver.product_classification. Donations, subscriptions, events, shipping, and tax are excluded. Donations are tracked separately in serving.donation_detail and donor_summary.
- For drill_down_table: return ONLY detail columns. Widget auto-computes group totals.
- TOP (N) with parens, DATEADD(YEAR,-2,GETDATE()) for relative dates
- SELECT only the columns the user asks for — never dump all columns

## Query Patterns
Top N donors: SELECT TOP (N) display_name, total_given, avg_gift, last_gift_date, lifecycle_stage FROM serving.donor_summary WHERE display_name <> 'Unknown' ORDER BY total_given DESC
Monthly trends: SELECT m.display_name, m.donation_month, m.amount FROM serving.donor_monthly m WHERE m.person_id IN (SELECT TOP (N) person_id FROM serving.donor_summary WHERE display_name <> 'Unknown' ORDER BY total_given DESC) ORDER BY m.display_name, m.donation_month
Cross-domain: JOIN serving views on person_id to combine giving + commerce + events for the same person
`.trim();
