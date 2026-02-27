/**
 * Seed the intel layer with dimensions, metrics, synonyms, and allowlists.
 * All SQL expressions adapted to Sozo's actual serving.* table/column names.
 *
 * Table mapping (GPT-5.2 plan → Sozo actual):
 *   dbo.donation_detail   → serving.donation_detail (donated_at, amount, fund, appeal, payment_method, source_system)
 *   dbo.donor_summary     → serving.donor_summary   (total_given, avg_gift, first_gift_date, last_gift_date, lifecycle_stage)
 *   dbo.donor_monthly     → serving.donor_monthly    (donation_month, donation_year, gifts, amount)
 *   dbo.order_detail      → serving.order_detail     (order_date, total_amount, order_status)
 *   dbo.event_detail      → serving.event_detail     (payment_date, order_total, event_name, ticket_type)
 *   dbo.subscription_detail → serving.subscription_detail (subscription_status, cadence, amount, source_system)
 *   dbo.communication_detail → serving.communication_detail (sent_at, channel, direction)
 *   dbo.payment_detail    → serving.payment_detail   (payment_date, amount, payment_method)
 *   dbo.tag_detail        → serving.tag_detail       (tag_value, tag_group, applied_at)
 *   dbo.wealth_screening  → serving.wealth_screening (giving_capacity, capacity_label, quality_score)
 *   dbo.lost_recurring_donors → serving.lost_recurring_donors (monthly_amount, annual_value, status, category)
 *
 * Usage: node scripts/pipeline/seed_intel_catalog.js
 */
const fs = require('fs');
const path = require('path');
const sql = require('mssql');

function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

async function main() {
  loadEnv();
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 120000,
  });

  console.log('Seeding intel catalog...\n');

  // ── 1. DIMENSIONS ────────────────────────────────────────────────────────
  console.log('=== Seeding dimensions ===');
  const dimensions = [
    // Time dimensions
    { key: 'time.date', display: 'Date', desc: 'Transaction/event date', table: 'serving.donation_detail', col: 'donated_at', dtype: 'date', isTime: 1, ops: '["=","between",">=","<=",">","<"]' },
    { key: 'time.month', display: 'Month', desc: 'Year-month (yyyy-MM)', table: 'serving.donation_detail', col: 'donation_month', dtype: 'nvarchar', isTime: 1, ops: '["=","in","between",">=","<="]' },
    { key: 'time.year', display: 'Year', desc: 'Calendar year', table: 'serving.donation_detail', col: 'donation_year', dtype: 'int', isTime: 1, ops: '["=","in","between",">=","<="]' },

    // Person dimensions
    { key: 'person.lifecycle_stage', display: 'Lifecycle Stage', desc: 'Donor lifecycle: active, cooling, lapsed, lost', table: 'serving.donor_summary', col: 'lifecycle_stage', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]', values: '["active","cooling","lapsed","lost"]' },
    { key: 'person.state', display: 'State', desc: 'Person state/province', table: 'silver.contact', col: 'state', dtype: 'nvarchar', isTime: 0, ops: '["=","in","like"]' },
    { key: 'person.city', display: 'City', desc: 'Person city', table: 'silver.contact', col: 'city', dtype: 'nvarchar', isTime: 0, ops: '["=","in","like"]' },
    { key: 'person.gender', display: 'Gender', desc: 'Person gender', table: 'silver.contact', col: 'gender', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]' },
    { key: 'person.source_system', display: 'Source System', desc: 'Origin CRM/platform', table: 'silver.contact', col: 'source_system', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]' },

    // Giving dimensions
    { key: 'giving.fund', display: 'Fund', desc: 'Donation fund/designation', table: 'serving.donation_detail', col: 'fund', dtype: 'nvarchar', isTime: 0, ops: '["=","in","like"]' },
    { key: 'giving.appeal', display: 'Appeal', desc: 'Campaign/appeal code', table: 'serving.donation_detail', col: 'appeal', dtype: 'nvarchar', isTime: 0, ops: '["=","in","like"]' },
    { key: 'giving.payment_method', display: 'Payment Method', desc: 'Method of payment', table: 'serving.donation_detail', col: 'payment_method', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]' },
    { key: 'giving.source_system', display: 'Giving Source', desc: 'Source system for donations', table: 'serving.donation_detail', col: 'source_system', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]' },

    // Commerce dimensions
    { key: 'commerce.order_status', display: 'Order Status', desc: 'Order status', table: 'serving.order_detail', col: 'order_status', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]' },
    { key: 'commerce.order_month', display: 'Order Month', desc: 'Order year-month', table: 'serving.order_detail', col: 'order_month', dtype: 'nvarchar', isTime: 1, ops: '["=","in","between"]' },

    // Event dimensions
    { key: 'events.event_name', display: 'Event Name', desc: 'Event title', table: 'serving.event_detail', col: 'event_name', dtype: 'nvarchar', isTime: 0, ops: '["=","in","like"]' },
    { key: 'events.ticket_type', display: 'Ticket Type', desc: 'Ticket type/level', table: 'serving.event_detail', col: 'ticket_type', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]' },
    { key: 'events.event_year', display: 'Event Year', desc: 'Event year', table: 'serving.event_detail', col: 'event_year', dtype: 'int', isTime: 1, ops: '["=","in","between"]' },

    // Subscription dimensions
    { key: 'subscriptions.product_name', display: 'Subscription Product', desc: 'Subscription product name', table: 'serving.subscription_detail', col: 'product_name', dtype: 'nvarchar', isTime: 0, ops: '["=","in","like"]' },
    { key: 'subscriptions.status', display: 'Subscription Status', desc: 'Active/Inactive', table: 'serving.subscription_detail', col: 'subscription_status', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]', values: '["Active","Inactive"]' },
    { key: 'subscriptions.cadence', display: 'Subscription Cadence', desc: 'Monthly/Quarterly/Annual', table: 'serving.subscription_detail', col: 'cadence', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]' },
    { key: 'subscriptions.source_system', display: 'Subscription Source', desc: 'keap/subbly', table: 'serving.subscription_detail', col: 'source_system', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]', values: '["keap","subbly"]' },

    // Tag dimensions
    { key: 'tags.tag_group', display: 'Tag Group', desc: 'Tag category/group', table: 'serving.tag_detail', col: 'tag_group', dtype: 'nvarchar', isTime: 0, ops: '["=","in","like"]' },
    { key: 'tags.tag_value', display: 'Tag Value', desc: 'Tag value', table: 'serving.tag_detail', col: 'tag_value', dtype: 'nvarchar', isTime: 0, ops: '["=","in","like"]' },

    // Wealth dimensions
    { key: 'wealth.capacity_label', display: 'Capacity Label', desc: 'Wealth capacity band', table: 'serving.wealth_screening', col: 'capacity_label', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]', values: '["Ultra High ($250K+)","Very High ($100K-$250K)","High ($25K-$100K)","Medium ($10K-$25K)","Standard"]' },

    // Communication dimensions
    { key: 'communications.channel', display: 'Channel', desc: 'Communication channel', table: 'serving.communication_detail', col: 'channel', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]' },
    { key: 'communications.direction', display: 'Direction', desc: 'Inbound/Outbound', table: 'serving.communication_detail', col: 'direction', dtype: 'nvarchar', isTime: 0, ops: '["=","in"]', values: '["inbound","outbound"]' },
  ];

  for (const dim of dimensions) {
    const req = pool.request();
    req.input('key', sql.NVarChar, dim.key);
    req.input('display', sql.NVarChar, dim.display);
    req.input('desc', sql.NVarChar, dim.desc);
    req.input('table', sql.NVarChar, dim.table);
    req.input('col', sql.NVarChar, dim.col);
    req.input('dtype', sql.NVarChar, dim.dtype);
    req.input('isTime', sql.Bit, dim.isTime);
    req.input('values', sql.NVarChar, dim.values || null);
    req.input('ops', sql.NVarChar, dim.ops);
    await req.query(`
      IF NOT EXISTS (SELECT 1 FROM intel.dimension_definition WHERE dimension_key = @key)
        INSERT INTO intel.dimension_definition
          (dimension_key, display_name, description, source_table, source_column, data_type, is_time_dimension, allowed_values_json, allowed_operators_json)
        VALUES (@key, @display, @desc, @table, @col, @dtype, @isTime, @values, @ops)
    `);
    console.log(`  dim: ${dim.key}`);
  }

  // ── 2. METRICS ───────────────────────────────────────────────────────────
  console.log('\n=== Seeding metrics ===');

  // All SQL adapted to serving.* tables with correct column names
  const metrics = [
    // ── GIVING ──────────────────────────────────────────────
    {
      key: 'giving.total_donations_usd', display: 'Total Donations ($)',
      desc: 'Sum of donation amounts in window', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'donation', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(SUM(d.amount) AS DECIMAL(18,4)) AS value
FROM serving.donation_detail d
WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
  AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'giving.donation_count', display: 'Donation Count',
      desc: 'Number of donations in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'donation', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM serving.donation_detail d
WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
  AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'giving.avg_donation_usd', display: 'Average Gift ($)',
      desc: 'Average donation amount in window', type: 'ratio', unit: 'usd', fmt: 'currency',
      grain: 'donation', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(AVG(CAST(d.amount AS DECIMAL(18,4))) AS DECIMAL(18,4)) AS value
FROM serving.donation_detail d
WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
  AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'giving.unique_donors', display: 'Unique Donors',
      desc: 'Distinct donors who gave in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(COUNT_BIG(DISTINCT d.person_id) AS DECIMAL(18,4)) AS value
FROM serving.donation_detail d
WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
  AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'giving.new_donors', display: 'New Donors',
      desc: 'Donors whose first gift occurs in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH first_gift AS (
  SELECT person_id, MIN(donated_at) AS first_gift_date
  FROM serving.donation_detail
  GROUP BY person_id
)
SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM first_gift fg
WHERE (@start_date IS NULL OR fg.first_gift_date >= @start_date)
  AND (@end_date   IS NULL OR fg.first_gift_date < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'giving.reactivated_donors', display: 'Reactivated Donors',
      desc: 'Donors who gave in window after 12+ months since prior gift', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH gifts AS (
  SELECT person_id, donated_at,
         LAG(donated_at) OVER (PARTITION BY person_id ORDER BY donated_at) AS prev_date
  FROM serving.donation_detail
),
reactivations AS (
  SELECT person_id, donated_at
  FROM gifts
  WHERE prev_date IS NOT NULL
    AND DATEDIFF(DAY, prev_date, donated_at) >= 365
)
SELECT CAST(COUNT_BIG(DISTINCT r.person_id) AS DECIMAL(18,4)) AS value
FROM reactivations r
WHERE (@start_date IS NULL OR r.donated_at >= @start_date)
  AND (@end_date   IS NULL OR r.donated_at < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'giving.major_donor_count', display: 'Major Donors (>= $1,000)',
      desc: 'Distinct donors giving at least $1,000 in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH per_person AS (
  SELECT d.person_id, SUM(d.amount) AS total_amt
  FROM serving.donation_detail d
  WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
    AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))
  GROUP BY d.person_id
)
SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM per_person WHERE total_amt >= 1000`
    },
    {
      key: 'giving.donor_retention_rate', display: 'Donor Retention Rate',
      desc: 'Percent of prior-period donors who gave again in current period', type: 'ratio', unit: 'percent', fmt: 'percent',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH params AS (
  SELECT @start_date AS cur_start, DATEADD(DAY,1,@end_date) AS cur_end_excl,
    DATEADD(DAY, -DATEDIFF(DAY, @start_date, DATEADD(DAY,1,@end_date)), @start_date) AS prior_start,
    @start_date AS prior_end_excl
),
cur AS (SELECT DISTINCT d.person_id FROM serving.donation_detail d CROSS JOIN params p WHERE d.donated_at >= p.cur_start AND d.donated_at < p.cur_end_excl),
prior_d AS (SELECT DISTINCT d.person_id FROM serving.donation_detail d CROSS JOIN params p WHERE d.donated_at >= p.prior_start AND d.donated_at < p.prior_end_excl)
SELECT CASE WHEN (SELECT COUNT_BIG(1) FROM prior_d) = 0 THEN NULL
  ELSE CAST(100.0 * (SELECT COUNT_BIG(1) FROM prior_d pr INNER JOIN cur cu ON cu.person_id = pr.person_id)
    / (SELECT COUNT_BIG(1) FROM prior_d) AS DECIMAL(18,4)) END AS value`
    },
    {
      key: 'giving.churned_donors', display: 'Churned Donors',
      desc: 'Donors who gave in prior period but not in current period', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH params AS (
  SELECT @start_date AS cur_start, DATEADD(DAY,1,@end_date) AS cur_end_excl,
    DATEADD(DAY, -DATEDIFF(DAY, @start_date, DATEADD(DAY,1,@end_date)), @start_date) AS prior_start,
    @start_date AS prior_end_excl
),
cur AS (SELECT DISTINCT d.person_id FROM serving.donation_detail d CROSS JOIN params p WHERE d.donated_at >= p.cur_start AND d.donated_at < p.cur_end_excl),
prior_d AS (SELECT DISTINCT d.person_id FROM serving.donation_detail d CROSS JOIN params p WHERE d.donated_at >= p.prior_start AND d.donated_at < p.prior_end_excl)
SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM prior_d pr LEFT JOIN cur cu ON cu.person_id = pr.person_id WHERE cu.person_id IS NULL`
    },
    {
      key: 'giving.lifetime_giving_usd', display: 'Lifetime Giving ($)',
      desc: 'Sum of all donations (all-time)', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'person', window: 'all_time', deps: null,
      sql: `SELECT CAST(SUM(d.amount) AS DECIMAL(18,4)) AS value FROM serving.donation_detail d`
    },
    {
      key: 'giving.median_gift_usd', display: 'Median Gift ($)',
      desc: 'Median donation amount in window', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'donation', window: 'last_12_months', deps: null,
      sql: `WITH x AS (
  SELECT CAST(d.amount AS DECIMAL(18,4)) AS amt
  FROM serving.donation_detail d
  WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
    AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))
)
SELECT TOP 1 CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amt) OVER() AS DECIMAL(18,4)) AS value FROM x`
    },
    {
      key: 'giving.gift_frequency_per_donor', display: 'Gift Frequency (gifts/donor)',
      desc: 'Average number of gifts per donor in window', type: 'ratio', unit: 'count', fmt: 'decimal',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH base AS (
  SELECT COUNT_BIG(1) AS gifts, COUNT_BIG(DISTINCT d.person_id) AS donors
  FROM serving.donation_detail d
  WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
    AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))
)
SELECT CASE WHEN donors = 0 THEN NULL ELSE CAST(1.0 * gifts / donors AS DECIMAL(18,4)) END AS value FROM base`
    },
    {
      key: 'giving.days_since_last_gift_avg', display: 'Avg Days Since Last Gift',
      desc: 'Average days since each donor last gave', type: 'aggregate', unit: 'days', fmt: 'decimal',
      grain: 'person', window: 'all_time', deps: null,
      sql: `SELECT CAST(AVG(CAST(ds.days_since_last AS DECIMAL(18,4))) AS DECIMAL(18,4)) AS value
FROM serving.donor_summary ds WHERE ds.days_since_last IS NOT NULL`
    },
    {
      key: 'giving.payment_total_usd', display: 'Total Payments ($)',
      desc: 'Sum of payments in window', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'payment', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(SUM(p.amount) AS DECIMAL(18,4)) AS value
FROM serving.payment_detail p
WHERE (@start_date IS NULL OR p.payment_date >= @start_date)
  AND (@end_date   IS NULL OR p.payment_date < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'giving.top10_donor_concentration_pct', display: 'Top 10 Donor Concentration (%)',
      desc: 'Percent of total giving from top 10 donors', type: 'ratio', unit: 'percent', fmt: 'percent',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH per_person AS (
  SELECT d.person_id, SUM(d.amount) AS amt
  FROM serving.donation_detail d
  WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
    AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))
  GROUP BY d.person_id
),
tot AS (SELECT SUM(amt) AS total_amt FROM per_person),
top10 AS (SELECT TOP (10) amt FROM per_person ORDER BY amt DESC)
SELECT CASE WHEN (SELECT total_amt FROM tot) IS NULL OR (SELECT total_amt FROM tot)=0 THEN NULL
  ELSE CAST(100.0 * (SELECT SUM(amt) FROM top10) / (SELECT total_amt FROM tot) AS DECIMAL(18,4)) END AS value`
    },

    // ── COMMERCE ────────────────────────────────────────────
    {
      key: 'commerce.total_order_revenue_usd', display: 'Total Order Revenue ($)',
      desc: 'Sum of order totals in window (excludes $0 non-commerce orders)', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'order', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(SUM(o.total_amount) AS DECIMAL(18,4)) AS value
FROM serving.order_detail o
WHERE o.total_amount > 0
  AND (@start_date IS NULL OR o.order_date >= @start_date)
  AND (@end_date   IS NULL OR o.order_date < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'commerce.order_count', display: 'Order Count',
      desc: 'Number of commerce orders in window (excludes $0 non-commerce orders)', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'order', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM serving.order_detail o
WHERE o.total_amount > 0
  AND (@start_date IS NULL OR o.order_date >= @start_date)
  AND (@end_date   IS NULL OR o.order_date < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'commerce.avg_order_value_usd', display: 'Average Order Value ($)',
      desc: 'Average order total in window (excludes $0 non-commerce orders)', type: 'ratio', unit: 'usd', fmt: 'currency',
      grain: 'order', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(AVG(CAST(o.total_amount AS DECIMAL(18,4))) AS DECIMAL(18,4)) AS value
FROM serving.order_detail o
WHERE o.total_amount > 0
  AND (@start_date IS NULL OR o.order_date >= @start_date)
  AND (@end_date   IS NULL OR o.order_date < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'commerce.unique_buyers', display: 'Unique Buyers',
      desc: 'Distinct people who placed a commerce order (total > $0) in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `SELECT CAST(COUNT_BIG(DISTINCT o.person_id) AS DECIMAL(18,4)) AS value
FROM serving.order_detail o
WHERE o.total_amount > 0
  AND (@start_date IS NULL OR o.order_date >= @start_date)
  AND (@end_date   IS NULL OR o.order_date < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'commerce.woo_revenue_usd', display: 'WooCommerce Revenue ($)',
      desc: 'WooCommerce net sales in window', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'order', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(SUM(w.net_sales) AS DECIMAL(18,4)) AS value
FROM serving.woo_order_detail w
WHERE (@start_date IS NULL OR w.order_date >= @start_date)
  AND (@end_date   IS NULL OR w.order_date < DATEADD(DAY,1,@end_date))`
    },

    // ── EVENTS ──────────────────────────────────────────────
    {
      key: 'events.total_event_revenue_usd', display: 'Total Event Revenue ($)',
      desc: 'Sum of event ticket revenue in window', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'event', window: 'last_12_months', deps: null,
      sql: `SELECT CAST(SUM(e.order_total) AS DECIMAL(18,4)) AS value
FROM serving.event_detail e
WHERE (@start_date IS NULL OR e.payment_date >= @start_date)
  AND (@end_date   IS NULL OR e.payment_date < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'events.attendance_count', display: 'Event Attendance',
      desc: 'Count of event attendance records in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'event', window: 'last_12_months', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM serving.event_detail e
WHERE (@start_date IS NULL OR e.payment_date >= @start_date)
  AND (@end_date   IS NULL OR e.payment_date < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'events.unique_attendees', display: 'Unique Attendees',
      desc: 'Distinct people attending events in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `SELECT CAST(COUNT_BIG(DISTINCT e.person_id) AS DECIMAL(18,4)) AS value
FROM serving.event_detail e
WHERE (@start_date IS NULL OR e.payment_date >= @start_date)
  AND (@end_date   IS NULL OR e.payment_date < DATEADD(DAY,1,@end_date))`
    },

    // ── SUBSCRIPTIONS ───────────────────────────────────────
    {
      key: 'subscriptions.active_subscriptions', display: 'Active Subscriptions',
      desc: 'Count of active subscriptions (Subbly only for accuracy)', type: 'snapshot', unit: 'count', fmt: 'integer',
      grain: 'subscription', window: 'as_of', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM serving.subscription_detail s
WHERE s.subscription_status = 'Active' AND s.source_system = 'subbly'`
    },
    {
      key: 'subscriptions.mrr_usd', display: 'MRR ($)',
      desc: 'Monthly recurring revenue (active Subbly subscriptions normalized to monthly)', type: 'derived', unit: 'usd', fmt: 'currency',
      grain: 'subscription', window: 'as_of', deps: null,
      sql: `SELECT CAST(SUM(
  CASE
    WHEN s.cadence = 'monthly' THEN s.amount
    WHEN s.cadence = 'quarterly' THEN s.amount / 3.0
    WHEN s.cadence = 'annual' THEN s.amount / 12.0
    WHEN s.cadence = 'weekly' THEN s.amount * 4.345
    ELSE s.amount
  END
) AS DECIMAL(18,4)) AS value
FROM serving.subscription_detail s
WHERE s.subscription_status = 'Active' AND s.source_system = 'subbly'`
    },
    {
      key: 'subscriptions.arr_usd', display: 'ARR ($)',
      desc: 'Annual recurring revenue (MRR x 12)', type: 'derived', unit: 'usd', fmt: 'currency',
      grain: 'subscription', window: 'as_of', deps: 'subscriptions.mrr_usd',
      sql: `SELECT CAST(12.0 * SUM(
  CASE
    WHEN s.cadence = 'monthly' THEN s.amount
    WHEN s.cadence = 'quarterly' THEN s.amount / 3.0
    WHEN s.cadence = 'annual' THEN s.amount / 12.0
    WHEN s.cadence = 'weekly' THEN s.amount * 4.345
    ELSE s.amount
  END
) AS DECIMAL(18,4)) AS value
FROM serving.subscription_detail s
WHERE s.subscription_status = 'Active' AND s.source_system = 'subbly'`
    },
    {
      key: 'subscriptions.lost_recurring_donors', display: 'Lost Recurring Donors',
      desc: 'Count of lost recurring donors', type: 'snapshot', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'as_of', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM serving.lost_recurring_donors`
    },
    {
      key: 'subscriptions.lost_mrr_usd', display: 'Lost MRR ($)',
      desc: 'Monthly revenue lost from recurring donors who stopped', type: 'snapshot', unit: 'usd', fmt: 'currency',
      grain: 'person', window: 'as_of', deps: null,
      sql: `SELECT CAST(SUM(l.monthly_amount) AS DECIMAL(18,4)) AS value
FROM serving.lost_recurring_donors l`
    },
    {
      key: 'subscriptions.lost_arr_usd', display: 'Lost ARR ($)',
      desc: 'Annual revenue lost from recurring donors who stopped', type: 'snapshot', unit: 'usd', fmt: 'currency',
      grain: 'person', window: 'as_of', deps: null,
      sql: `SELECT CAST(SUM(l.annual_value) AS DECIMAL(18,4)) AS value
FROM serving.lost_recurring_donors l`
    },

    // ── ENGAGEMENT ──────────────────────────────────────────
    {
      key: 'engagement.communication_count', display: 'Communications',
      desc: 'Count of communication records in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'communication', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM serving.communication_detail c
WHERE (@start_date IS NULL OR c.sent_at >= @start_date)
  AND (@end_date   IS NULL OR c.sent_at < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'engagement.unique_communicated_people', display: 'People Communicated With',
      desc: 'Distinct people with any communication in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(COUNT_BIG(DISTINCT c.person_id) AS DECIMAL(18,4)) AS value
FROM serving.communication_detail c
WHERE (@start_date IS NULL OR c.sent_at >= @start_date)
  AND (@end_date   IS NULL OR c.sent_at < DATEADD(DAY,1,@end_date))`
    },

    // ── TAGS ────────────────────────────────────────────────
    {
      key: 'tags.tagged_people', display: 'Tagged People',
      desc: 'Distinct people with any tag', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'all_time', deps: null,
      sql: `SELECT CAST(COUNT_BIG(DISTINCT t.person_id) AS DECIMAL(18,4)) AS value FROM serving.tag_detail t`
    },
    {
      key: 'tags.tag_count', display: 'Tag Assignments',
      desc: 'Total tag assignment rows', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'tag', window: 'all_time', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value FROM serving.tag_detail t`
    },

    // ── WEALTH ──────────────────────────────────────────────
    {
      key: 'wealth.screened_people', display: 'Wealth Screened People',
      desc: 'Count of people with wealth screening', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'all_time', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value FROM serving.wealth_screening`
    },
    {
      key: 'wealth.high_capacity_people', display: 'High Capacity People',
      desc: 'People with High or above capacity label', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'all_time', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM serving.wealth_screening w
WHERE w.capacity_label IN ('High ($25K-$100K)','Very High ($100K-$250K)','Ultra High ($250K+)')`
    },
    {
      key: 'wealth.avg_quality_score', display: 'Avg Wealth Quality Score',
      desc: 'Average wealth screening quality score', type: 'aggregate', unit: 'score', fmt: 'decimal',
      grain: 'person', window: 'all_time', deps: null,
      sql: `SELECT CAST(AVG(CAST(w.quality_score AS DECIMAL(18,4))) AS DECIMAL(18,4)) AS value FROM serving.wealth_screening w`
    },
    {
      key: 'wealth.total_untapped_capacity_usd', display: 'Total Untapped Capacity ($)',
      desc: 'Sum of (annual capacity - annualized giving) for undertapped donors', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'person', window: 'all_time', deps: null,
      sql: `SELECT CAST(SUM(
  w.giving_capacity - ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH, ds.first_gift_date, GETDATE()) / 12.0), 0)
) AS DECIMAL(18,4)) AS value
FROM serving.wealth_screening w
JOIN serving.donor_summary ds ON ds.person_id = w.person_id
WHERE w.giving_capacity > ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH, ds.first_gift_date, GETDATE()) / 12.0), 0)`
    },

    // ── LIFECYCLE ───────────────────────────────────────────
    {
      key: 'lifecycle.active_donors', display: 'Active Donors',
      desc: 'Count of donors in active lifecycle stage', type: 'snapshot', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'as_of', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value FROM serving.donor_summary ds WHERE ds.lifecycle_stage = 'active'`
    },
    {
      key: 'lifecycle.cooling_donors', display: 'Cooling Donors',
      desc: 'Count of donors in cooling stage', type: 'snapshot', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'as_of', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value FROM serving.donor_summary ds WHERE ds.lifecycle_stage = 'cooling'`
    },
    {
      key: 'lifecycle.lapsed_donors', display: 'Lapsed Donors',
      desc: 'Count of donors in lapsed stage', type: 'snapshot', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'as_of', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value FROM serving.donor_summary ds WHERE ds.lifecycle_stage = 'lapsed'`
    },
    {
      key: 'lifecycle.lost_donors', display: 'Lost Donors',
      desc: 'Count of donors in lost stage', type: 'snapshot', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'as_of', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value FROM serving.donor_summary ds WHERE ds.lifecycle_stage = 'lost'`
    },
    {
      key: 'lifecycle.donor_count_total', display: 'Total Donors',
      desc: 'Total people in donor summary', type: 'snapshot', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'as_of', deps: null,
      sql: `SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value FROM serving.donor_summary`
    },

    // ── CROSS-STREAM ────────────────────────────────────────
    {
      key: 'crossstream.giver_buyer_overlap', display: 'Giver+Buyer Overlap',
      desc: 'People who both donated and ordered in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH donors AS (
  SELECT DISTINCT d.person_id FROM serving.donation_detail d
  WHERE (@start_date IS NULL OR d.donated_at >= @start_date)
    AND (@end_date   IS NULL OR d.donated_at < DATEADD(DAY,1,@end_date))
),
buyers AS (
  SELECT DISTINCT o.person_id FROM serving.order_detail o
  WHERE (@start_date IS NULL OR o.order_date >= @start_date)
    AND (@end_date   IS NULL OR o.order_date < DATEADD(DAY,1,@end_date))
)
SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM donors d INNER JOIN buyers b ON b.person_id = d.person_id`
    },
    {
      key: 'crossstream.total_revenue_usd', display: 'Total Revenue ($)',
      desc: 'Donations + Orders + Events combined', type: 'derived', unit: 'usd', fmt: 'currency',
      grain: 'time', window: 'last_30_days', deps: 'giving.total_donations_usd,commerce.total_order_revenue_usd,events.total_event_revenue_usd',
      sql: `WITH d AS (
  SELECT SUM(amount) AS amt FROM serving.donation_detail
  WHERE (@start_date IS NULL OR donated_at >= @start_date)
    AND (@end_date   IS NULL OR donated_at < DATEADD(DAY,1,@end_date))
),
o AS (
  SELECT SUM(total_amount) AS amt FROM serving.order_detail
  WHERE (@start_date IS NULL OR order_date >= @start_date)
    AND (@end_date   IS NULL OR order_date < DATEADD(DAY,1,@end_date))
),
e AS (
  SELECT SUM(order_total) AS amt FROM serving.event_detail
  WHERE (@start_date IS NULL OR payment_date >= @start_date)
    AND (@end_date   IS NULL OR payment_date < DATEADD(DAY,1,@end_date))
)
SELECT CAST(COALESCE((SELECT amt FROM d),0) + COALESCE((SELECT amt FROM o),0) + COALESCE((SELECT amt FROM e),0) AS DECIMAL(18,4)) AS value`
    },

    // ── STRIPE ──────────────────────────────────────────────
    {
      key: 'stripe.total_charges_usd', display: 'Stripe Charges ($)',
      desc: 'Total Stripe charge amount in window', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'payment', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(SUM(s.amount) AS DECIMAL(18,4)) AS value
FROM serving.stripe_charge_detail s
WHERE s.status = 'succeeded'
  AND (@start_date IS NULL OR s.created_at >= @start_date)
  AND (@end_date   IS NULL OR s.created_at < DATEADD(DAY,1,@end_date))`
    },
    {
      key: 'stripe.refunded_volume_usd', display: 'Stripe Refunds ($)',
      desc: 'Total Stripe refunded amount in window', type: 'aggregate', unit: 'usd', fmt: 'currency',
      grain: 'payment', window: 'last_30_days', deps: null,
      sql: `SELECT CAST(SUM(s.amount_refunded) AS DECIMAL(18,4)) AS value
FROM serving.stripe_charge_detail s
WHERE (@start_date IS NULL OR s.created_at >= @start_date)
  AND (@end_date   IS NULL OR s.created_at < DATEADD(DAY,1,@end_date))`
    },

    // ── ACQUISITION ─────────────────────────────────────────
    {
      key: 'acquisition.first_time_buyer_count', display: 'First-time Buyers',
      desc: 'People whose first order occurs in window', type: 'aggregate', unit: 'count', fmt: 'integer',
      grain: 'person', window: 'last_12_months', deps: null,
      sql: `WITH first_order AS (
  SELECT person_id, MIN(order_date) AS first_order_date
  FROM serving.order_detail
  GROUP BY person_id
)
SELECT CAST(COUNT_BIG(1) AS DECIMAL(18,4)) AS value
FROM first_order fo
WHERE (@start_date IS NULL OR fo.first_order_date >= @start_date)
  AND (@end_date   IS NULL OR fo.first_order_date < DATEADD(DAY,1,@end_date))`
    },
  ];

  for (const m of metrics) {
    const req = pool.request();
    req.input('key', sql.NVarChar, m.key);
    req.input('display', sql.NVarChar, m.display);
    req.input('desc', sql.NVarChar, m.desc || null);
    req.input('type', sql.NVarChar, m.type);
    req.input('unit', sql.NVarChar, m.unit);
    req.input('fmt', sql.NVarChar, m.fmt || null);
    req.input('grain', sql.NVarChar, m.grain);
    req.input('window', sql.NVarChar, m.window || null);
    req.input('sqlExpr', sql.NVarChar, m.sql);
    req.input('deps', sql.NVarChar, m.deps || null);
    await req.query(`
      IF NOT EXISTS (SELECT 1 FROM intel.metric_definition WHERE metric_key = @key)
        INSERT INTO intel.metric_definition
          (metric_key, display_name, description, metric_type, unit, format_hint, grain, default_time_window, sql_expression, depends_on_metric_keys)
        VALUES (@key, @display, @desc, @type, @unit, @fmt, @grain, @window, @sqlExpr, @deps)
    `);
    console.log(`  metric: ${m.key}`);
  }

  // ── 3. SYNONYMS ──────────────────────────────────────────────────────────
  console.log('\n=== Seeding synonyms ===');
  const synonyms = [
    // Giving
    ['giving.total_donations_usd', ['total giving', 'total donations', 'how much was donated', 'donation total', 'giving total', 'sum of donations', 'revenue from donations']],
    ['giving.donation_count', ['number of donations', 'how many donations', 'donation count', 'gift count', 'number of gifts']],
    ['giving.avg_donation_usd', ['average gift', 'average donation', 'avg donation', 'avg gift', 'typical gift size', 'mean gift']],
    ['giving.unique_donors', ['how many donors', 'unique donors', 'number of donors', 'donor count', 'distinct donors', 'how many people gave']],
    ['giving.new_donors', ['new donors', 'first-time donors', 'first time donors', 'new givers', 'donor acquisition']],
    ['giving.reactivated_donors', ['reactivated donors', 'returning donors', 'donors who came back', 'win-back donors']],
    ['giving.major_donor_count', ['major donors', 'big donors', 'large donors', 'donors over 1000', 'high-value donors']],
    ['giving.donor_retention_rate', ['donor retention', 'retention rate', 'how many donors stayed', 'donor loyalty', 'donor renewal rate']],
    ['giving.churned_donors', ['churned donors', 'lost donors recently', 'donors who stopped', 'donor attrition', 'donors who left']],
    ['giving.lifetime_giving_usd', ['lifetime giving', 'all-time giving', 'total ever donated', 'cumulative donations']],
    ['giving.median_gift_usd', ['median gift', 'median donation', 'middle gift amount', 'typical donation']],
    ['giving.gift_frequency_per_donor', ['gift frequency', 'how often donors give', 'gifts per donor', 'giving frequency']],
    ['giving.days_since_last_gift_avg', ['days since last gift', 'recency', 'how long since donors gave', 'last gift timing']],
    ['giving.top10_donor_concentration_pct', ['donor concentration', 'top donor dependence', 'revenue concentration', 'how much top donors give']],

    // Commerce
    ['commerce.total_order_revenue_usd', ['order revenue', 'commerce revenue', 'product sales', 'merchandise sales', 'total orders']],
    ['commerce.order_count', ['number of orders', 'order count', 'how many orders']],
    ['commerce.avg_order_value_usd', ['average order value', 'avg order', 'AOV', 'typical order']],
    ['commerce.unique_buyers', ['unique buyers', 'how many customers', 'customer count', 'distinct buyers']],
    ['commerce.woo_revenue_usd', ['woo revenue', 'woocommerce sales', 'woo sales', 'online store revenue']],

    // Events
    ['events.total_event_revenue_usd', ['event revenue', 'ticket revenue', 'event income', 'events total']],
    ['events.attendance_count', ['event attendance', 'how many attended', 'ticket count', 'attendance count']],
    ['events.unique_attendees', ['unique attendees', 'how many people attended', 'distinct attendees']],

    // Subscriptions
    ['subscriptions.active_subscriptions', ['active subscriptions', 'current subscriptions', 'active subs', 'how many subscribers']],
    ['subscriptions.mrr_usd', ['MRR', 'monthly recurring revenue', 'recurring revenue', 'monthly revenue']],
    ['subscriptions.arr_usd', ['ARR', 'annual recurring revenue', 'yearly recurring']],
    ['subscriptions.lost_recurring_donors', ['lost recurring donors', 'canceled recurring', 'stopped recurring', 'recurring churn']],
    ['subscriptions.lost_mrr_usd', ['lost MRR', 'churn MRR', 'lost monthly revenue']],

    // Engagement
    ['engagement.communication_count', ['communications', 'messages sent', 'outreach count', 'touchpoints']],

    // Wealth
    ['wealth.screened_people', ['wealth screened', 'screened contacts', 'people with screening']],
    ['wealth.high_capacity_people', ['high capacity donors', 'wealthy donors', 'high net worth']],
    ['wealth.total_untapped_capacity_usd', ['untapped capacity', 'giving potential', 'capacity gap total', 'unrealized giving']],

    // Lifecycle
    ['lifecycle.active_donors', ['active donors', 'current donors', 'engaged donors']],
    ['lifecycle.cooling_donors', ['cooling donors', 'at-risk donors', 'donors slowing down']],
    ['lifecycle.lapsed_donors', ['lapsed donors', 'inactive donors', 'dormant donors']],
    ['lifecycle.lost_donors', ['lost donors', 'gone donors', 'donors we lost']],

    // Cross-stream
    ['crossstream.giver_buyer_overlap', ['giver-buyer overlap', 'donors who buy', 'buyers who donate', 'crossover donors']],
    ['crossstream.total_revenue_usd', ['total revenue', 'all revenue', 'combined revenue', 'overall revenue']],

    // Stripe
    ['stripe.total_charges_usd', ['stripe charges', 'stripe volume', 'stripe revenue', 'card charges']],
    ['stripe.refunded_volume_usd', ['stripe refunds', 'refund volume', 'refunded amount']],
  ];

  for (const [metricKey, syns] of synonyms) {
    for (const syn of syns) {
      const req = pool.request();
      req.input('key', sql.NVarChar, metricKey);
      req.input('syn', sql.NVarChar, syn);
      await req.query(`
        IF NOT EXISTS (SELECT 1 FROM intel.metric_synonym WHERE metric_key = @key AND synonym = @syn)
          INSERT INTO intel.metric_synonym (metric_key, synonym, weight) VALUES (@key, @syn, 100)
      `);
    }
    console.log(`  synonyms: ${metricKey} (${syns.length})`);
  }

  // ── 4. ALLOWLIST (metric → dimension) ────────────────────────────────────
  console.log('\n=== Seeding metric-dimension allowlist ===');

  // Define which dimensions each metric category can use
  const timeDims = ['time.date', 'time.month', 'time.year'];
  const personDims = ['person.lifecycle_stage', 'person.state', 'person.city', 'person.source_system'];
  const givingDims = ['giving.fund', 'giving.appeal', 'giving.payment_method', 'giving.source_system'];
  const commerceDims = ['commerce.order_status', 'commerce.order_month'];
  const eventDims = ['events.event_name', 'events.ticket_type', 'events.event_year'];
  const subDims = ['subscriptions.product_name', 'subscriptions.status', 'subscriptions.cadence', 'subscriptions.source_system'];
  const tagDims = ['tags.tag_group', 'tags.tag_value'];
  const wealthDims = ['wealth.capacity_label'];
  const commDims = ['communications.channel', 'communications.direction'];

  const allowlistRules = {
    // Giving metrics get time + person + giving dims
    'giving.total_donations_usd': [...timeDims, ...personDims, ...givingDims],
    'giving.donation_count': [...timeDims, ...personDims, ...givingDims],
    'giving.avg_donation_usd': [...timeDims, ...personDims, ...givingDims],
    'giving.unique_donors': [...timeDims, ...personDims, ...givingDims],
    'giving.new_donors': [...timeDims, ...personDims],
    'giving.reactivated_donors': [...timeDims, ...personDims],
    'giving.major_donor_count': [...timeDims, ...personDims],
    'giving.donor_retention_rate': [...timeDims],
    'giving.churned_donors': [...timeDims],
    'giving.lifetime_giving_usd': [...personDims, ...givingDims],
    'giving.median_gift_usd': [...timeDims, ...givingDims],
    'giving.gift_frequency_per_donor': [...timeDims],
    'giving.days_since_last_gift_avg': ['person.lifecycle_stage'],
    'giving.payment_total_usd': [...timeDims, ...personDims],
    'giving.top10_donor_concentration_pct': [...timeDims],

    // Commerce metrics
    'commerce.total_order_revenue_usd': [...timeDims, ...personDims, ...commerceDims],
    'commerce.order_count': [...timeDims, ...personDims, ...commerceDims],
    'commerce.avg_order_value_usd': [...timeDims, ...commerceDims],
    'commerce.unique_buyers': [...timeDims, ...personDims],
    'commerce.woo_revenue_usd': [...timeDims],

    // Event metrics
    'events.total_event_revenue_usd': [...timeDims, ...eventDims],
    'events.attendance_count': [...timeDims, ...eventDims],
    'events.unique_attendees': [...timeDims, ...eventDims],

    // Subscription metrics
    'subscriptions.active_subscriptions': [...subDims],
    'subscriptions.mrr_usd': [...subDims],
    'subscriptions.arr_usd': [...subDims],
    'subscriptions.lost_recurring_donors': [],
    'subscriptions.lost_mrr_usd': [],
    'subscriptions.lost_arr_usd': [],

    // Engagement
    'engagement.communication_count': [...timeDims, ...commDims],
    'engagement.unique_communicated_people': [...timeDims, ...commDims],

    // Tags
    'tags.tagged_people': [...tagDims],
    'tags.tag_count': [...tagDims],

    // Wealth
    'wealth.screened_people': [...wealthDims],
    'wealth.high_capacity_people': [...wealthDims],
    'wealth.avg_quality_score': [...wealthDims],
    'wealth.total_untapped_capacity_usd': [...wealthDims],

    // Lifecycle
    'lifecycle.active_donors': ['person.state', 'person.city'],
    'lifecycle.cooling_donors': ['person.state', 'person.city'],
    'lifecycle.lapsed_donors': ['person.state', 'person.city'],
    'lifecycle.lost_donors': ['person.state', 'person.city'],
    'lifecycle.donor_count_total': ['person.lifecycle_stage', 'person.state'],

    // Cross-stream
    'crossstream.giver_buyer_overlap': [...timeDims],
    'crossstream.total_revenue_usd': [...timeDims],

    // Stripe
    'stripe.total_charges_usd': [...timeDims],
    'stripe.refunded_volume_usd': [...timeDims],

    // Acquisition
    'acquisition.first_time_buyer_count': [...timeDims],
  };

  let allowlistCount = 0;
  for (const [metricKey, dimKeys] of Object.entries(allowlistRules)) {
    for (const dimKey of dimKeys) {
      const req = pool.request();
      req.input('mk', sql.NVarChar, metricKey);
      req.input('dk', sql.NVarChar, dimKey);
      await req.query(`
        IF NOT EXISTS (SELECT 1 FROM intel.metric_dimension_allowlist WHERE metric_key = @mk AND dimension_key = @dk)
          INSERT INTO intel.metric_dimension_allowlist (metric_key, dimension_key) VALUES (@mk, @dk)
      `);
      allowlistCount++;
    }
  }
  console.log(`  allowlist: ${allowlistCount} entries`);

  // ── 5. SEMANTIC POLICIES ─────────────────────────────────────────────────
  console.log('\n=== Seeding semantic policies ===');
  const policies = [
    {
      key: 'giving_capacity_annual',
      desc: 'giving_capacity in wealth_screening is an ANNUAL estimate. Always compare against annualized giving (total_given / years_active), never raw lifetime total_given.',
      json: JSON.stringify({
        rule: 'capacity_comparison',
        affected_tables: ['serving.wealth_screening', 'serving.donor_summary'],
        instruction: 'When comparing giving_capacity to actual giving, use: total_given / NULLIF(CEILING(DATEDIFF(MONTH, first_gift_date, GETDATE()) / 12.0), 0)',
        severity: 'critical'
      })
    },
    {
      key: 'subbly_subscriptions_only',
      desc: 'Keap subscriptions are STALE. Always filter WHERE source_system = \'subbly\' for active subscription counts/MRR.',
      json: JSON.stringify({
        rule: 'subscription_source_filter',
        affected_tables: ['serving.subscription_detail'],
        instruction: "Always add WHERE source_system = 'subbly' when querying active subscriptions",
        severity: 'critical'
      })
    },
    {
      key: 'exclude_unknown_display_name',
      desc: 'Always add WHERE display_name <> \'Unknown\' on top-N or donor ranking queries.',
      json: JSON.stringify({
        rule: 'display_name_filter',
        affected_tables: ['serving.donor_summary', 'serving.donation_detail', 'serving.order_detail'],
        instruction: "Add WHERE display_name <> 'Unknown' on any top-N or donor ranking query",
        severity: 'high'
      })
    },
    {
      key: 'never_expose_person_id',
      desc: 'Never include person_id, donation_id, or any _id column in SELECT output.',
      json: JSON.stringify({
        rule: 'id_column_exclusion',
        instruction: 'Never include person_id, donation_id, or any _id column in SELECT output — these are internal keys',
        severity: 'critical'
      })
    },
    {
      key: 'use_top_not_limit',
      desc: 'Azure SQL uses TOP (N), not LIMIT. Always use TOP (N) with parentheses.',
      json: JSON.stringify({
        rule: 'sql_dialect',
        instruction: 'Use TOP (N) not LIMIT for Azure SQL. Always include parentheses: TOP (10)',
        severity: 'high'
      })
    },
  ];

  for (const p of policies) {
    const req = pool.request();
    req.input('key', sql.NVarChar, p.key);
    req.input('desc', sql.NVarChar, p.desc);
    req.input('json', sql.NVarChar, p.json);
    await req.query(`
      IF NOT EXISTS (SELECT 1 FROM intel.semantic_policy WHERE policy_key = @key)
        INSERT INTO intel.semantic_policy (policy_key, description, policy_json)
        VALUES (@key, @desc, @json)
    `);
    console.log(`  policy: ${p.key}`);
  }

  console.log('\n=== Catalog seeding complete ===');
  console.log(`  Dimensions: ${dimensions.length}`);
  console.log(`  Metrics: ${metrics.length}`);
  console.log(`  Synonym groups: ${synonyms.length}`);
  console.log(`  Allowlist entries: ${allowlistCount}`);
  console.log(`  Policies: ${policies.length}`);

  await pool.close();
}

main().catch(err => { console.error(err); process.exit(1); });
