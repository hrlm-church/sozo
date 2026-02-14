/**
 * Create Gold Layer Views — sozov2
 *
 * Creates gold schema + 8 views for constituent intelligence.
 * Requires: silver tables + silver.identity_map populated.
 *
 * Usage: node scripts/pipeline/create_gold_views.js
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

const VIEWS = [
  {
    name: 'gold.person',
    sql: `CREATE VIEW gold.person AS
    SELECT
      im.master_id,
      c.first_name, c.last_name,
      COALESCE(c.first_name + ' ' + c.last_name, c.first_name, c.last_name, 'Unknown') AS display_name,
      c.email_primary, c.phone_primary,
      c.address_line1, c.city, c.state, c.postal_code, c.country,
      c.date_of_birth, c.gender, c.spouse_name, c.household_name, c.organization_name,
      c.source_system AS primary_source, c.created_at,
      (SELECT COUNT(DISTINCT im2.source_system) FROM silver.identity_map im2 WHERE im2.master_id = im.master_id) AS source_count,
      (SELECT STRING_AGG(ss, ', ') FROM (SELECT DISTINCT im2.source_system AS ss FROM silver.identity_map im2 WHERE im2.master_id = im.master_id) x) AS source_systems
    FROM silver.identity_map im
    JOIN silver.contact c ON c.contact_id = im.contact_id
    WHERE im.is_primary = 1`
  },
  {
    name: 'gold.person_giving',
    sql: `CREATE VIEW gold.person_giving AS
    SELECT
      p.master_id, p.display_name, p.email_primary, p.primary_source, p.source_systems,
      ISNULL(g.donation_count, 0) AS donation_count,
      ISNULL(g.total_given, 0) AS total_given,
      g.avg_gift, g.largest_gift, g.first_gift_date, g.last_gift_date,
      DATEDIFF(DAY, g.last_gift_date, GETDATE()) AS days_since_last,
      g.fund_count, g.active_months
    FROM gold.person p
    LEFT JOIN (
      SELECT im.master_id,
        COUNT(*) AS donation_count, SUM(d.amount) AS total_given,
        AVG(d.amount) AS avg_gift, MAX(d.amount) AS largest_gift,
        MIN(d.donated_at) AS first_gift_date, MAX(d.donated_at) AS last_gift_date,
        COUNT(DISTINCT d.fund_code) AS fund_count,
        COUNT(DISTINCT FORMAT(d.donated_at, 'yyyy-MM')) AS active_months
      FROM silver.donation d
      JOIN silver.identity_map im ON im.source_system = d.source_system AND im.source_id = d.contact_source_id
      GROUP BY im.master_id
    ) g ON g.master_id = p.master_id`
  },
  {
    name: 'gold.person_commerce',
    sql: `CREATE VIEW gold.person_commerce AS
    SELECT p.master_id, p.display_name, p.email_primary,
      ISNULL(o.order_count, 0) AS order_count,
      ISNULL(o.total_spent, 0) AS total_spent,
      o.first_order_date, o.last_order_date,
      ISNULL(inv.invoice_count, 0) AS invoice_count,
      ISNULL(inv.total_invoiced, 0) AS total_invoiced,
      ISNULL(inv.total_paid, 0) AS total_paid,
      ISNULL(pay.payment_count, 0) AS payment_count,
      ISNULL(pay.total_payments, 0) AS total_payments
    FROM gold.person p
    LEFT JOIN (
      SELECT im.master_id,
        COUNT(DISTINCT o.keap_id) AS order_count,
        SUM(oi.price_per_unit * oi.qty) AS total_spent,
        MIN(o.created_at) AS first_order_date, MAX(o.created_at) AS last_order_date
      FROM silver.[order] o
      JOIN silver.identity_map im ON im.source_system = 'keap' AND TRY_CAST(im.source_id AS INT) = o.contact_keap_id
      LEFT JOIN silver.order_item oi ON oi.order_keap_id = o.keap_id
      GROUP BY im.master_id
    ) o ON o.master_id = p.master_id
    LEFT JOIN (
      SELECT im.master_id,
        COUNT(*) AS invoice_count, SUM(i.total) AS total_invoiced, SUM(i.total_paid) AS total_paid
      FROM silver.invoice i
      JOIN silver.identity_map im ON im.source_system = 'keap' AND TRY_CAST(im.source_id AS INT) = i.contact_keap_id
      GROUP BY im.master_id
    ) inv ON inv.master_id = p.master_id
    LEFT JOIN (
      SELECT im.master_id,
        COUNT(*) AS payment_count, SUM(py.amount) AS total_payments
      FROM silver.payment py
      JOIN silver.identity_map im ON im.source_system = 'keap' AND TRY_CAST(im.source_id AS INT) = py.contact_keap_id
      GROUP BY im.master_id
    ) pay ON pay.master_id = p.master_id`
  },
  {
    name: 'gold.person_engagement',
    sql: `CREATE VIEW gold.person_engagement AS
    SELECT p.master_id, p.display_name,
      ISNULL(n.note_count, 0) AS note_count, n.last_note_date,
      ISNULL(cm.comm_count, 0) AS comm_count, cm.last_comm_date,
      ISNULL(n.note_count, 0) + ISNULL(cm.comm_count, 0) AS total_interactions
    FROM gold.person p
    LEFT JOIN (
      SELECT im.master_id, COUNT(*) AS note_count, MAX(nt.created_at) AS last_note_date
      FROM silver.note nt
      JOIN silver.identity_map im ON im.source_system = nt.source_system AND im.source_id = nt.contact_source_id
      GROUP BY im.master_id
    ) n ON n.master_id = p.master_id
    LEFT JOIN (
      SELECT im.master_id, COUNT(*) AS comm_count, MAX(cm.comm_date) AS last_comm_date
      FROM silver.communication cm
      JOIN silver.identity_map im ON im.source_system = cm.source_system AND im.source_id = cm.contact_source_id
      GROUP BY im.master_id
    ) cm ON cm.master_id = p.master_id`
  },
  {
    name: 'gold.constituent_360',
    sql: `CREATE VIEW gold.constituent_360 AS
    SELECT p.master_id, p.display_name, p.first_name, p.last_name,
      p.email_primary, p.phone_primary, p.city, p.state, p.postal_code,
      p.date_of_birth, p.gender, p.spouse_name, p.household_name, p.organization_name,
      p.primary_source, p.source_count, p.source_systems,
      ISNULL(g.donation_count, 0) AS donation_count,
      ISNULL(g.total_given, 0) AS total_given,
      g.avg_gift, g.largest_gift, g.first_gift_date, g.last_gift_date,
      DATEDIFF(DAY, g.last_gift_date, GETDATE()) AS days_since_last_gift,
      ISNULL(c.order_count, 0) AS order_count,
      ISNULL(c.total_spent, 0) AS total_spent,
      c.last_order_date,
      ISNULL(c.total_payments, 0) AS total_payments,
      ISNULL(e.note_count, 0) AS note_count,
      ISNULL(e.comm_count, 0) AS comm_count,
      ISNULL(e.total_interactions, 0) AS total_interactions,
      ISNULL(g.total_given, 0) + ISNULL(c.total_spent, 0) AS lifetime_value
    FROM gold.person p
    LEFT JOIN gold.person_giving g ON g.master_id = p.master_id
    LEFT JOIN gold.person_commerce c ON c.master_id = p.master_id
    LEFT JOIN gold.person_engagement e ON e.master_id = p.master_id`
  },
  {
    name: 'gold.monthly_trends',
    sql: `CREATE VIEW gold.monthly_trends AS
    SELECT FORMAT(d.donated_at, 'yyyy-MM') AS month,
      YEAR(d.donated_at) AS yr, MONTH(d.donated_at) AS mo,
      COUNT(*) AS donation_count,
      COUNT(DISTINCT im.master_id) AS unique_donors,
      SUM(d.amount) AS total_amount, AVG(d.amount) AS avg_gift, MAX(d.amount) AS max_gift,
      d.source_system
    FROM silver.donation d
    LEFT JOIN silver.identity_map im ON im.source_system = d.source_system AND im.source_id = d.contact_source_id
    WHERE d.donated_at IS NOT NULL
    GROUP BY FORMAT(d.donated_at, 'yyyy-MM'), YEAR(d.donated_at), MONTH(d.donated_at), d.source_system`
  },
  {
    name: 'gold.product_summary',
    sql: `CREATE VIEW gold.product_summary AS
    SELECT p.keap_id AS product_id, p.name AS product_name, p.price AS list_price, p.sku, p.status,
      ISNULL(s.times_ordered, 0) AS times_ordered,
      ISNULL(s.total_qty, 0) AS total_qty_sold,
      ISNULL(s.total_revenue, 0) AS total_revenue,
      s.first_ordered, s.last_ordered
    FROM silver.product p
    LEFT JOIN (
      SELECT oi.product_keap_id,
        COUNT(*) AS times_ordered, SUM(oi.qty) AS total_qty,
        SUM(oi.price_per_unit * oi.qty) AS total_revenue,
        MIN(oi.created_at) AS first_ordered, MAX(oi.created_at) AS last_ordered
      FROM silver.order_item oi WHERE oi.product_keap_id IS NOT NULL
      GROUP BY oi.product_keap_id
    ) s ON s.product_keap_id = p.keap_id`
  },
  {
    name: 'gold.subscription_health',
    sql: `CREATE VIEW gold.subscription_health AS
    SELECT sub.subscription_id, p.master_id, p.display_name, p.email_primary,
      sub.billing_amount, sub.billing_cycle, sub.frequency, sub.status,
      sub.start_date, sub.end_date, sub.next_bill_date, sub.reason_stopped,
      sub.product_id, pr.name AS product_name,
      CASE
        WHEN sub.status = 'Active' AND sub.next_bill_date < GETDATE() THEN 'at_risk'
        WHEN sub.status = 'Active' THEN 'active'
        WHEN sub.status = 'Inactive' AND sub.reason_stopped IS NOT NULL THEN 'churned'
        ELSE LOWER(ISNULL(sub.status, 'unknown'))
      END AS health_status
    FROM silver.subscription sub
    JOIN silver.identity_map im ON im.source_system = 'keap' AND TRY_CAST(im.source_id AS INT) = sub.contact_keap_id
    JOIN gold.person p ON p.master_id = im.master_id
    LEFT JOIN silver.product pr ON pr.keap_id = sub.product_id`
  },
];

async function main() {
  loadEnv();
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST, database: 'sozov2',
    user: process.env.SOZO_SQL_USER, password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 60000
  });

  // Create gold schema
  console.log('Creating gold schema...');
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
      EXEC('CREATE SCHEMA gold')
  `);

  // Create each view
  for (const v of VIEWS) {
    console.log(`  ${v.name}...`);
    try {
      await pool.request().query(`IF OBJECT_ID('${v.name}', 'V') IS NOT NULL DROP VIEW ${v.name}`);
      await pool.request().query(v.sql);
      console.log(`    OK`);
    } catch (err) {
      console.error(`    FAIL: ${err.message.substring(0, 200)}`);
    }
  }

  // Quick tests
  console.log('\nTesting gold views...');
  const tests = [
    ['Person count', 'SELECT COUNT(*) AS n FROM gold.person'],
    ['Top 5 donors', 'SELECT TOP 5 display_name, total_given, donation_count FROM gold.person_giving WHERE total_given > 0 ORDER BY total_given DESC'],
    ['Top 5 products by revenue', 'SELECT TOP 5 product_name, total_revenue, total_qty_sold FROM gold.product_summary ORDER BY total_revenue DESC'],
    ['Monthly trends (recent)', 'SELECT TOP 5 month, source_system, donation_count, unique_donors, total_amount FROM gold.monthly_trends ORDER BY month DESC'],
    ['Subscription health', 'SELECT health_status, COUNT(*) AS cnt, SUM(billing_amount) AS mrr FROM gold.subscription_health GROUP BY health_status'],
  ];

  for (const [label, q] of tests) {
    try {
      const r = await pool.request().query(q);
      console.log(`\n  ${label}:`);
      for (const row of r.recordset) {
        const vals = Object.entries(row).map(([k, v]) => `${k}=${v}`).join(', ');
        console.log(`    ${vals}`);
      }
    } catch (e) {
      console.error(`  ${label} FAIL: ${e.message.substring(0, 200)}`);
    }
  }

  await pool.close();
  console.log('\nDone.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
