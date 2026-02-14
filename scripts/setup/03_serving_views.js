#!/usr/bin/env node
/**
 * Create denormalized serving views so the LLM never needs to write JOINs.
 * These are SQL VIEWs (not materialized) — zero storage cost, same perf as manual JOINs.
 */
const path = require('path'), fs = require('fs'), sql = require('mssql');

function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}
loadEnv();

const VIEWS = [
  {
    name: 'serving.donation_detail',
    description: 'Every donation with person name, fund, appeal — no JOINs needed',
    sql: `CREATE OR ALTER VIEW serving.donation_detail AS
SELECT
  d.id              AS donation_id,
  d.person_id,
  p.display_name,
  p.first_name,
  p.last_name,
  p.email,
  p.household_id,
  p.household_name,
  p.lifecycle_stage,
  d.amount,
  d.currency,
  d.donated_at,
  d.payment_method,
  d.fund,
  d.appeal,
  d.designation,
  d.source_ref,
  s.display_name    AS source_system,
  FORMAT(d.donated_at, 'yyyy-MM') AS donation_month,
  YEAR(d.donated_at) AS donation_year
FROM giving.donation d
JOIN serving.person_360 p ON p.person_id = d.person_id
LEFT JOIN meta.source_system s ON s.source_id = d.source_id`
  },
  {
    name: 'serving.order_detail',
    description: 'Every order with person name — no JOINs needed',
    sql: `CREATE OR ALTER VIEW serving.order_detail AS
SELECT
  o.id              AS order_id,
  o.person_id,
  p.display_name,
  p.first_name,
  p.last_name,
  p.email,
  p.household_id,
  o.order_number,
  o.total_amount,
  o.order_date,
  o.status          AS order_status,
  o.source_ref,
  s.display_name    AS source_system,
  FORMAT(o.order_date, 'yyyy-MM') AS order_month,
  YEAR(o.order_date) AS order_year
FROM commerce.[order] o
JOIN serving.person_360 p ON p.person_id = o.person_id
LEFT JOIN meta.source_system s ON s.source_id = o.source_id`
  },
  {
    name: 'serving.subscription_detail',
    description: 'Every subscription with person name — no JOINs needed',
    sql: `CREATE OR ALTER VIEW serving.subscription_detail AS
SELECT
  sub.id            AS subscription_id,
  sub.person_id,
  p.display_name,
  p.first_name,
  p.last_name,
  p.email,
  sub.product_name,
  sub.amount,
  sub.cadence,
  sub.status        AS subscription_status,
  sub.start_date,
  sub.next_renewal,
  sub.is_gift,
  sub.source_ref,
  s.display_name    AS source_system
FROM commerce.subscription sub
JOIN serving.person_360 p ON p.person_id = sub.person_id
LEFT JOIN meta.source_system s ON s.source_id = sub.source_id`
  },
  {
    name: 'serving.payment_detail',
    description: 'Every payment with person name — no JOINs needed',
    sql: `CREATE OR ALTER VIEW serving.payment_detail AS
SELECT
  pay.id            AS payment_id,
  pay.person_id,
  p.display_name,
  p.first_name,
  p.last_name,
  p.email,
  pay.amount,
  pay.payment_date,
  pay.method        AS payment_method,
  pay.status        AS payment_status,
  pay.donation_id,
  pay.order_id,
  pay.invoice_id,
  pay.source_ref,
  s.display_name    AS source_system,
  FORMAT(pay.payment_date, 'yyyy-MM') AS payment_month
FROM commerce.payment pay
JOIN serving.person_360 p ON p.person_id = pay.person_id
LEFT JOIN meta.source_system s ON s.source_id = pay.source_id`
  },
  {
    name: 'serving.invoice_detail',
    description: 'Every invoice with person name — no JOINs needed',
    sql: `CREATE OR ALTER VIEW serving.invoice_detail AS
SELECT
  inv.id            AS invoice_id,
  inv.person_id,
  p.display_name,
  p.first_name,
  p.last_name,
  p.email,
  inv.invoice_number,
  inv.total         AS invoice_total,
  inv.status        AS invoice_status,
  inv.issued_at,
  inv.paid_at,
  inv.source_ref,
  s.display_name    AS source_system,
  FORMAT(inv.issued_at, 'yyyy-MM') AS invoice_month
FROM commerce.invoice inv
JOIN serving.person_360 p ON p.person_id = inv.person_id
LEFT JOIN meta.source_system s ON s.source_id = inv.source_id`
  },
  {
    name: 'serving.tag_detail',
    description: 'Every tag with person name — no JOINs needed',
    sql: `CREATE OR ALTER VIEW serving.tag_detail AS
SELECT
  t.id              AS tag_id,
  t.person_id,
  p.display_name,
  p.first_name,
  p.last_name,
  t.tag_value,
  t.tag_group,
  t.applied_at,
  t.source_ref,
  s.display_name    AS source_system
FROM engagement.tag t
JOIN serving.person_360 p ON p.person_id = t.person_id
LEFT JOIN meta.source_system s ON s.source_id = t.source_id`
  },
  {
    name: 'serving.communication_detail',
    description: 'Every communication with person name — no JOINs needed',
    sql: `CREATE OR ALTER VIEW serving.communication_detail AS
SELECT
  c.id              AS communication_id,
  c.person_id,
  p.display_name,
  p.first_name,
  p.last_name,
  c.channel,
  c.direction,
  c.subject,
  c.sent_at,
  c.source_ref,
  s.display_name    AS source_system
FROM engagement.communication c
JOIN serving.person_360 p ON p.person_id = c.person_id
LEFT JOIN meta.source_system s ON s.source_id = c.source_id`
  },
  {
    name: 'serving.donor_summary',
    description: 'One row per donor with pre-ranked totals — instant top-N queries',
    sql: `CREATE OR ALTER VIEW serving.donor_summary AS
SELECT
  p.person_id,
  COALESCE(p.display_name, p.first_name + ' ' + p.last_name, 'Anonymous') AS display_name,
  p.first_name,
  p.last_name,
  p.email,
  p.household_id,
  p.household_name,
  p.lifecycle_stage,
  p.source_systems,
  COUNT(*)                          AS donation_count,
  SUM(d.amount)                     AS total_given,
  AVG(d.amount)                     AS avg_gift,
  MAX(d.amount)                     AS largest_gift,
  MIN(d.donated_at)                 AS first_gift_date,
  MAX(d.donated_at)                 AS last_gift_date,
  DATEDIFF(DAY, MAX(d.donated_at), GETDATE()) AS days_since_last,
  COUNT(DISTINCT d.fund)            AS fund_count,
  COUNT(DISTINCT FORMAT(d.donated_at,'yyyy-MM')) AS active_months
FROM giving.donation d
JOIN serving.person_360 p ON p.person_id = d.person_id
GROUP BY p.person_id, p.display_name, p.first_name, p.last_name,
         p.email, p.household_id, p.household_name,
         p.lifecycle_stage, p.source_systems`
  },
  {
    name: 'serving.donor_monthly',
    description: 'One row per donor-month with pre-aggregated amounts — instant monthly breakdowns',
    sql: `CREATE OR ALTER VIEW serving.donor_monthly AS
SELECT
  d.person_id,
  COALESCE(p.display_name, p.first_name + ' ' + p.last_name, 'Anonymous') AS display_name,
  FORMAT(d.donated_at, 'yyyy-MM')   AS donation_month,
  YEAR(d.donated_at)                AS donation_year,
  COUNT(*)                          AS gifts,
  SUM(d.amount)                     AS amount,
  MAX(d.fund)                       AS primary_fund,
  MAX(d.payment_method)             AS primary_method
FROM giving.donation d
JOIN serving.person_360 p ON p.person_id = d.person_id
GROUP BY d.person_id, p.display_name, p.first_name, p.last_name,
         FORMAT(d.donated_at, 'yyyy-MM'), YEAR(d.donated_at)`
  },
];

(async () => {
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: process.env.SOZO_SQL_DB,
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 30000,
    options: { encrypt: true, trustServerCertificate: false }
  });

  for (const view of VIEWS) {
    try {
      await pool.request().query(view.sql);
      console.log(`OK  ${view.name} — ${view.description}`);
    } catch (err) {
      console.error(`FAIL ${view.name}:`, err.message);
    }
  }

  // Verify all views
  const r = await pool.request().query(`
    SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS
    WHERE TABLE_SCHEMA = 'serving' ORDER BY TABLE_NAME
  `);
  console.log('\nServing views:', r.recordset.map(x => x.TABLE_NAME).join(', '));

  await pool.close();
  console.log('\nDone.');
})();
