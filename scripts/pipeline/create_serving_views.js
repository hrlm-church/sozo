/**
 * Create Serving Layer Views — sozov2
 *
 * Creates the `serving` schema and 11 views that the Sozo chat app expects.
 * Each view joins silver tables through silver.identity_map to provide
 * unified person_id (= master_id) and display_name.
 *
 * Usage: node scripts/pipeline/create_serving_views.js
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

// ═══════════════════════════════════════════════════════════════════════════
// VIEW DEFINITIONS
// ═══════════════════════════════════════════════════════════════════════════

const VIEWS = [
  // ── 1. PERSON 360 ────────────────────────────────────────────────────
  {
    name: 'serving.person_360',
    sql: `CREATE VIEW serving.person_360 AS
    SELECT
      im.master_id AS person_id,
      COALESCE(NULLIF(c.first_name,'NULL') + ' ' + NULLIF(c.last_name,'NULL'), NULLIF(c.first_name,'NULL'), NULLIF(c.last_name,'NULL'), 'Unknown') AS display_name,
      NULLIF(c.first_name,'NULL') AS first_name, NULLIF(c.last_name,'NULL') AS last_name, c.email_primary AS email, c.phone_primary AS phone,
      c.city, c.state, c.postal_code, c.country,
      c.date_of_birth, c.gender, c.spouse_name,
      c.household_name, c.organization_name,
      c.source_system AS primary_source,
      (SELECT COUNT(DISTINCT im2.source_system) FROM silver.identity_map im2 WHERE im2.master_id = im.master_id) AS source_count,
      (SELECT STRING_AGG(ss, ', ') FROM (SELECT DISTINCT im2.source_system AS ss FROM silver.identity_map im2 WHERE im2.master_id = im.master_id) x) AS source_systems,

      -- Giving aggregates (from deduplicated serving.donation_detail)
      ISNULL(g.donation_count, 0) AS donation_count,
      ISNULL(g.lifetime_giving, 0) AS lifetime_giving,
      g.avg_gift, g.largest_gift,
      g.first_gift_date, g.last_gift_date,
      DATEDIFF(DAY, g.last_gift_date, GETDATE()) AS recency_days,

      -- Commerce aggregates
      ISNULL(o.order_count, 0) AS order_count,
      ISNULL(o.total_spent, 0) AS total_spent,

      -- Tag count
      ISNULL(t.tag_count, 0) AS tag_count,

      -- Engagement
      ISNULL(n.note_count, 0) AS note_count,
      ISNULL(cm.comm_count, 0) AS comm_count,

      -- Lifecycle
      CASE
        WHEN g.donation_count IS NULL OR g.donation_count = 0 THEN 'prospect'
        WHEN DATEDIFF(DAY, g.last_gift_date, GETDATE()) <= 180 THEN 'active'
        WHEN DATEDIFF(DAY, g.last_gift_date, GETDATE()) <= 365 THEN 'cooling'
        WHEN DATEDIFF(DAY, g.last_gift_date, GETDATE()) <= 730 THEN 'lapsed'
        ELSE 'lost'
      END AS lifecycle_stage,

      c.created_at

    FROM silver.identity_map im
    JOIN silver.contact c ON c.contact_id = im.contact_id
    LEFT JOIN (
      SELECT person_id AS master_id,
        COUNT(*) AS donation_count, SUM(amount) AS lifetime_giving,
        AVG(amount) AS avg_gift, MAX(amount) AS largest_gift,
        MIN(donated_at) AS first_gift_date, MAX(donated_at) AS last_gift_date
      FROM serving.donation_detail
      WHERE amount > 0
      GROUP BY person_id
    ) g ON g.master_id = im.master_id
    LEFT JOIN (
      SELECT im2.master_id, COUNT(DISTINCT o.keap_id) AS order_count,
        SUM(oi.price_per_unit * oi.qty) AS total_spent
      FROM silver.[order] o
      JOIN silver.order_item oi ON oi.order_keap_id = o.keap_id
      JOIN silver.identity_map im2 ON im2.source_system = 'keap' AND im2.source_id = CAST(o.contact_keap_id AS VARCHAR)
      GROUP BY im2.master_id
    ) o ON o.master_id = im.master_id
    LEFT JOIN (
      SELECT im2.master_id, COUNT(*) AS tag_count
      FROM silver.contact_tag ct
      JOIN silver.identity_map im2 ON im2.source_system = 'keap' AND im2.source_id = CAST(ct.contact_keap_id AS VARCHAR)
      GROUP BY im2.master_id
    ) t ON t.master_id = im.master_id
    LEFT JOIN (
      SELECT im2.master_id, COUNT(*) AS note_count
      FROM silver.note nt
      JOIN silver.identity_map im2 ON im2.source_system = nt.source_system AND im2.source_id = nt.contact_source_id
      GROUP BY im2.master_id
    ) n ON n.master_id = im.master_id
    LEFT JOIN (
      SELECT im2.master_id, COUNT(*) AS comm_count
      FROM silver.communication cm
      JOIN silver.identity_map im2 ON im2.source_system = cm.source_system AND im2.source_id = cm.contact_source_id
      GROUP BY im2.master_id
    ) cm ON cm.master_id = im.master_id
    WHERE im.is_primary = 1`
  },

  // ── 2. HOUSEHOLD 360 ────────────────────────────────────────────────
  {
    name: 'serving.household_360',
    sql: `CREATE VIEW serving.household_360 AS
    SELECT
      ROW_NUMBER() OVER (ORDER BY ISNULL(c.household_name, c.last_name), c.state) AS household_id,
      ISNULL(c.household_name, c.last_name) AS name,
      COUNT(DISTINCT im.master_id) AS member_count,
      SUM(ISNULL(g.total_given, 0)) AS household_giving_total,
      CASE
        WHEN SUM(ISNULL(g.total_given, 0)) = 0 THEN 'none'
        WHEN MAX(g.last_gift_date) < DATEADD(YEAR, -1, GETDATE()) THEN 'declining'
        WHEN MIN(g.first_gift_date) > DATEADD(YEAR, -1, GETDATE()) THEN 'growing'
        ELSE 'stable'
      END AS giving_trend,
      c.state, c.city
    FROM silver.identity_map im
    JOIN silver.contact c ON c.contact_id = im.contact_id
    LEFT JOIN (
      SELECT person_id AS master_id, SUM(amount) AS total_given,
        MIN(donated_at) AS first_gift_date, MAX(donated_at) AS last_gift_date
      FROM serving.donation_detail
      WHERE amount > 0
      GROUP BY person_id
    ) g ON g.master_id = im.master_id
    WHERE im.is_primary = 1
      AND (c.household_name IS NOT NULL OR c.last_name IS NOT NULL)
    GROUP BY ISNULL(c.household_name, c.last_name), c.state, c.city`
  },

  // ── 3. DONATION DETAIL ──────────────────────────────────────────────
  // Deduplicates cross-source donations (givebutter mirrors donor_direct).
  // Uses ROW_NUMBER to keep one row per (person, amount, date), preferring donor_direct.
  // Filters out bad dates (year 1900 defaults).
  // Treats literal "NULL" strings as SQL NULL for name construction.
  {
    name: 'serving.donation_detail',
    sql: `CREATE VIEW serving.donation_detail AS
    WITH raw_donations AS (
      SELECT
        d.donation_id,
        COALESCE(im.master_id, c.contact_id) AS person_id,
        COALESCE(
          NULLIF(c.first_name, 'NULL') + ' ' + NULLIF(c.last_name, 'NULL'),
          NULLIF(c.first_name, 'NULL'),
          NULLIF(c.last_name, 'NULL'),
          'Unknown'
        ) AS display_name,
        NULLIF(c.first_name, 'NULL') AS first_name,
        NULLIF(c.last_name, 'NULL') AS last_name,
        c.email_primary AS email,
        d.amount, d.currency,
        d.donated_at,
        FORMAT(d.donated_at, 'yyyy-MM') AS donation_month,
        YEAR(d.donated_at) AS donation_year,
        d.payment_type AS payment_method,
        d.fund_code AS fund,
        d.source_code AS appeal,
        d.campaign_name AS designation,
        d.source_system, d.source_id AS source_ref,
        ROW_NUMBER() OVER (
          PARTITION BY COALESCE(im.master_id, c.contact_id), d.amount, CAST(d.donated_at AS DATE)
          ORDER BY CASE d.source_system
            WHEN 'donor_direct' THEN 1
            WHEN 'kindful' THEN 2
            WHEN 'keap_import' THEN 3
            ELSE 4
          END
        ) AS rn
      FROM silver.donation d
      LEFT JOIN silver.contact c ON c.source_system = d.source_system AND c.source_id = d.contact_source_id
      LEFT JOIN silver.identity_map im ON im.contact_id = c.contact_id
      WHERE d.donated_at IS NOT NULL AND YEAR(d.donated_at) >= 2000
    )
    SELECT donation_id, person_id, display_name, first_name, last_name, email,
      amount, currency, donated_at, donation_month, donation_year,
      payment_method, fund, appeal, designation, source_system, source_ref
    FROM raw_donations
    WHERE rn = 1`
  },

  // ── 4. DONOR SUMMARY ───────────────────────────────────────────────
  // Picks best non-NULL display name per person (avoids MAX picking "NULL NULL" or "Unknown")
  {
    name: 'serving.donor_summary',
    sql: `CREATE VIEW serving.donor_summary AS
    SELECT
      person_id,
      COALESCE(
        MAX(CASE WHEN display_name NOT IN ('Unknown') AND display_name NOT LIKE '%NULL%' THEN display_name END),
        MAX(CASE WHEN display_name <> 'Unknown' THEN display_name END),
        MAX(display_name)
      ) AS display_name,
      COALESCE(
        MAX(CASE WHEN first_name IS NOT NULL THEN first_name END),
        MAX(first_name)
      ) AS first_name,
      COALESCE(
        MAX(CASE WHEN last_name IS NOT NULL THEN last_name END),
        MAX(last_name)
      ) AS last_name,
      MAX(email) AS email,
      MAX(source_system) AS primary_source,
      COUNT(*) AS donation_count,
      SUM(amount) AS total_given,
      AVG(amount) AS avg_gift,
      MAX(amount) AS largest_gift,
      MIN(donated_at) AS first_gift_date,
      MAX(donated_at) AS last_gift_date,
      DATEDIFF(DAY, MAX(donated_at), GETDATE()) AS days_since_last,
      COUNT(DISTINCT fund) AS fund_count,
      COUNT(DISTINCT donation_month) AS active_months,
      CASE
        WHEN DATEDIFF(DAY, MAX(donated_at), GETDATE()) <= 180 THEN 'active'
        WHEN DATEDIFF(DAY, MAX(donated_at), GETDATE()) <= 365 THEN 'cooling'
        WHEN DATEDIFF(DAY, MAX(donated_at), GETDATE()) <= 730 THEN 'lapsed'
        ELSE 'lost'
      END AS lifecycle_stage
    FROM serving.donation_detail
    WHERE amount > 0
    GROUP BY person_id`
  },

  // ── 5. DONOR MONTHLY ───────────────────────────────────────────────
  // Best non-NULL display name per person-month
  {
    name: 'serving.donor_monthly',
    sql: `CREATE VIEW serving.donor_monthly AS
    SELECT
      person_id,
      COALESCE(
        MAX(CASE WHEN display_name NOT IN ('Unknown') AND display_name NOT LIKE '%NULL%' THEN display_name END),
        MAX(display_name)
      ) AS display_name,
      donation_month,
      donation_year,
      COUNT(*) AS gifts,
      SUM(amount) AS amount,
      MAX(fund) AS primary_fund,
      MAX(payment_method) AS primary_method
    FROM serving.donation_detail
    WHERE amount > 0
    GROUP BY person_id, donation_month, donation_year`
  },

  // ── 6. ORDER DETAIL ────────────────────────────────────────────────
  {
    name: 'serving.order_detail',
    sql: `CREATE VIEW serving.order_detail AS
    SELECT
      o.order_id,
      im.master_id AS person_id,
      COALESCE(NULLIF(c.first_name,'NULL') + ' ' + NULLIF(c.last_name,'NULL'), NULLIF(c.first_name,'NULL'), NULLIF(c.last_name,'NULL'), 'Unknown') AS display_name,
      NULLIF(c.first_name,'NULL') AS first_name, NULLIF(c.last_name,'NULL') AS last_name, c.email_primary AS email,
      o.keap_id AS order_number,
      ISNULL(oi_total.total_amount, 0) AS total_amount,
      o.created_at AS order_date,
      FORMAT(o.created_at, 'yyyy-MM') AS order_month,
      YEAR(o.created_at) AS order_year,
      o.order_status,
      'keap' AS source_system
    FROM silver.[order] o
    JOIN silver.identity_map im ON im.source_system = 'keap' AND im.source_id = CAST(o.contact_keap_id AS VARCHAR)
    LEFT JOIN silver.contact c ON c.contact_id = im.contact_id
    LEFT JOIN (
      SELECT order_keap_id, SUM(price_per_unit * qty) AS total_amount
      FROM silver.order_item GROUP BY order_keap_id
    ) oi_total ON oi_total.order_keap_id = o.keap_id
    WHERE im.is_primary = 1`
  },

  // ── 7. PAYMENT DETAIL ──────────────────────────────────────────────
  {
    name: 'serving.payment_detail',
    sql: `CREATE VIEW serving.payment_detail AS
    SELECT
      p.payment_id,
      im.master_id AS person_id,
      COALESCE(NULLIF(c.first_name,'NULL') + ' ' + NULLIF(c.last_name,'NULL'), NULLIF(c.first_name,'NULL'), NULLIF(c.last_name,'NULL'), 'Unknown') AS display_name,
      NULLIF(c.first_name,'NULL') AS first_name, NULLIF(c.last_name,'NULL') AS last_name, c.email_primary AS email,
      p.amount,
      p.pay_date AS payment_date,
      FORMAT(p.pay_date, 'yyyy-MM') AS payment_month,
      p.pay_type AS payment_method,
      p.invoice_keap_id AS invoice_id,
      'keap' AS source_system
    FROM silver.payment p
    JOIN silver.identity_map im ON im.source_system = 'keap' AND im.source_id = CAST(p.contact_keap_id AS VARCHAR)
    LEFT JOIN silver.contact c ON c.contact_id = im.contact_id
    WHERE im.is_primary = 1`
  },

  // ── 8. INVOICE DETAIL ──────────────────────────────────────────────
  {
    name: 'serving.invoice_detail',
    sql: `CREATE VIEW serving.invoice_detail AS
    SELECT
      i.invoice_id,
      im.master_id AS person_id,
      COALESCE(NULLIF(c.first_name,'NULL') + ' ' + NULLIF(c.last_name,'NULL'), NULLIF(c.first_name,'NULL'), NULLIF(c.last_name,'NULL'), 'Unknown') AS display_name,
      NULLIF(c.first_name,'NULL') AS first_name, NULLIF(c.last_name,'NULL') AS last_name, c.email_primary AS email,
      i.keap_id AS invoice_number,
      i.total AS invoice_total,
      i.pay_status AS invoice_status,
      i.created_at AS issued_at,
      FORMAT(i.created_at, 'yyyy-MM') AS invoice_month,
      'keap' AS source_system
    FROM silver.invoice i
    JOIN silver.identity_map im ON im.source_system = 'keap' AND im.source_id = CAST(i.contact_keap_id AS VARCHAR)
    LEFT JOIN silver.contact c ON c.contact_id = im.contact_id
    WHERE im.is_primary = 1`
  },

  // ── 9. SUBSCRIPTION DETAIL ─────────────────────────────────────────
  {
    name: 'serving.subscription_detail',
    sql: `CREATE VIEW serving.subscription_detail AS
    SELECT
      s.subscription_id,
      im.master_id AS person_id,
      COALESCE(NULLIF(c.first_name,'NULL') + ' ' + NULLIF(c.last_name,'NULL'), NULLIF(c.first_name,'NULL'), NULLIF(c.last_name,'NULL'), 'Unknown') AS display_name,
      NULLIF(c.first_name,'NULL') AS first_name, NULLIF(c.last_name,'NULL') AS last_name, c.email_primary AS email,
      ISNULL(pr.name, 'Unknown Product') AS product_name,
      s.billing_amount AS amount,
      s.billing_cycle AS cadence,
      s.status AS subscription_status,
      s.start_date,
      s.next_bill_date AS next_renewal,
      s.reason_stopped,
      'keap' AS source_system
    FROM silver.subscription s
    JOIN silver.identity_map im ON im.source_system = 'keap' AND im.source_id = CAST(s.contact_keap_id AS VARCHAR)
    LEFT JOIN silver.contact c ON c.contact_id = im.contact_id
    LEFT JOIN silver.product pr ON pr.keap_id = s.product_id
    WHERE im.is_primary = 1`
  },

  // ── 10. TAG DETAIL ─────────────────────────────────────────────────
  {
    name: 'serving.tag_detail',
    sql: `CREATE VIEW serving.tag_detail AS
    SELECT
      ct.contact_tag_id AS tag_id,
      im.master_id AS person_id,
      COALESCE(NULLIF(c.first_name,'NULL') + ' ' + NULLIF(c.last_name,'NULL'), NULLIF(c.first_name,'NULL'), NULLIF(c.last_name,'NULL'), 'Unknown') AS display_name,
      NULLIF(c.first_name,'NULL') AS first_name, NULLIF(c.last_name,'NULL') AS last_name,
      t.group_name AS tag_value,
      t.category_name AS tag_group,
      ct.date_applied AS applied_at,
      'keap' AS source_system
    FROM silver.contact_tag ct
    JOIN silver.tag t ON t.keap_id = ct.tag_keap_id
    JOIN silver.identity_map im ON im.source_system = 'keap' AND im.source_id = CAST(ct.contact_keap_id AS VARCHAR)
    LEFT JOIN silver.contact c ON c.contact_id = im.contact_id
    WHERE im.is_primary = 1`
  },

  // ── 11. COMMUNICATION DETAIL ───────────────────────────────────────
  {
    name: 'serving.communication_detail',
    sql: `CREATE VIEW serving.communication_detail AS
    SELECT
      cm.comm_id AS communication_id,
      COALESCE(im.master_id, c.contact_id) AS person_id,
      COALESCE(NULLIF(c.first_name,'NULL') + ' ' + NULLIF(c.last_name,'NULL'), NULLIF(c.first_name,'NULL'), NULLIF(c.last_name,'NULL'), 'Unknown') AS display_name,
      NULLIF(c.first_name,'NULL') AS first_name, NULLIF(c.last_name,'NULL') AS last_name,
      cm.comm_type AS channel,
      cm.direction,
      cm.subject,
      cm.comm_date AS sent_at,
      cm.source_system
    FROM silver.communication cm
    LEFT JOIN silver.contact c ON c.source_system = cm.source_system AND c.source_id = cm.contact_source_id
    LEFT JOIN silver.identity_map im ON im.contact_id = c.contact_id`
  },
];

// ═══════════════════════════════════════════════════════════════════════════
// MAIN
// ═══════════════════════════════════════════════════════════════════════════

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

  // Create serving schema
  console.log('Creating serving schema...');
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'serving')
      EXEC('CREATE SCHEMA serving')
  `);

  // Create each view
  for (const v of VIEWS) {
    console.log(`  ${v.name}...`);
    try {
      await pool.request().query(
        `IF OBJECT_ID('${v.name}', 'V') IS NOT NULL DROP VIEW ${v.name}`
      );
      await pool.request().query(v.sql);
      // Row count
      const cnt = await pool.request().query(`SELECT COUNT(*) n FROM ${v.name}`);
      console.log(`    OK (${cnt.recordset[0].n.toLocaleString()} rows)`);
    } catch (err) {
      console.error(`    FAIL: ${err.message.substring(0, 200)}`);
    }
  }

  await pool.close();
  console.log(`\nDone. Created ${VIEWS.length} serving views.`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
