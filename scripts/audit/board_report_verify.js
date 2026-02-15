/**
 * Board Report Fact-Check — Verification Queries
 *
 * Connects to Azure SQL (sozov2) and runs 12 queries to verify
 * board report claims about giving, donors, retention, etc.
 *
 * Run: node scripts/audit/board_report_verify.js
 */

const fs = require('fs');
const path = require('path');
const sql = require('mssql');

// ── env ─────────────────────────────────────────────────────────────────────
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(p)) { console.error('ERROR: .env.local not found'); process.exit(1); }
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

function getDbConfig() {
  return {
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 300000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 5, min: 0, idleTimeoutMillis: 10000 },
  };
}

// ── formatting helpers ──────────────────────────────────────────────────────
function fmtDollar(val) {
  if (val == null) return '$0.00';
  return '$' + Number(val).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function fmtNum(val) {
  if (val == null) return '0';
  return Number(val).toLocaleString('en-US');
}

function fmtPct(val) {
  if (val == null) return '0.0%';
  return Number(val).toFixed(1) + '%';
}

function printTable(rows, formatters = {}) {
  if (!rows || rows.length === 0) {
    console.log('  (no rows returned)');
    return;
  }
  const cols = Object.keys(rows[0]);
  // compute column widths
  const widths = {};
  for (const c of cols) {
    widths[c] = c.length;
    for (const r of rows) {
      const fmt = formatters[c];
      const val = fmt ? fmt(r[c]) : String(r[c] ?? '');
      widths[c] = Math.max(widths[c], val.length);
    }
  }
  // header
  const hdr = cols.map(c => c.padEnd(widths[c])).join('  |  ');
  console.log('  ' + hdr);
  console.log('  ' + cols.map(c => '-'.repeat(widths[c])).join('--+--'));
  // rows
  for (const r of rows) {
    const line = cols.map(c => {
      const fmt = formatters[c];
      const val = fmt ? fmt(r[c]) : String(r[c] ?? '');
      // right-align numbers and dollars
      if (fmt === fmtDollar || fmt === fmtNum || fmt === fmtPct) {
        return val.padStart(widths[c]);
      }
      return val.padEnd(widths[c]);
    }).join('  |  ');
    console.log('  ' + line);
  }
}

// ── queries ─────────────────────────────────────────────────────────────────
const queries = [
  {
    label: '1. TOTAL LIFETIME GIVING',
    sql: `SELECT SUM(amount) AS total_giving, COUNT(*) AS donation_count, COUNT(DISTINCT person_id) AS unique_donors FROM serving.donation_detail`,
    fmt: { total_giving: fmtDollar, donation_count: fmtNum, unique_donors: fmtNum }
  },
  {
    label: '2. DONOR TIER BREAKDOWN',
    sql: `SELECT 
  CASE 
    WHEN total_given >= 10000 THEN 'Major ($10K+)'
    WHEN total_given >= 5000 THEN 'Mid-Major ($5K-$10K)'
    WHEN total_given >= 1000 THEN 'Core ($1K-$5K)'
    WHEN total_given >= 100 THEN 'Developing ($100-$1K)'
    ELSE 'Entry (Under $100)'
  END AS tier,
  COUNT(*) AS donors,
  SUM(total_given) AS total_given
FROM serving.donor_summary
WHERE display_name <> 'Unknown'
GROUP BY CASE 
    WHEN total_given >= 10000 THEN 'Major ($10K+)'
    WHEN total_given >= 5000 THEN 'Mid-Major ($5K-$10K)'
    WHEN total_given >= 1000 THEN 'Core ($1K-$5K)'
    WHEN total_given >= 100 THEN 'Developing ($100-$1K)'
    ELSE 'Entry (Under $100)'
  END
ORDER BY MIN(total_given) DESC`,
    fmt: { donors: fmtNum, total_given: fmtDollar }
  },
  {
    label: '3. GIVING BY YEAR (2018-2025)',
    sql: `SELECT donation_year, 
  SUM(amount) AS total_giving, 
  COUNT(*) AS donations,
  COUNT(DISTINCT person_id) AS unique_donors,
  AVG(amount) AS avg_gift
FROM serving.donation_detail
WHERE donation_year >= 2018 AND donation_year <= 2025
GROUP BY donation_year
ORDER BY donation_year`,
    fmt: { total_giving: fmtDollar, donations: fmtNum, unique_donors: fmtNum, avg_gift: fmtDollar }
  },
  {
    label: '4. FIRST-YEAR RETENTION BY COHORT',
    sql: `WITH first_gift AS (
  SELECT person_id, MIN(donation_year) AS cohort_year
  FROM serving.donation_detail
  WHERE amount > 0
  GROUP BY person_id
),
cohort_donors AS (
  SELECT fg.cohort_year, fg.person_id,
    MAX(CASE WHEN dd.donation_year = fg.cohort_year + 1 THEN 1 ELSE 0 END) AS gave_yr1,
    MAX(CASE WHEN dd.donation_year = fg.cohort_year + 2 THEN 1 ELSE 0 END) AS gave_yr2,
    MAX(CASE WHEN dd.donation_year = fg.cohort_year + 3 THEN 1 ELSE 0 END) AS gave_yr3
  FROM first_gift fg
  JOIN serving.donation_detail dd ON dd.person_id = fg.person_id
  WHERE fg.cohort_year >= 2018 AND fg.cohort_year <= 2024
  GROUP BY fg.cohort_year, fg.person_id
)
SELECT cohort_year,
  COUNT(*) AS new_donors,
  CAST(SUM(gave_yr1) * 100.0 / COUNT(*) AS DECIMAL(5,1)) AS yr1_retention_pct,
  CAST(SUM(gave_yr2) * 100.0 / COUNT(*) AS DECIMAL(5,1)) AS yr2_retention_pct,
  CAST(SUM(gave_yr3) * 100.0 / COUNT(*) AS DECIMAL(5,1)) AS yr3_retention_pct
FROM cohort_donors
GROUP BY cohort_year
ORDER BY cohort_year`,
    fmt: { new_donors: fmtNum, yr1_retention_pct: fmtPct, yr2_retention_pct: fmtPct, yr3_retention_pct: fmtPct }
  },
  {
    label: '5. DONOR RECENCY DISTRIBUTION',
    sql: `SELECT 
  CASE
    WHEN days_since_last <= 180 THEN 'Active (0-6 months)'
    WHEN days_since_last <= 365 THEN 'Cooling (6-12 months)'
    WHEN days_since_last <= 730 THEN 'Lapsing (12-24 months)'
    ELSE 'Lapsed (24+ months)'
  END AS segment,
  COUNT(*) AS donors,
  SUM(total_given) AS lifetime_value
FROM serving.donor_summary
WHERE display_name <> 'Unknown'
GROUP BY CASE
    WHEN days_since_last <= 180 THEN 'Active (0-6 months)'
    WHEN days_since_last <= 365 THEN 'Cooling (6-12 months)'
    WHEN days_since_last <= 730 THEN 'Lapsing (12-24 months)'
    ELSE 'Lapsed (24+ months)'
  END
ORDER BY MIN(days_since_last)`,
    fmt: { donors: fmtNum, lifetime_value: fmtDollar }
  },
  {
    label: '6. TOTAL CONTACT COUNT',
    sql: `SELECT COUNT(*) AS total_contacts FROM serving.person_360`,
    fmt: { total_contacts: fmtNum }
  },
  {
    label: '7. COMMERCE STATS',
    sql: `SELECT COUNT(*) AS total_orders, COUNT(DISTINCT person_id) AS unique_buyers, SUM(total_amount) AS total_revenue FROM serving.order_detail WHERE display_name <> 'Unknown'`,
    fmt: { total_orders: fmtNum, unique_buyers: fmtNum, total_revenue: fmtDollar }
  },
  {
    label: '8. SUBSCRIPTION STATS',
    sql: `SELECT subscription_status, COUNT(*) AS cnt, SUM(amount) AS total_amount FROM serving.subscription_detail GROUP BY subscription_status`,
    fmt: { cnt: fmtNum, total_amount: fmtDollar }
  },
  {
    label: '9. TAG / ENGAGEMENT COUNT',
    sql: `SELECT COUNT(DISTINCT person_id) AS tagged_contacts, COUNT(*) AS total_tags FROM serving.tag_detail`,
    fmt: { tagged_contacts: fmtNum, total_tags: fmtNum }
  },
  {
    label: '10. AVERAGE AND MEDIAN GIFT',
    sql: `SELECT 
  AVG(amount) AS avg_gift,
  (SELECT TOP 1 PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) OVER () FROM serving.donation_detail) AS median_gift
FROM serving.donation_detail
WHERE amount > 0`,
    fmt: { avg_gift: fmtDollar, median_gift: fmtDollar }
  },
  {
    label: '11. DECEMBER GIVING (SEASONALITY)',
    sql: `SELECT donation_year, donation_month, SUM(amount) AS total, COUNT(*) AS gifts, AVG(amount) AS avg_gift
FROM serving.donation_detail
WHERE MONTH(donated_at) = 12 AND donation_year >= 2018
GROUP BY donation_year, donation_month
ORDER BY donation_year`,
    fmt: { total: fmtDollar, gifts: fmtNum, avg_gift: fmtDollar }
  },
  {
    label: '12. SOURCE SYSTEM BREAKDOWN OF DONATIONS',
    sql: `SELECT source_system, COUNT(*) AS donations, SUM(amount) AS total, COUNT(DISTINCT person_id) AS donors
FROM serving.donation_detail
GROUP BY source_system
ORDER BY total DESC`,
    fmt: { donations: fmtNum, total: fmtDollar, donors: fmtNum }
  },
];

// ── main ────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();
  const cfg = getDbConfig();
  console.log(`\nConnecting to ${cfg.server} / ${cfg.database} ...\n`);

  const pool = await sql.connect(cfg);
  console.log('Connected successfully.\n');
  console.log('='.repeat(80));
  console.log('  BOARD REPORT FACT-CHECK — VERIFICATION QUERIES');
  console.log('  Database: sozov2');
  console.log('  Run date: ' + new Date().toISOString().slice(0, 19).replace('T', ' '));
  console.log('='.repeat(80));

  for (const q of queries) {
    console.log('\n' + '-'.repeat(80));
    console.log(`  ${q.label}`);
    console.log('-'.repeat(80));
    try {
      const result = await pool.request().query(q.sql);
      printTable(result.recordset, q.fmt);
    } catch (err) {
      console.log(`  ERROR: ${err.message}`);
    }
  }

  console.log('\n' + '='.repeat(80));
  console.log('  VERIFICATION COMPLETE');
  console.log('='.repeat(80) + '\n');

  await pool.close();
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
