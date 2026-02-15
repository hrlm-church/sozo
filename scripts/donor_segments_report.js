const sql = require('mssql');
const fs = require('fs');
const path = require('path');

const envPath = path.join(__dirname, '..', '.env.local');
const envText = fs.readFileSync(envPath, 'utf8');
const env = {};
for (const line of envText.split('\n')) {
  const m = line.match(/^([^#=]+)=(.*)$/);
  if (m) env[m[1].trim()] = m[2].trim();
}

const config = {
  server: env.SOZO_SQL_HOST,
  database: 'sozov2',
  user: env.SOZO_SQL_USER,
  password: env.SOZO_SQL_PASSWORD,
  options: { encrypt: true, trustServerCertificate: false },
  requestTimeout: 120000,
  connectionTimeout: 30000,
};

function fmt(n) {
  if (n == null) return '-';
  return Number(n).toLocaleString('en-US');
}

function fmtMoney(n) {
  if (n == null) return '-';
  return '$' + Number(n).toLocaleString('en-US');
}

function printTable(rows, cols) {
  if (!rows || rows.length === 0) { console.log('  (no data)\n'); return; }
  const widths = {};
  for (const c of cols) {
    widths[c.key] = Math.max(c.label.length, ...rows.map(r => String(c.fmt ? c.fmt(r[c.key]) : (r[c.key] ?? '-')).length));
  }
  const hdr = cols.map(c => c.right ? c.label.padStart(widths[c.key]) : c.label.padEnd(widths[c.key])).join('  ');
  console.log('  ' + hdr);
  console.log('  ' + cols.map(c => '-'.repeat(widths[c.key])).join('  '));
  for (const r of rows) {
    const line = cols.map(c => {
      const val = String(c.fmt ? c.fmt(r[c.key]) : (r[c.key] ?? '-'));
      return c.right ? val.padStart(widths[c.key]) : val.padEnd(widths[c.key]);
    }).join('  ');
    console.log('  ' + line);
  }
  console.log('');
}

const queries = [
  {
    title: '1. DONOR FREQUENCY SEGMENTS',
    sql: "SELECT CASE WHEN donation_count = 1 THEN '1-One-Time' WHEN donation_count BETWEEN 2 AND 3 THEN '2-Occasional (2-3)' WHEN donation_count BETWEEN 4 AND 11 THEN '3-Regular (4-11)' WHEN donation_count >= 12 THEN '4-Committed (12+)' END AS segment, COUNT(*) donors, CAST(SUM(total_given) AS INT) total, CAST(AVG(total_given) AS INT) avg_ltv, CAST(AVG(avg_gift) AS INT) avg_gift FROM serving.donor_summary WHERE display_name<>'Unknown' GROUP BY CASE WHEN donation_count=1 THEN '1-One-Time' WHEN donation_count BETWEEN 2 AND 3 THEN '2-Occasional (2-3)' WHEN donation_count BETWEEN 4 AND 11 THEN '3-Regular (4-11)' WHEN donation_count>=12 THEN '4-Committed (12+)' END ORDER BY 1",
    cols: [
      { key: 'segment', label: 'Segment' },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
      { key: 'total', label: 'Total Given', right: true, fmt: fmtMoney },
      { key: 'avg_ltv', label: 'Avg LTV', right: true, fmt: fmtMoney },
      { key: 'avg_gift', label: 'Avg Gift', right: true, fmt: fmtMoney },
    ]
  },
  {
    title: '2. GIFT AMOUNT SEGMENTS (Lifetime Tiers)',
    sql: "SELECT CASE WHEN total_given<25 THEN '1-Micro (<$25)' WHEN total_given<100 THEN '2-Entry ($25-$100)' WHEN total_given<500 THEN '3-Developing ($100-$500)' WHEN total_given<1000 THEN '4-Growing ($500-$1K)' WHEN total_given<5000 THEN '5-Core ($1K-$5K)' WHEN total_given<10000 THEN '6-Mid-Major ($5K-$10K)' ELSE '7-Major ($10K+)' END AS tier, COUNT(*) donors, CAST(SUM(total_given) AS INT) total, CAST(AVG(donation_count) AS INT) avg_gifts, CAST(AVG(CAST(days_since_last AS FLOAT)) AS INT) avg_recency FROM serving.donor_summary WHERE display_name<>'Unknown' GROUP BY CASE WHEN total_given<25 THEN '1-Micro (<$25)' WHEN total_given<100 THEN '2-Entry ($25-$100)' WHEN total_given<500 THEN '3-Developing ($100-$500)' WHEN total_given<1000 THEN '4-Growing ($500-$1K)' WHEN total_given<5000 THEN '5-Core ($1K-$5K)' WHEN total_given<10000 THEN '6-Mid-Major ($5K-$10K)' ELSE '7-Major ($10K+)' END ORDER BY 1",
    cols: [
      { key: 'tier', label: 'Tier' },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
      { key: 'total', label: 'Total Given', right: true, fmt: fmtMoney },
      { key: 'avg_gifts', label: 'Avg Gifts', right: true, fmt: fmt },
      { key: 'avg_recency', label: 'Avg Recency (days)', right: true, fmt: fmt },
    ]
  },
  {
    title: '3. RECENCY x FREQUENCY MATRIX (RFM)',
    sql: "SELECT CASE WHEN days_since_last<=180 THEN 'Active' WHEN days_since_last<=365 THEN 'Cooling' WHEN days_since_last<=730 THEN 'Lapsing' ELSE 'Lapsed' END AS recency, CASE WHEN donation_count=1 THEN 'One-Time' WHEN donation_count<=3 THEN 'Occasional' WHEN donation_count<=11 THEN 'Regular' ELSE 'Committed' END AS frequency, COUNT(*) donors, CAST(SUM(total_given) AS INT) total_value, CAST(AVG(total_given) AS INT) avg_ltv FROM serving.donor_summary WHERE display_name<>'Unknown' GROUP BY CASE WHEN days_since_last<=180 THEN 'Active' WHEN days_since_last<=365 THEN 'Cooling' WHEN days_since_last<=730 THEN 'Lapsing' ELSE 'Lapsed' END, CASE WHEN donation_count=1 THEN 'One-Time' WHEN donation_count<=3 THEN 'Occasional' WHEN donation_count<=11 THEN 'Regular' ELSE 'Committed' END ORDER BY 1,2",
    cols: [
      { key: 'recency', label: 'Recency' },
      { key: 'frequency', label: 'Frequency' },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
      { key: 'total_value', label: 'Total Value', right: true, fmt: fmtMoney },
      { key: 'avg_ltv', label: 'Avg LTV', right: true, fmt: fmtMoney },
    ]
  },
  {
    title: '4. GIVING CHANNEL / PAYMENT METHOD',
    sql: "SELECT payment_method, COUNT(*) gifts, COUNT(DISTINCT person_id) donors, CAST(SUM(amount) AS INT) total FROM serving.donation_detail WHERE payment_method IS NOT NULL GROUP BY payment_method ORDER BY SUM(amount) DESC",
    cols: [
      { key: 'payment_method', label: 'Payment Method' },
      { key: 'gifts', label: 'Gifts', right: true, fmt: fmt },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
      { key: 'total', label: 'Total', right: true, fmt: fmtMoney },
    ]
  },
  {
    title: '5. TOP 20 FUNDS / APPEALS',
    sql: "SELECT TOP 20 fund, COUNT(*) gifts, COUNT(DISTINCT person_id) donors, CAST(SUM(amount) AS INT) total, CAST(AVG(amount) AS INT) avg FROM serving.donation_detail WHERE fund IS NOT NULL GROUP BY fund ORDER BY SUM(amount) DESC",
    cols: [
      { key: 'fund', label: 'Fund' },
      { key: 'gifts', label: 'Gifts', right: true, fmt: fmt },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
      { key: 'total', label: 'Total', right: true, fmt: fmtMoney },
      { key: 'avg', label: 'Avg Gift', right: true, fmt: fmtMoney },
    ]
  },
  {
    title: '6. DONOR UPGRADE / DOWNGRADE PATTERNS (YoY)',
    sql: "WITH yearly AS (SELECT person_id, donation_year, SUM(amount) AS year_total FROM serving.donation_detail WHERE donation_year>=2022 GROUP BY person_id, donation_year), yoy AS (SELECT a.person_id, a.donation_year, a.year_total AS current_year, b.year_total AS prior_year, CASE WHEN b.year_total IS NULL THEN 'New' WHEN a.year_total > b.year_total*1.1 THEN 'Upgraded' WHEN a.year_total < b.year_total*0.9 THEN 'Downgraded' ELSE 'Stable' END AS trend FROM yearly a LEFT JOIN yearly b ON b.person_id=a.person_id AND b.donation_year=a.donation_year-1) SELECT donation_year, trend, COUNT(*) donors, CAST(SUM(current_year) AS INT) total FROM yoy GROUP BY donation_year, trend ORDER BY donation_year, trend",
    cols: [
      { key: 'donation_year', label: 'Year' },
      { key: 'trend', label: 'Trend' },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
      { key: 'total', label: 'Total', right: true, fmt: fmtMoney },
    ]
  },
  {
    title: '7. WEALTH CAPACITY GAP (Top 30 Untapped)',
    sql: "SELECT TOP 30 w.display_name, w.capacity_label, w.giving_capacity, ISNULL(d.total_given,0) AS actual_giving, w.giving_capacity - ISNULL(d.total_given,0) AS gap, d.lifecycle_stage, d.donation_count, d.days_since_last FROM serving.wealth_screening w LEFT JOIN serving.donor_summary d ON d.person_id=w.person_id WHERE w.giving_capacity > 25000 ORDER BY (w.giving_capacity - ISNULL(d.total_given,0)) DESC",
    cols: [
      { key: 'display_name', label: 'Name' },
      { key: 'capacity_label', label: 'Capacity' },
      { key: 'giving_capacity', label: 'Capacity $', right: true, fmt: fmtMoney },
      { key: 'actual_giving', label: 'Actual Given', right: true, fmt: fmtMoney },
      { key: 'gap', label: 'Gap', right: true, fmt: fmtMoney },
      { key: 'lifecycle_stage', label: 'Stage' },
      { key: 'donation_count', label: 'Gifts', right: true, fmt: fmt },
      { key: 'days_since_last', label: 'Recency', right: true, fmt: fmt },
    ]
  },
  {
    title: '8. GIVING SEASONALITY BY MONTH',
    sql: "SELECT MONTH(donated_at) AS mo, COUNT(DISTINCT person_id) donors, COUNT(*) gifts, CAST(SUM(amount) AS INT) total, CAST(AVG(amount) AS INT) avg FROM serving.donation_detail GROUP BY MONTH(donated_at) ORDER BY 1",
    cols: [
      { key: 'mo', label: 'Month' },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
      { key: 'gifts', label: 'Gifts', right: true, fmt: fmt },
      { key: 'total', label: 'Total', right: true, fmt: fmtMoney },
      { key: 'avg', label: 'Avg Gift', right: true, fmt: fmtMoney },
    ]
  },
  {
    title: '9. FIRST GIFT AMOUNT DISTRIBUTION',
    sql: "WITH first_gifts AS (SELECT person_id, MIN(amount) AS first_amount FROM serving.donation_detail GROUP BY person_id) SELECT CASE WHEN first_amount<10 THEN '$1-$10' WHEN first_amount<25 THEN '$10-$25' WHEN first_amount<50 THEN '$25-$50' WHEN first_amount<100 THEN '$50-$100' WHEN first_amount<250 THEN '$100-$250' ELSE '$250+' END AS first_gift_range, COUNT(*) donors FROM first_gifts GROUP BY CASE WHEN first_amount<10 THEN '$1-$10' WHEN first_amount<25 THEN '$10-$25' WHEN first_amount<50 THEN '$25-$50' WHEN first_amount<100 THEN '$50-$100' WHEN first_amount<250 THEN '$100-$250' ELSE '$250+' END ORDER BY MIN(first_amount)",
    cols: [
      { key: 'first_gift_range', label: 'First Gift Range' },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
    ]
  },
  {
    title: '10. GEOGRAPHIC CONCENTRATION (Top 15 States)',
    sql: "SELECT TOP 15 p.state, COUNT(DISTINCT d.person_id) donors, CAST(SUM(d.total_given) AS INT) total, CAST(AVG(d.total_given) AS INT) avg_ltv FROM serving.donor_summary d JOIN serving.person_360 p ON p.person_id=d.person_id WHERE p.state IS NOT NULL AND p.state<>'' GROUP BY p.state ORDER BY SUM(d.total_given) DESC",
    cols: [
      { key: 'state', label: 'State' },
      { key: 'donors', label: 'Donors', right: true, fmt: fmt },
      { key: 'total', label: 'Total Given', right: true, fmt: fmtMoney },
      { key: 'avg_ltv', label: 'Avg LTV', right: true, fmt: fmtMoney },
    ]
  },
];

async function main() {
  console.log('='.repeat(90));
  console.log('  SOZO DONOR INTELLIGENCE REPORT -- sozov2');
  console.log('  Generated: ' + new Date().toISOString().slice(0,19).replace('T',' '));
  console.log('='.repeat(90));
  console.log('');

  let pool;
  try {
    pool = await sql.connect(config);
    console.log('  Connected to Azure SQL (sozov2)\n');

    for (const q of queries) {
      console.log('-'.repeat(90));
      console.log('  ' + q.title);
      console.log('-'.repeat(90));
      try {
        const result = await pool.request().query(q.sql);
        printTable(result.recordset, q.cols);
      } catch (err) {
        console.log('  ERROR: ' + err.message + '\n');
      }
    }

    console.log('='.repeat(90));
    console.log('  REPORT COMPLETE');
    console.log('='.repeat(90));
  } catch (err) {
    console.error('Connection error:', err.message);
  } finally {
    if (pool) await pool.close();
  }
}

main();
