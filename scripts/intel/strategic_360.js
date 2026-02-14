const sql = require('mssql');
const fs = require('fs');
const path = require('path');

const envPath = path.join(__dirname, '..', '..', '.env.local');
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
  requestTimeout: 300000,
  connectionTimeout: 30000,
};

function tbl(label, rows) {
  console.log('\n' + '='.repeat(90));
  console.log('  ' + label);
  console.log('='.repeat(90));
  if (!rows || rows.length === 0) { console.log('  (no rows)'); return; }
  console.table(rows);
}

async function main() {
  const pool = await sql.connect(config);
  console.log('Connected.\n');

  // 1. Overall database inventory
  const inv = await pool.request().query(`
    SELECT 'person_360' t, COUNT(*) n FROM serving.person_360 UNION ALL
    SELECT 'donor_summary', COUNT(*) FROM serving.donor_summary UNION ALL
    SELECT 'donation_detail', COUNT(*) FROM serving.donation_detail UNION ALL
    SELECT 'donor_monthly', COUNT(*) FROM serving.donor_monthly UNION ALL
    SELECT 'order_detail', COUNT(*) FROM serving.order_detail UNION ALL
    SELECT 'payment_detail', COUNT(*) FROM serving.payment_detail UNION ALL
    SELECT 'invoice_detail', COUNT(*) FROM serving.invoice_detail UNION ALL
    SELECT 'subscription_detail', COUNT(*) FROM serving.subscription_detail UNION ALL
    SELECT 'tag_detail', COUNT(*) FROM serving.tag_detail UNION ALL
    SELECT 'household_360', COUNT(*) FROM serving.household_360 UNION ALL
    SELECT 'communication_detail', COUNT(*) FROM serving.communication_detail UNION ALL
    SELECT 'wealth_screening', COUNT(*) FROM serving.wealth_screening UNION ALL
    SELECT 'lost_recurring_donors', COUNT(*) FROM serving.lost_recurring_donors UNION ALL
    SELECT 'stripe_customer', COUNT(*) FROM serving.stripe_customer`);
  tbl('1. DATABASE INVENTORY', inv.recordset);

  // 2. Contact email coverage
  const email = await pool.request().query(`
    SELECT COUNT(*) total,
      SUM(CASE WHEN email IS NOT NULL AND email<>'' THEN 1 ELSE 0 END) has_email,
      SUM(CASE WHEN phone IS NOT NULL AND phone<>'' THEN 1 ELSE 0 END) has_phone,
      SUM(CASE WHEN city IS NOT NULL AND city<>'' THEN 1 ELSE 0 END) has_city,
      SUM(CASE WHEN state IS NOT NULL AND state<>'' THEN 1 ELSE 0 END) has_state
    FROM serving.person_360`);
  tbl('2. CONTACT DATA COMPLETENESS', email.recordset);

  // 3. Donor retention year-over-year
  const ret = await pool.request().query(`
    WITH yearly_donors AS (
      SELECT DISTINCT person_id, donation_year
      FROM serving.donation_detail WHERE donation_year>=2020
    )
    SELECT a.donation_year,
      COUNT(DISTINCT a.person_id) total_donors,
      SUM(CASE WHEN b.person_id IS NOT NULL THEN 1 ELSE 0 END) retained_next_year,
      CAST(SUM(CASE WHEN b.person_id IS NOT NULL THEN 1.0 ELSE 0 END)*100/COUNT(*) AS DECIMAL(5,1)) retention_pct
    FROM yearly_donors a
    LEFT JOIN yearly_donors b ON b.person_id=a.person_id AND b.donation_year=a.donation_year+1
    WHERE a.donation_year<2025
    GROUP BY a.donation_year ORDER BY a.donation_year`);
  tbl('3. DONOR RETENTION RATE (Year-over-Year)', ret.recordset);

  // 4. New vs returning donors per year
  const newret = await pool.request().query(`
    WITH first_year AS (
      SELECT person_id, MIN(donation_year) first_yr FROM serving.donation_detail GROUP BY person_id
    )
    SELECT d.donation_year,
      COUNT(DISTINCT d.person_id) total_donors,
      SUM(CASE WHEN f.first_yr=d.donation_year THEN 1 ELSE 0 END) new_donors,
      SUM(CASE WHEN f.first_yr<d.donation_year THEN 1 ELSE 0 END) returning_donors,
      CAST(SUM(CASE WHEN f.first_yr<d.donation_year THEN 1.0 ELSE 0 END)*100/COUNT(DISTINCT d.person_id) AS DECIMAL(5,1)) returning_pct
    FROM serving.donation_detail d
    JOIN first_year f ON f.person_id=d.person_id
    WHERE d.donation_year>=2020
    GROUP BY d.donation_year ORDER BY d.donation_year`);
  tbl('4. NEW vs RETURNING DONORS', newret.recordset);

  // 5. Commerce yearly trends
  const comm = await pool.request().query(`
    SELECT order_year, COUNT(DISTINCT person_id) buyers,
      COUNT(*) orders, CAST(SUM(total_amount) AS INT) revenue,
      CAST(AVG(total_amount) AS INT) avg_order
    FROM serving.order_detail
    WHERE order_year>=2020 AND display_name<>'Unknown'
    GROUP BY order_year ORDER BY order_year`);
  tbl('5. COMMERCE YEARLY TRENDS', comm.recordset);

  // 6. Subscription status summary
  const subs = await pool.request().query(`
    SELECT subscription_status, COUNT(*) subs,
      COUNT(DISTINCT person_id) people,
      CAST(SUM(amount) AS INT) monthly_value
    FROM serving.subscription_detail
    GROUP BY subscription_status`);
  tbl('6. SUBSCRIPTION STATUS', subs.recordset);

  // 7. Cross-stream overlap (person_360 based)
  const overlap = await pool.request().query(`
    SELECT
      CASE
        WHEN donation_count>0 AND order_count>0 THEN 'Donor + Buyer'
        WHEN donation_count>0 THEN 'Donor Only'
        WHEN order_count>0 THEN 'Buyer Only'
        ELSE 'Engaged Only (no transactions)'
      END AS segment,
      COUNT(*) people,
      CAST(AVG(lifetime_giving) AS INT) avg_giving,
      CAST(AVG(total_spent) AS INT) avg_commerce,
      CAST(AVG(CAST(tag_count AS FLOAT)) AS INT) avg_tags
    FROM serving.person_360 WHERE display_name<>'Unknown'
    GROUP BY CASE
        WHEN donation_count>0 AND order_count>0 THEN 'Donor + Buyer'
        WHEN donation_count>0 THEN 'Donor Only'
        WHEN order_count>0 THEN 'Buyer Only'
        ELSE 'Engaged Only (no transactions)'
      END ORDER BY COUNT(*) DESC`);
  tbl('7. CROSS-STREAM OVERLAP', overlap.recordset);

  // 8. Household giving distribution
  const hh = await pool.request().query(`
    SELECT CASE WHEN household_giving_total=0 THEN '$0'
      WHEN household_giving_total<100 THEN '$1-$100'
      WHEN household_giving_total<1000 THEN '$100-$1K'
      WHEN household_giving_total<10000 THEN '$1K-$10K'
      ELSE '$10K+' END AS tier,
      COUNT(*) households,
      CAST(SUM(household_giving_total) AS INT) total
    FROM serving.household_360
    GROUP BY CASE WHEN household_giving_total=0 THEN '$0'
      WHEN household_giving_total<100 THEN '$1-$100'
      WHEN household_giving_total<1000 THEN '$100-$1K'
      WHEN household_giving_total<10000 THEN '$1K-$10K'
      ELSE '$10K+' END ORDER BY MIN(household_giving_total)`);
  tbl('8. HOUSEHOLD GIVING DISTRIBUTION', hh.recordset);

  // 9. Monthly giving trend (last 24 months)
  const monthly = await pool.request().query(`
    SELECT donation_month, COUNT(DISTINCT person_id) donors,
      CAST(SUM(amount) AS INT) total
    FROM serving.donation_detail
    WHERE donation_month >= FORMAT(DATEADD(YEAR,-2,GETDATE()),'yyyy-MM')
    GROUP BY donation_month ORDER BY donation_month`);
  tbl('9. MONTHLY GIVING (last 24 months)', monthly.recordset);

  // 10. Donor-to-buyer conversion (do donors also buy?)
  const d2b = await pool.request().query(`
    SELECT CASE WHEN order_count>0 THEN 'Also a Buyer' ELSE 'Donor Only' END AS status,
      COUNT(*) donors,
      CAST(AVG(lifetime_giving) AS INT) avg_giving,
      CAST(AVG(total_spent) AS INT) avg_commerce
    FROM serving.person_360
    WHERE donation_count>0 AND display_name<>'Unknown'
    GROUP BY CASE WHEN order_count>0 THEN 'Also a Buyer' ELSE 'Donor Only' END`);
  tbl('10. DO DONORS ALSO BUY?', d2b.recordset);

  // 11. Top 15 states by contact count
  const geo = await pool.request().query(`
    SELECT TOP 15 state, COUNT(*) contacts,
      SUM(CASE WHEN donation_count>0 THEN 1 ELSE 0 END) donors,
      SUM(CASE WHEN order_count>0 THEN 1 ELSE 0 END) buyers,
      CAST(SUM(lifetime_giving) AS INT) total_giving
    FROM serving.person_360
    WHERE state IS NOT NULL AND state<>'' AND display_name<>'Unknown'
    GROUP BY state ORDER BY COUNT(*) DESC`);
  tbl('11. TOP 15 STATES BY CONTACTS', geo.recordset);

  // 12. Lifecycle stage funnel
  const lc = await pool.request().query(`
    SELECT lifecycle_stage, COUNT(*) people,
      CAST(SUM(lifetime_giving) AS INT) total_giving,
      CAST(AVG(lifetime_giving) AS INT) avg_giving,
      CAST(SUM(total_spent) AS INT) total_commerce
    FROM serving.person_360 WHERE display_name<>'Unknown'
    GROUP BY lifecycle_stage ORDER BY COUNT(*) DESC`);
  tbl('12. LIFECYCLE STAGE FUNNEL', lc.recordset);

  // 13. Giving by source system
  const src = await pool.request().query(`
    SELECT source_system, COUNT(*) gifts,
      COUNT(DISTINCT person_id) donors,
      CAST(SUM(amount) AS INT) total
    FROM serving.donation_detail
    GROUP BY source_system ORDER BY SUM(amount) DESC`);
  tbl('13. GIVING BY SOURCE SYSTEM', src.recordset);

  // 14. Top 10 donors with cross-stream data
  const top10 = await pool.request().query(`
    SELECT TOP 10 p.display_name,
      p.lifetime_giving, p.donation_count, p.order_count,
      p.total_spent, p.tag_count, p.lifecycle_stage, p.recency_days
    FROM serving.person_360 p
    WHERE p.display_name<>'Unknown'
    ORDER BY p.lifetime_giving DESC`);
  tbl('14. TOP 10 DONORS (FULL 360)', top10.recordset);

  // 15. Quarterly giving trend
  const qtr = await pool.request().query(`
    SELECT donation_year,
      DATEPART(QUARTER, donated_at) AS q,
      COUNT(DISTINCT person_id) donors,
      CAST(SUM(amount) AS INT) total
    FROM serving.donation_detail
    WHERE donation_year>=2022
    GROUP BY donation_year, DATEPART(QUARTER, donated_at)
    ORDER BY donation_year, DATEPART(QUARTER, donated_at)`);
  tbl('15. QUARTERLY GIVING TREND', qtr.recordset);

  await pool.close();
  console.log('\nDONE.');
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
