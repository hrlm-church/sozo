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
  console.log('  -> ' + rows.length + ' rows');
}

async function main() {
  const pool = await sql.connect(config);
  console.log('Connected to sozov2.\n');

  // C2: Top tags - use COUNT(*) instead of COUNT(DISTINCT person_id) for speed
  console.log('Running C2 (fast)...');
  const c2 = await pool.request().query(`
    SELECT TOP 30 tag_value, tag_group, COUNT(*) tag_assignments
    FROM serving.tag_detail WHERE tag_value IS NOT NULL AND tag_group IS NOT NULL
    GROUP BY tag_value, tag_group ORDER BY COUNT(*) DESC`);
  tbl('C2. TOP 30 TAGS BY ASSIGNMENT COUNT', c2.recordset);

  // C3: Engagement depth using person_360 tag_count (fast)
  console.log('Running C3...');
  const c3 = await pool.request().query(`
    SELECT CASE WHEN tag_count>=100 THEN '1-Super Engaged (100+ tags)'
      WHEN tag_count>=30 THEN '2-Highly Engaged (30-100)'
      WHEN tag_count>=10 THEN '3-Moderate (10-30)'
      WHEN tag_count>=1 THEN '4-Light (1-10)'
      ELSE '5-No Tags' END AS engagement,
      COUNT(*) people,
      SUM(CASE WHEN donation_count>0 THEN 1 ELSE 0 END) donors,
      CAST(SUM(CASE WHEN donation_count>0 THEN 1.0 ELSE 0 END)*100/COUNT(*) AS DECIMAL(5,1)) donor_pct,
      CAST(AVG(lifetime_giving) AS INT) avg_giving,
      CAST(AVG(order_count) AS FLOAT) avg_orders
    FROM serving.person_360 WHERE display_name<>'Unknown'
    GROUP BY CASE WHEN tag_count>=100 THEN '1-Super Engaged (100+ tags)'
      WHEN tag_count>=30 THEN '2-Highly Engaged (30-100)'
      WHEN tag_count>=10 THEN '3-Moderate (10-30)'
      WHEN tag_count>=1 THEN '4-Light (1-10)'
      ELSE '5-No Tags' END ORDER BY 1`);
  tbl('C3. ENGAGEMENT DEPTH vs DONOR CONVERSION', c3.recordset);

  // C4: Communication channels
  console.log('Running C4...');
  const c4 = await pool.request().query(`
    SELECT channel, direction, COUNT(*) comms, COUNT(DISTINCT person_id) people
    FROM serving.communication_detail
    GROUP BY channel, direction ORDER BY COUNT(*) DESC`);
  tbl('C4. COMMUNICATION CHANNELS', c4.recordset);

  // C5: Lifecycle x engagement
  console.log('Running C5...');
  const c5 = await pool.request().query(`
    SELECT lifecycle_stage, COUNT(*) people,
      CAST(AVG(CAST(tag_count AS FLOAT)) AS INT) avg_tags,
      CAST(AVG(CAST(note_count AS FLOAT)) AS INT) avg_notes,
      CAST(AVG(CAST(comm_count AS FLOAT)) AS INT) avg_comms,
      CAST(AVG(CAST(order_count AS FLOAT)) AS INT) avg_orders,
      CAST(AVG(lifetime_giving) AS INT) avg_giving
    FROM serving.person_360 WHERE display_name<>'Unknown'
    GROUP BY lifecycle_stage ORDER BY COUNT(*) DESC`);
  tbl('C5. LIFECYCLE STAGE x ENGAGEMENT', c5.recordset);

  // C6: Top engaged zero giving
  console.log('Running C6...');
  const c6 = await pool.request().query(`
    SELECT TOP 20 display_name, tag_count, note_count, comm_count,
      order_count, total_spent, lifecycle_stage
    FROM serving.person_360
    WHERE lifetime_giving=0 AND display_name<>'Unknown'
    ORDER BY tag_count + note_count + comm_count DESC`);
  tbl('C6. TOP 20 ENGAGED NON-DONORS (CONVERSION TARGETS)', c6.recordset);

  // C7: True Girl top tags (filter first = faster)
  console.log('Running C7...');
  const c7 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(*) assignments
    FROM serving.tag_detail WHERE tag_group='True Girl' AND tag_value IS NOT NULL
    GROUP BY tag_value ORDER BY COUNT(*) DESC`);
  tbl('C7. TOP TRUE GIRL TAGS', c7.recordset);

  // C8: B2BB top tags
  console.log('Running C8...');
  const c8 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(*) assignments
    FROM serving.tag_detail WHERE tag_group='B2BB' AND tag_value IS NOT NULL
    GROUP BY tag_value ORDER BY COUNT(*) DESC`);
  tbl('C8. TOP B2BB (BIBLE STUDY) TAGS', c8.recordset);

  // C9: Yearly giving trends
  console.log('Running C9...');
  const c9 = await pool.request().query(`
    SELECT donation_year, COUNT(DISTINCT person_id) donors,
      COUNT(*) gifts, CAST(SUM(amount) AS INT) total,
      CAST(AVG(amount) AS INT) avg_gift
    FROM serving.donation_detail WHERE donation_year>=2018
    GROUP BY donation_year ORDER BY donation_year`);
  tbl('C9. YEARLY GIVING TRENDS (2018+)', c9.recordset);

  // C10: Payment methods
  console.log('Running C10...');
  const c10 = await pool.request().query(`
    SELECT payment_method, COUNT(*) gifts, COUNT(DISTINCT person_id) donors,
      CAST(SUM(amount) AS INT) total
    FROM serving.donation_detail WHERE payment_method IS NOT NULL
    GROUP BY payment_method ORDER BY SUM(amount) DESC`);
  tbl('C10. PAYMENT METHOD BREAKDOWN', c10.recordset);

  // C11: Donor Assignment tags
  console.log('Running C11...');
  const c11 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(*) assignments
    FROM serving.tag_detail WHERE tag_group='Donor Assignment' AND tag_value IS NOT NULL
    GROUP BY tag_value ORDER BY COUNT(*) DESC`);
  tbl('C11. DONOR ASSIGNMENT TAGS', c11.recordset);

  // C12: Nurture tags
  console.log('Running C12...');
  const c12 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(*) assignments
    FROM serving.tag_detail WHERE tag_group='Nurture Tags' AND tag_value IS NOT NULL
    GROUP BY tag_value ORDER BY COUNT(*) DESC`);
  tbl('C12. NURTURE TAGS', c12.recordset);

  // C13: Tour marketing tags (aggregated by season)
  console.log('Running C13...');
  const c13 = await pool.request().query(`
    SELECT tag_group, COUNT(*) assignments
    FROM serving.tag_detail
    WHERE tag_group LIKE '%Tour%'
    GROUP BY tag_group ORDER BY COUNT(*) DESC`);
  tbl('C13. TOUR MARKETING TAG GROUPS', c13.recordset);

  // C14: Subscription box related tags
  console.log('Running C14...');
  const c14 = await pool.request().query(`
    SELECT TOP 20 tag_value, tag_group, COUNT(*) assignments
    FROM serving.tag_detail
    WHERE tag_group IN ('Box Tracking', 'Subscription Boxes 2023 Import', 'Subscription Box Funnel', 'Feb Boxes')
    AND tag_value IS NOT NULL
    GROUP BY tag_value, tag_group ORDER BY COUNT(*) DESC`);
  tbl('C14. SUBSCRIPTION BOX RELATED TAGS', c14.recordset);

  // C15: Wealth screening capacity gap
  console.log('Running C15...');
  const c15 = await pool.request().query(`
    SELECT w.capacity_label,
      COUNT(*) screened,
      SUM(CASE WHEN d.person_id IS NOT NULL THEN 1 ELSE 0 END) are_donors,
      CAST(AVG(w.giving_capacity) AS INT) avg_capacity,
      CAST(AVG(ISNULL(d.total_given,0)) AS INT) avg_actual,
      CAST(AVG(w.giving_capacity) - AVG(ISNULL(d.total_given,0)) AS INT) avg_gap
    FROM serving.wealth_screening w
    LEFT JOIN serving.donor_summary d ON d.person_id=w.person_id
    GROUP BY w.capacity_label ORDER BY AVG(w.giving_capacity) DESC`);
  tbl('C15. WEALTH CAPACITY GAP', c15.recordset);

  await pool.close();
  console.log('\n\nALL QUERIES COMPLETED.');
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
