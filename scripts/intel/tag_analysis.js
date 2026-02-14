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

  // C1: Tag group distribution (no JOIN - fast)
  console.log('Running C1...');
  const c1 = await pool.request().query(`
    SELECT tag_group, COUNT(DISTINCT person_id) tagged_people, COUNT(*) total_tags
    FROM serving.tag_detail WHERE tag_group IS NOT NULL
    GROUP BY tag_group ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C1. TAG GROUP DISTRIBUTION', c1.recordset);

  // C2: Top 30 tags (no JOIN - fast)
  console.log('Running C2...');
  const c2 = await pool.request().query(`
    SELECT TOP 30 tag_value, tag_group, COUNT(DISTINCT person_id) people
    FROM serving.tag_detail WHERE tag_value IS NOT NULL
    GROUP BY tag_value, tag_group ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C2. TOP 30 MOST COMMON TAGS', c2.recordset);

  // C3: Engagement depth (no JOIN - uses CTE on tag_detail only)
  console.log('Running C3...');
  const c3 = await pool.request().query(`
    WITH tag_counts AS (
      SELECT person_id, COUNT(DISTINCT tag_group) groups, COUNT(*) total_tags
      FROM serving.tag_detail GROUP BY person_id
    )
    SELECT CASE WHEN groups=1 THEN '1-Single Program'
      WHEN groups=2 THEN '2-Two Programs'
      WHEN groups=3 THEN '3-Three Programs'
      WHEN groups>=4 THEN '4-Multi (4+)' END AS depth,
      COUNT(*) people, CAST(AVG(total_tags) AS INT) avg_tags
    FROM tag_counts
    GROUP BY CASE WHEN groups=1 THEN '1-Single Program'
      WHEN groups=2 THEN '2-Two Programs'
      WHEN groups=3 THEN '3-Three Programs'
      WHEN groups>=4 THEN '4-Multi (4+)' END ORDER BY 1`);
  tbl('C3. MULTI-TAG ENGAGEMENT DEPTH', c3.recordset);

  // C4: Communication channels (small table)
  console.log('Running C4...');
  const c4 = await pool.request().query(`
    SELECT channel, direction, COUNT(*) comms, COUNT(DISTINCT person_id) people
    FROM serving.communication_detail
    GROUP BY channel, direction ORDER BY COUNT(*) DESC`);
  tbl('C4. COMMUNICATION CHANNELS', c4.recordset);

  // C5: Lifecycle stage x engagement (person_360 only - fast)
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

  // C6: Top engagement zero giving (person_360 only - fast)
  console.log('Running C6...');
  const c6 = await pool.request().query(`
    SELECT TOP 20 display_name, tag_count, note_count, comm_count,
      order_count, total_spent, lifecycle_stage
    FROM serving.person_360
    WHERE lifetime_giving=0 AND display_name<>'Unknown'
    ORDER BY tag_count + note_count + comm_count DESC`);
  tbl('C6. TOP ENGAGEMENT + ZERO GIVING (CONVERSION TARGETS)', c6.recordset);

  // C7: Program affinity (tag_detail only, no JOIN)
  console.log('Running C7...');
  const c7 = await pool.request().query(`
    WITH person_programs AS (
      SELECT person_id,
        MAX(CASE WHEN tag_group='True Girl' THEN 1 ELSE 0 END) AS tg,
        MAX(CASE WHEN tag_group='B2BB' THEN 1 ELSE 0 END) AS b2bb,
        MAX(CASE WHEN tag_group='Donor Assignment' THEN 1 ELSE 0 END) AS donor,
        MAX(CASE WHEN tag_group='Customer Tags' THEN 1 ELSE 0 END) AS customer,
        MAX(CASE WHEN tag_group='Nurture Tags' THEN 1 ELSE 0 END) AS nurture,
        MAX(CASE WHEN tag_group='Box Tracking' THEN 1 ELSE 0 END) AS box
      FROM serving.tag_detail WHERE tag_group IS NOT NULL GROUP BY person_id
    )
    SELECT CASE
      WHEN tg=1 AND b2bb=1 AND donor=1 THEN 'TrueGirl+B2BB+Donor'
      WHEN tg=1 AND b2bb=1 THEN 'TrueGirl+B2BB'
      WHEN tg=1 AND donor=1 THEN 'TrueGirl+Donor'
      WHEN b2bb=1 AND donor=1 THEN 'B2BB+Donor'
      WHEN tg=1 THEN 'TrueGirl Only'
      WHEN b2bb=1 THEN 'B2BB Only'
      WHEN donor=1 THEN 'Donor Only'
      WHEN customer=1 THEN 'Customer Only'
      WHEN nurture=1 THEN 'Nurture Only'
      WHEN box=1 THEN 'Box Tracking Only'
      ELSE 'Other' END AS segment,
      COUNT(*) people
    FROM person_programs
    GROUP BY CASE
      WHEN tg=1 AND b2bb=1 AND donor=1 THEN 'TrueGirl+B2BB+Donor'
      WHEN tg=1 AND b2bb=1 THEN 'TrueGirl+B2BB'
      WHEN tg=1 AND donor=1 THEN 'TrueGirl+Donor'
      WHEN b2bb=1 AND donor=1 THEN 'B2BB+Donor'
      WHEN tg=1 THEN 'TrueGirl Only'
      WHEN b2bb=1 THEN 'B2BB Only'
      WHEN donor=1 THEN 'Donor Only'
      WHEN customer=1 THEN 'Customer Only'
      WHEN nurture=1 THEN 'Nurture Only'
      WHEN box=1 THEN 'Box Tracking Only'
      ELSE 'Other' END ORDER BY COUNT(*) DESC`);
  tbl('C7. PROGRAM AFFINITY SEGMENTS', c7.recordset);

  // C8: True Girl tags
  console.log('Running C8...');
  const c8 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(DISTINCT person_id) people
    FROM serving.tag_detail WHERE tag_group='True Girl'
    GROUP BY tag_value ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C8. TOP TRUE GIRL TAGS', c8.recordset);

  // C9: B2BB tags
  console.log('Running C9...');
  const c9 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(DISTINCT person_id) people
    FROM serving.tag_detail WHERE tag_group='B2BB'
    GROUP BY tag_value ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C9. TOP B2BB (BIBLE STUDY) TAGS', c9.recordset);

  // C10: Yearly giving trends (donation_detail - fast)
  console.log('Running C10...');
  const c10 = await pool.request().query(`
    SELECT donation_year, COUNT(DISTINCT person_id) donors,
      COUNT(*) gifts, CAST(SUM(amount) AS INT) total,
      CAST(AVG(amount) AS INT) avg_gift
    FROM serving.donation_detail WHERE donation_year>=2018
    GROUP BY donation_year ORDER BY donation_year`);
  tbl('C10. YEARLY GIVING TRENDS (2018+)', c10.recordset);

  // C11: Payment methods
  console.log('Running C11...');
  const c11 = await pool.request().query(`
    SELECT payment_method, COUNT(*) gifts, COUNT(DISTINCT person_id) donors,
      CAST(SUM(amount) AS INT) total
    FROM serving.donation_detail WHERE payment_method IS NOT NULL
    GROUP BY payment_method ORDER BY SUM(amount) DESC`);
  tbl('C11. PAYMENT METHOD BREAKDOWN', c11.recordset);

  // C12: Engaged non-donors via person_360 (faster than tag JOIN)
  console.log('Running C12...');
  const c12 = await pool.request().query(`
    SELECT CASE WHEN tag_count>=100 THEN 'Super Engaged (100+ tags)'
      WHEN tag_count>=30 THEN 'Highly Engaged (30-100 tags)'
      WHEN tag_count>=10 THEN 'Moderately Engaged (10-30 tags)'
      WHEN tag_count>=1 THEN 'Lightly Engaged (1-10 tags)'
      ELSE 'No Tags' END AS engagement,
      COUNT(*) people,
      SUM(CASE WHEN donation_count=0 THEN 1 ELSE 0 END) non_donors,
      CAST(AVG(order_count) AS INT) avg_orders,
      CAST(AVG(total_spent) AS INT) avg_commerce
    FROM serving.person_360 WHERE display_name<>'Unknown'
    GROUP BY CASE WHEN tag_count>=100 THEN 'Super Engaged (100+ tags)'
      WHEN tag_count>=30 THEN 'Highly Engaged (30-100 tags)'
      WHEN tag_count>=10 THEN 'Moderately Engaged (10-30 tags)'
      WHEN tag_count>=1 THEN 'Lightly Engaged (1-10 tags)'
      ELSE 'No Tags' END ORDER BY MIN(tag_count) DESC`);
  tbl('C12. ENGAGEMENT LEVEL vs DONOR CONVERSION', c12.recordset);

  // C13: Donor Assignment tag details
  console.log('Running C13...');
  const c13 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(DISTINCT person_id) people
    FROM serving.tag_detail WHERE tag_group='Donor Assignment'
    GROUP BY tag_value ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C13. DONOR ASSIGNMENT TAGS', c13.recordset);

  // C14: Nurture tags
  console.log('Running C14...');
  const c14 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(DISTINCT person_id) people
    FROM serving.tag_detail WHERE tag_group='Nurture Tags'
    GROUP BY tag_value ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C14. NURTURE TAGS', c14.recordset);

  await pool.close();
  console.log('\n\nALL TAG/ENGAGEMENT QUERIES COMPLETED.');
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
