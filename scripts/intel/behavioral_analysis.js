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
  requestTimeout: 180000,
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

  // ============== SECTION A: DONOR BEHAVIOR ==============
  console.log('\n\n' + '#'.repeat(90));
  console.log('  SECTION A: DONOR BEHAVIOR ANALYSIS');
  console.log('#'.repeat(90));

  // A1: Frequency segments
  const a1 = await pool.request().query(`
    SELECT CASE WHEN donation_count=1 THEN '1-One-Time'
      WHEN donation_count BETWEEN 2 AND 3 THEN '2-Occasional (2-3)'
      WHEN donation_count BETWEEN 4 AND 11 THEN '3-Regular (4-11)'
      WHEN donation_count>=12 THEN '4-Committed (12+)' END AS segment,
      COUNT(*) donors, CAST(SUM(total_given) AS INT) total,
      CAST(AVG(total_given) AS INT) avg_ltv, CAST(AVG(avg_gift) AS INT) avg_gift
    FROM serving.donor_summary WHERE display_name<>'Unknown'
    GROUP BY CASE WHEN donation_count=1 THEN '1-One-Time'
      WHEN donation_count BETWEEN 2 AND 3 THEN '2-Occasional (2-3)'
      WHEN donation_count BETWEEN 4 AND 11 THEN '3-Regular (4-11)'
      WHEN donation_count>=12 THEN '4-Committed (12+)' END ORDER BY 1`);
  tbl('A1. DONOR FREQUENCY SEGMENTS', a1.recordset);

  // A2: Lifetime giving tiers
  const a2 = await pool.request().query(`
    SELECT CASE WHEN total_given<25 THEN '1-Micro (<$25)'
      WHEN total_given<100 THEN '2-Entry ($25-$100)'
      WHEN total_given<500 THEN '3-Developing ($100-$500)'
      WHEN total_given<1000 THEN '4-Growing ($500-$1K)'
      WHEN total_given<5000 THEN '5-Core ($1K-$5K)'
      WHEN total_given<10000 THEN '6-Mid-Major ($5K-$10K)'
      ELSE '7-Major ($10K+)' END AS tier,
      COUNT(*) donors, CAST(SUM(total_given) AS INT) total,
      CAST(AVG(donation_count) AS INT) avg_gifts,
      CAST(AVG(CAST(days_since_last AS FLOAT)) AS INT) avg_recency
    FROM serving.donor_summary WHERE display_name<>'Unknown'
    GROUP BY CASE WHEN total_given<25 THEN '1-Micro (<$25)'
      WHEN total_given<100 THEN '2-Entry ($25-$100)'
      WHEN total_given<500 THEN '3-Developing ($100-$500)'
      WHEN total_given<1000 THEN '4-Growing ($500-$1K)'
      WHEN total_given<5000 THEN '5-Core ($1K-$5K)'
      WHEN total_given<10000 THEN '6-Mid-Major ($5K-$10K)'
      ELSE '7-Major ($10K+)' END ORDER BY 1`);
  tbl('A2. LIFETIME GIVING TIERS', a2.recordset);

  // A3: RFM Matrix
  const a3 = await pool.request().query(`
    SELECT CASE WHEN days_since_last<=180 THEN 'Active (0-6mo)'
      WHEN days_since_last<=365 THEN 'Cooling (6-12mo)'
      WHEN days_since_last<=730 THEN 'Lapsing (1-2yr)'
      ELSE 'Lapsed (2yr+)' END AS recency,
      CASE WHEN donation_count=1 THEN 'One-Time'
        WHEN donation_count<=3 THEN 'Occasional'
        WHEN donation_count<=11 THEN 'Regular'
        ELSE 'Committed' END AS frequency,
      COUNT(*) donors, CAST(SUM(total_given) AS INT) total_value,
      CAST(AVG(total_given) AS INT) avg_ltv
    FROM serving.donor_summary WHERE display_name<>'Unknown'
    GROUP BY CASE WHEN days_since_last<=180 THEN 'Active (0-6mo)'
      WHEN days_since_last<=365 THEN 'Cooling (6-12mo)'
      WHEN days_since_last<=730 THEN 'Lapsing (1-2yr)'
      ELSE 'Lapsed (2yr+)' END,
      CASE WHEN donation_count=1 THEN 'One-Time'
        WHEN donation_count<=3 THEN 'Occasional'
        WHEN donation_count<=11 THEN 'Regular'
        ELSE 'Committed' END
    ORDER BY 1,2`);
  tbl('A3. RECENCY x FREQUENCY MATRIX (RFM)', a3.recordset);

  // A4: Top 20 Funds
  const a4 = await pool.request().query(`
    SELECT TOP 20 fund, COUNT(*) gifts, COUNT(DISTINCT person_id) donors,
      CAST(SUM(amount) AS INT) total, CAST(AVG(amount) AS INT) avg_gift
    FROM serving.donation_detail WHERE fund IS NOT NULL
    GROUP BY fund ORDER BY SUM(amount) DESC`);
  tbl('A4. TOP 20 FUNDS BY REVENUE', a4.recordset);

  // A5: Donor upgrade/downgrade YoY
  const a5 = await pool.request().query(`
    WITH yearly AS (
      SELECT person_id, donation_year, SUM(amount) AS year_total
      FROM serving.donation_detail WHERE donation_year>=2022
      GROUP BY person_id, donation_year
    ), yoy AS (
      SELECT a.person_id, a.donation_year, a.year_total current_yr,
        b.year_total prior_yr,
        CASE WHEN b.year_total IS NULL THEN 'New'
          WHEN a.year_total > b.year_total*1.1 THEN 'Upgraded'
          WHEN a.year_total < b.year_total*0.9 THEN 'Downgraded'
          ELSE 'Stable' END AS trend
      FROM yearly a LEFT JOIN yearly b
        ON b.person_id=a.person_id AND b.donation_year=a.donation_year-1
    )
    SELECT donation_year, trend, COUNT(*) donors, CAST(SUM(current_yr) AS INT) total
    FROM yoy GROUP BY donation_year, trend ORDER BY donation_year, trend`);
  tbl('A5. DONOR UPGRADE/DOWNGRADE PATTERNS (YoY)', a5.recordset);

  // A6: Giving seasonality
  const a6 = await pool.request().query(`
    SELECT MONTH(donated_at) AS mo, COUNT(DISTINCT person_id) donors,
      COUNT(*) gifts, CAST(SUM(amount) AS INT) total, CAST(AVG(amount) AS INT) avg_gift
    FROM serving.donation_detail GROUP BY MONTH(donated_at) ORDER BY 1`);
  tbl('A6. GIVING SEASONALITY BY MONTH', a6.recordset);

  // A7: First gift amount distribution
  const a7 = await pool.request().query(`
    WITH first_gifts AS (
      SELECT person_id, MIN(amount) AS first_amount
      FROM serving.donation_detail GROUP BY person_id
    )
    SELECT CASE WHEN first_amount<10 THEN '$1-$10'
      WHEN first_amount<25 THEN '$10-$25'
      WHEN first_amount<50 THEN '$25-$50'
      WHEN first_amount<100 THEN '$50-$100'
      WHEN first_amount<250 THEN '$100-$250'
      ELSE '$250+' END AS first_gift_range,
      COUNT(*) donors
    FROM first_gifts GROUP BY CASE WHEN first_amount<10 THEN '$1-$10'
      WHEN first_amount<25 THEN '$10-$25'
      WHEN first_amount<50 THEN '$25-$50'
      WHEN first_amount<100 THEN '$50-$100'
      WHEN first_amount<250 THEN '$100-$250'
      ELSE '$250+' END ORDER BY MIN(first_amount)`);
  tbl('A7. FIRST GIFT AMOUNT DISTRIBUTION', a7.recordset);

  // A8: Geographic concentration
  const a8 = await pool.request().query(`
    SELECT TOP 15 p.state, COUNT(DISTINCT d.person_id) donors,
      CAST(SUM(d.total_given) AS INT) total,
      CAST(AVG(d.total_given) AS INT) avg_ltv
    FROM serving.donor_summary d
    JOIN serving.person_360 p ON p.person_id=d.person_id
    WHERE p.state IS NOT NULL AND p.state<>''
    GROUP BY p.state ORDER BY SUM(d.total_given) DESC`);
  tbl('A8. GEOGRAPHIC CONCENTRATION (Top 15 States)', a8.recordset);

  // A9: Wealth capacity gap
  const a9 = await pool.request().query(`
    SELECT w.capacity_label,
      COUNT(*) screened,
      SUM(CASE WHEN d.person_id IS NOT NULL THEN 1 ELSE 0 END) are_donors,
      CAST(AVG(w.giving_capacity) AS INT) avg_capacity,
      CAST(AVG(ISNULL(d.total_given,0)) AS INT) avg_actual,
      CAST(AVG(w.giving_capacity) - AVG(ISNULL(d.total_given,0)) AS INT) avg_gap,
      CAST(SUM(w.giving_capacity - ISNULL(d.total_given,0)) AS BIGINT) total_gap
    FROM serving.wealth_screening w
    LEFT JOIN serving.donor_summary d ON d.person_id=w.person_id
    GROUP BY w.capacity_label ORDER BY AVG(w.giving_capacity) DESC`);
  tbl('A9. WEALTH CAPACITY GAP ANALYSIS', a9.recordset);

  // A10: Top 30 wealth gap individuals
  const a10 = await pool.request().query(`
    SELECT TOP 30 w.display_name, w.capacity_label, w.giving_capacity,
      ISNULL(d.total_given,0) AS actual_giving,
      w.giving_capacity - ISNULL(d.total_given,0) AS gap,
      d.lifecycle_stage, d.donation_count, d.days_since_last
    FROM serving.wealth_screening w
    LEFT JOIN serving.donor_summary d ON d.person_id=w.person_id
    WHERE w.giving_capacity>25000
    ORDER BY (w.giving_capacity - ISNULL(d.total_given,0)) DESC`);
  tbl('A10. TOP 30 WEALTH GAP INDIVIDUALS', a10.recordset);

  // ============== SECTION B: SUBSCRIPTIONS & COMMERCE ==============
  console.log('\n\n' + '#'.repeat(90));
  console.log('  SECTION B: SUBSCRIPTIONS & COMMERCE ANALYSIS');
  console.log('#'.repeat(90));

  // B1: Subscription products
  const b1 = await pool.request().query(`
    SELECT product_name, COUNT(*) total,
      SUM(CASE WHEN subscription_status='Active' THEN 1 ELSE 0 END) active,
      SUM(CASE WHEN subscription_status='Inactive' THEN 1 ELSE 0 END) churned,
      CAST(SUM(CASE WHEN subscription_status='Active' THEN 1 ELSE 0 END)*100.0/COUNT(*) AS DECIMAL(5,1)) retention_pct,
      CAST(AVG(amount) AS DECIMAL(8,2)) avg_price
    FROM serving.subscription_detail
    GROUP BY product_name ORDER BY COUNT(*) DESC`);
  tbl('B1. SUBSCRIPTION PRODUCT BREAKDOWN', b1.recordset);

  // B2: Subscriber → Donor crossover
  const b2 = await pool.request().query(`
    SELECT CASE WHEN d.person_id IS NOT NULL THEN 'Also a Donor' ELSE 'Never Donated' END AS donor_status,
      s.subscription_status, COUNT(DISTINCT s.person_id) subscribers,
      CAST(ISNULL(AVG(d.total_given),0) AS INT) avg_giving
    FROM serving.subscription_detail s
    LEFT JOIN serving.donor_summary d ON d.person_id=s.person_id
    GROUP BY CASE WHEN d.person_id IS NOT NULL THEN 'Also a Donor' ELSE 'Never Donated' END,
      s.subscription_status ORDER BY 1,2`);
  tbl('B2. SUBSCRIBER-DONOR CROSSOVER', b2.recordset);

  // B3: Order frequency segments
  const b3 = await pool.request().query(`
    WITH buyer_stats AS (
      SELECT person_id, COUNT(*) orders, SUM(total_amount) total_spent
      FROM serving.order_detail WHERE display_name<>'Unknown'
      GROUP BY person_id
    )
    SELECT CASE WHEN orders=1 THEN '1-Single Buyer'
      WHEN orders BETWEEN 2 AND 5 THEN '2-Repeat (2-5)'
      WHEN orders BETWEEN 6 AND 20 THEN '3-Loyal (6-20)'
      ELSE '4-VIP (20+)' END AS segment,
      COUNT(*) buyers, CAST(AVG(total_spent) AS INT) avg_spend,
      CAST(AVG(orders) AS INT) avg_orders
    FROM buyer_stats
    GROUP BY CASE WHEN orders=1 THEN '1-Single Buyer'
      WHEN orders BETWEEN 2 AND 5 THEN '2-Repeat (2-5)'
      WHEN orders BETWEEN 6 AND 20 THEN '3-Loyal (6-20)'
      ELSE '4-VIP (20+)' END ORDER BY 1`);
  tbl('B3. ORDER FREQUENCY SEGMENTS', b3.recordset);

  // B4: Buyers who never donated
  const b4 = await pool.request().query(`
    SELECT COUNT(DISTINCT o.person_id) AS total_buyers,
      SUM(CASE WHEN d.person_id IS NULL THEN 1 ELSE 0 END) AS never_donated,
      CAST(SUM(CASE WHEN d.person_id IS NULL THEN o.total_amount ELSE 0 END) AS INT) commerce_only_spend
    FROM (SELECT person_id, SUM(total_amount) total_amount FROM serving.order_detail WHERE display_name<>'Unknown' GROUP BY person_id) o
    LEFT JOIN serving.donor_summary d ON d.person_id=o.person_id`);
  tbl('B4. BUYERS WHO NEVER DONATED', b4.recordset);

  // B5: Triple overlap
  const b5 = await pool.request().query(`
    SELECT CASE
      WHEN d.person_id IS NOT NULL AND o.person_id IS NOT NULL THEN 'Donor+Buyer+Subscriber'
      WHEN d.person_id IS NOT NULL THEN 'Donor+Subscriber'
      WHEN o.person_id IS NOT NULL THEN 'Buyer+Subscriber'
      ELSE 'Subscriber Only' END AS overlap,
      COUNT(DISTINCT s.person_id) people
    FROM serving.subscription_detail s
    LEFT JOIN serving.donor_summary d ON d.person_id=s.person_id
    LEFT JOIN (SELECT DISTINCT person_id FROM serving.order_detail) o ON o.person_id=s.person_id
    GROUP BY CASE
      WHEN d.person_id IS NOT NULL AND o.person_id IS NOT NULL THEN 'Donor+Buyer+Subscriber'
      WHEN d.person_id IS NOT NULL THEN 'Donor+Subscriber'
      WHEN o.person_id IS NOT NULL THEN 'Buyer+Subscriber'
      ELSE 'Subscriber Only' END ORDER BY 1`);
  tbl('B5. TRIPLE OVERLAP: Subscriber + Donor + Buyer', b5.recordset);

  // B6: Lost recurring donors
  const b6 = await pool.request().query(`
    SELECT category, COUNT(*) donors,
      CAST(SUM(monthly_amount) AS INT) monthly_mrr,
      CAST(SUM(annual_value) AS INT) annual_value
    FROM serving.lost_recurring_donors
    GROUP BY category ORDER BY SUM(monthly_amount) DESC`);
  tbl('B6. LOST RECURRING DONORS BY CATEGORY', b6.recordset);

  // B7: Subscription price sensitivity
  const b7 = await pool.request().query(`
    SELECT CASE WHEN amount<20 THEN '$0-$20'
      WHEN amount<35 THEN '$20-$35'
      WHEN amount<50 THEN '$35-$50'
      WHEN amount<100 THEN '$50-$100'
      ELSE '$100+' END AS price_range,
      COUNT(*) total,
      SUM(CASE WHEN subscription_status='Active' THEN 1 ELSE 0 END) active,
      CAST(SUM(CASE WHEN subscription_status='Active' THEN 1.0 ELSE 0 END)*100/COUNT(*) AS DECIMAL(5,1)) retention_pct
    FROM serving.subscription_detail
    GROUP BY CASE WHEN amount<20 THEN '$0-$20'
      WHEN amount<35 THEN '$20-$35'
      WHEN amount<50 THEN '$35-$50'
      WHEN amount<100 THEN '$50-$100'
      ELSE '$100+' END ORDER BY MIN(amount)`);
  tbl('B7. SUBSCRIPTION PRICE SENSITIVITY', b7.recordset);

  // B8: Stripe customer analysis
  const b8 = await pool.request().query(`
    SELECT CASE WHEN person_id IS NOT NULL THEN 'Linked to Identity' ELSE 'Unlinked' END AS status,
      COUNT(*) customers, CAST(SUM(total_spend) AS INT) total_spend,
      CAST(AVG(total_spend) AS INT) avg_spend, SUM(payment_count) payments
    FROM serving.stripe_customer
    GROUP BY CASE WHEN person_id IS NOT NULL THEN 'Linked to Identity' ELSE 'Unlinked' END`);
  tbl('B8. STRIPE CUSTOMER LINKAGE', b8.recordset);

  // ============== SECTION C: TAGS & ENGAGEMENT ==============
  console.log('\n\n' + '#'.repeat(90));
  console.log('  SECTION C: TAGS & ENGAGEMENT ANALYSIS');
  console.log('#'.repeat(90));

  // C1: Tag group distribution
  const c1 = await pool.request().query(`
    SELECT t.tag_group, COUNT(DISTINCT t.person_id) tagged_people,
      COUNT(DISTINCT d.person_id) are_donors,
      CAST(COUNT(DISTINCT d.person_id)*100.0/NULLIF(COUNT(DISTINCT t.person_id),0) AS DECIMAL(5,1)) donor_pct,
      CAST(ISNULL(SUM(d.total_given),0) AS INT) total_giving
    FROM serving.tag_detail t
    LEFT JOIN serving.donor_summary d ON d.person_id=t.person_id
    WHERE t.tag_group IS NOT NULL
    GROUP BY t.tag_group ORDER BY COUNT(DISTINCT t.person_id) DESC`);
  tbl('C1. TAG GROUP DISTRIBUTION WITH DONOR OVERLAP', c1.recordset);

  // C2: Top 30 tags
  const c2 = await pool.request().query(`
    SELECT TOP 30 tag_value, tag_group, COUNT(DISTINCT person_id) people
    FROM serving.tag_detail WHERE tag_value IS NOT NULL
    GROUP BY tag_value, tag_group ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C2. TOP 30 MOST COMMON TAGS', c2.recordset);

  // C3: Engagement depth
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

  // C4: Engaged non-donors
  const c4 = await pool.request().query(`
    WITH tagged AS (
      SELECT person_id, COUNT(DISTINCT tag_group) programs, COUNT(*) tags
      FROM serving.tag_detail GROUP BY person_id
    )
    SELECT CASE WHEN t.programs>=4 THEN 'Super Engaged (4+ programs)'
      WHEN t.programs=3 THEN 'Highly Engaged (3 programs)'
      WHEN t.programs=2 THEN 'Moderately Engaged (2 programs)'
      ELSE 'Lightly Engaged (1 program)' END AS engagement_level,
      COUNT(*) people, CAST(AVG(t.tags) AS INT) avg_tags
    FROM tagged t
    WHERE t.person_id NOT IN (SELECT person_id FROM serving.donor_summary)
    GROUP BY CASE WHEN t.programs>=4 THEN 'Super Engaged (4+ programs)'
      WHEN t.programs=3 THEN 'Highly Engaged (3 programs)'
      WHEN t.programs=2 THEN 'Moderately Engaged (2 programs)'
      ELSE 'Lightly Engaged (1 program)' END ORDER BY 1`);
  tbl('C4. ENGAGED NON-DONORS (CONVERSION TARGETS)', c4.recordset);

  // C5: Communication channels
  const c5 = await pool.request().query(`
    SELECT channel, direction, COUNT(*) comms, COUNT(DISTINCT person_id) people
    FROM serving.communication_detail
    GROUP BY channel, direction ORDER BY COUNT(*) DESC`);
  tbl('C5. COMMUNICATION CHANNELS', c5.recordset);

  // C6: Lifecycle stage x engagement
  const c6 = await pool.request().query(`
    SELECT p.lifecycle_stage, COUNT(*) people,
      CAST(AVG(CAST(p.tag_count AS FLOAT)) AS INT) avg_tags,
      CAST(AVG(CAST(p.note_count AS FLOAT)) AS INT) avg_notes,
      CAST(AVG(CAST(p.comm_count AS FLOAT)) AS INT) avg_comms,
      CAST(AVG(CAST(p.order_count AS FLOAT)) AS INT) avg_orders,
      CAST(AVG(p.lifetime_giving) AS INT) avg_giving
    FROM serving.person_360 p WHERE p.display_name<>'Unknown'
    GROUP BY p.lifecycle_stage ORDER BY COUNT(*) DESC`);
  tbl('C6. LIFECYCLE STAGE x ENGAGEMENT', c6.recordset);

  // C7: Top engagement zero giving
  const c7 = await pool.request().query(`
    SELECT TOP 20 display_name, tag_count, note_count, comm_count,
      order_count, total_spent, lifecycle_stage
    FROM serving.person_360
    WHERE lifetime_giving=0 AND display_name<>'Unknown'
    ORDER BY tag_count + note_count + comm_count DESC`);
  tbl('C7. HIGHEST ENGAGEMENT + ZERO GIVING (CONVERSION TARGETS)', c7.recordset);

  // C8: Program affinity segments
  const c8 = await pool.request().query(`
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
      WHEN tg=1 AND b2bb=1 AND donor=1 THEN 'TrueGirl + B2BB + Donor'
      WHEN tg=1 AND b2bb=1 THEN 'TrueGirl + B2BB'
      WHEN tg=1 AND donor=1 THEN 'TrueGirl + Donor'
      WHEN b2bb=1 AND donor=1 THEN 'B2BB + Donor'
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
      WHEN tg=1 AND b2bb=1 AND donor=1 THEN 'TrueGirl + B2BB + Donor'
      WHEN tg=1 AND b2bb=1 THEN 'TrueGirl + B2BB'
      WHEN tg=1 AND donor=1 THEN 'TrueGirl + Donor'
      WHEN b2bb=1 AND donor=1 THEN 'B2BB + Donor'
      WHEN tg=1 THEN 'TrueGirl Only'
      WHEN b2bb=1 THEN 'B2BB Only'
      WHEN donor=1 THEN 'Donor Only'
      WHEN customer=1 THEN 'Customer Only'
      WHEN nurture=1 THEN 'Nurture Only'
      WHEN box=1 THEN 'Box Tracking Only'
      ELSE 'Other' END ORDER BY COUNT(*) DESC`);
  tbl('C8. PROGRAM AFFINITY SEGMENTS', c8.recordset);

  // C9: Yearly giving trends
  const c9 = await pool.request().query(`
    SELECT donation_year, COUNT(DISTINCT person_id) donors,
      COUNT(*) gifts, CAST(SUM(amount) AS INT) total,
      CAST(AVG(amount) AS INT) avg_gift
    FROM serving.donation_detail
    WHERE donation_year>=2018
    GROUP BY donation_year ORDER BY donation_year`);
  tbl('C9. YEARLY GIVING TRENDS (2018+)', c9.recordset);

  // C10: Payment method breakdown
  const c10 = await pool.request().query(`
    SELECT payment_method, COUNT(*) gifts, COUNT(DISTINCT person_id) donors,
      CAST(SUM(amount) AS INT) total
    FROM serving.donation_detail WHERE payment_method IS NOT NULL
    GROUP BY payment_method ORDER BY SUM(amount) DESC`);
  tbl('C10. PAYMENT METHOD BREAKDOWN', c10.recordset);

  // C11: True Girl tag breakdown
  const c11 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(DISTINCT person_id) people
    FROM serving.tag_detail WHERE tag_group='True Girl'
    GROUP BY tag_value ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C11. TOP TRUE GIRL TAGS', c11.recordset);

  // C12: B2BB tag breakdown
  const c12 = await pool.request().query(`
    SELECT TOP 20 tag_value, COUNT(DISTINCT person_id) people
    FROM serving.tag_detail WHERE tag_group='B2BB'
    GROUP BY tag_value ORDER BY COUNT(DISTINCT person_id) DESC`);
  tbl('C12. TOP B2BB (BIBLE STUDY) TAGS', c12.recordset);

  await pool.close();
  console.log('\n\nALL QUERIES COMPLETED SUCCESSFULLY.');
}

main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
