/**
 * Step 1.5 — Build Serving Layer
 *
 * Materializes serving.person_360 and serving.household_360 by aggregating
 * all giving, subscriptions, events, engagement, and tags per person/household.
 * Computes lifecycle stage, RFM scores, and LTV estimates.
 *
 * Run: node scripts/ingest/05_build_serving.js
 */

const fs = require('fs');
const path = require('path');
const sql = require('mssql');

// ── env & db ────────────────────────────────────────────────────────────────
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
    database: process.env.SOZO_SQL_DB,
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 600000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 10, min: 0, idleTimeoutMillis: 10000 },
  };
}

// ── main ────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();
  console.log('Step 1.5 — Build Serving Layer');
  console.log('='.repeat(60));

  const pool = await sql.connect(getDbConfig());

  try {
    // ═══════════════════════════════════════════════════════════════════════
    // PERSON 360
    // ═══════════════════════════════════════════════════════════════════════
    const skipPerson = process.argv.includes('--skip-person');
    if (skipPerson) {
      const p360Exist = await pool.request().query('SELECT COUNT(*) AS cnt FROM serving.person_360');
      console.log(`\n[1] Skipping person_360 (already built: ${p360Exist.recordset[0].cnt.toLocaleString()} records)`);
    } else {
    console.log('\n[1] Building serving.person_360...');
    await pool.request().batch(`TRUNCATE TABLE serving.person_360`);

    // Insert base person data
    await pool.request().batch(`
      INSERT INTO serving.person_360 (
        person_id, display_name, first_name, last_name, email, phone,
        household_id, household_name, source_systems
      )
      SELECT
        p.id,
        p.display_name,
        p.first_name,
        p.last_name,
        (SELECT TOP 1 e.email FROM person.email e WHERE e.person_id = p.id AND e.is_primary = 1),
        (SELECT TOP 1 ph.phone_display FROM person.phone ph WHERE ph.person_id = p.id AND ph.is_primary = 1),
        hm.household_id,
        hu.name,
        (
          SELECT STRING_AGG(ss.display_name, ', ')
          FROM (
            SELECT DISTINCT sl.source_id
            FROM person.source_link sl WHERE sl.person_id = p.id
          ) src
          JOIN meta.source_system ss ON ss.source_id = src.source_id
        )
      FROM person.profile p
      LEFT JOIN household.member hm ON hm.person_id = p.id
      LEFT JOIN household.unit hu ON hu.id = hm.household_id
    `);

    const p360Count = await pool.request().query('SELECT COUNT(*) AS cnt FROM serving.person_360');
    console.log(`  Base records: ${p360Count.recordset[0].cnt.toLocaleString()}`);

    // Update giving metrics
    console.log('  Computing giving metrics...');
    await pool.request().batch(`
      ;WITH giving_agg AS (
        SELECT
          d.person_id,
          COUNT(*) AS donation_count,
          SUM(d.amount) AS lifetime_giving,
          AVG(d.amount) AS avg_gift,
          MIN(d.donated_at) AS first_gift_date,
          MAX(d.donated_at) AS last_gift_date,
          MAX(d.amount) AS largest_gift,
          DATEDIFF(DAY, MAX(d.donated_at), SYSUTCDATETIME()) AS recency_days
        FROM giving.donation d
        WHERE d.person_id IS NOT NULL AND d.amount > 0
        GROUP BY d.person_id
      )
      UPDATE s
      SET
        s.donation_count = g.donation_count,
        s.lifetime_giving = g.lifetime_giving,
        s.avg_gift = g.avg_gift,
        s.first_gift_date = g.first_gift_date,
        s.last_gift_date = g.last_gift_date,
        s.largest_gift = g.largest_gift,
        s.recency_days = g.recency_days
      FROM serving.person_360 s
      JOIN giving_agg g ON g.person_id = s.person_id
    `);

    // Compute annual frequency + monetary (last 12 months)
    await pool.request().batch(`
      ;WITH annual AS (
        SELECT
          d.person_id,
          COUNT(*) AS freq,
          SUM(d.amount) AS monetary
        FROM giving.donation d
        WHERE d.person_id IS NOT NULL
          AND d.amount > 0
          AND d.donated_at >= DATEADD(YEAR, -1, SYSUTCDATETIME())
        GROUP BY d.person_id
      )
      UPDATE s
      SET
        s.frequency_annual = a.freq,
        s.monetary_annual = a.monetary
      FROM serving.person_360 s
      JOIN annual a ON a.person_id = s.person_id
    `);

    // Update subscription metrics
    console.log('  Computing subscription metrics...');
    await pool.request().batch(`
      ;WITH sub_agg AS (
        SELECT
          sub.person_id,
          SUM(CASE WHEN sub.status IN ('active', 'Active') THEN 1 ELSE 0 END) AS active_subs,
          SUM(
            CASE WHEN sub.start_date IS NOT NULL
              THEN DATEDIFF(MONTH, sub.start_date, ISNULL(sub.next_renewal, SYSUTCDATETIME()))
              ELSE 0
            END
          ) AS sub_months
        FROM commerce.subscription sub
        WHERE sub.person_id IS NOT NULL
        GROUP BY sub.person_id
      )
      UPDATE s
      SET
        s.active_subscriptions = sa.active_subs,
        s.subscription_months = sa.sub_months
      FROM serving.person_360 s
      JOIN sub_agg sa ON sa.person_id = s.person_id
    `);

    // Update engagement metrics
    console.log('  Computing engagement metrics...');
    await pool.request().batch(`
      ;WITH eng_agg AS (
        SELECT person_id, COUNT(*) AS cnt, MAX(occurred_at) AS last_eng
        FROM (
          SELECT person_id, created_at AS occurred_at FROM engagement.note WHERE person_id IS NOT NULL
          UNION ALL
          SELECT person_id, sent_at FROM engagement.communication WHERE person_id IS NOT NULL
          UNION ALL
          SELECT person_id, occurred_at FROM engagement.activity WHERE person_id IS NOT NULL
        ) x
        GROUP BY person_id
      )
      UPDATE s
      SET s.engagement_count = ea.cnt, s.last_engagement = ea.last_eng
      FROM serving.person_360 s
      JOIN eng_agg ea ON ea.person_id = s.person_id
    `);

    // Tag count
    await pool.request().batch(`
      ;WITH tag_agg AS (
        SELECT person_id, COUNT(*) AS cnt
        FROM engagement.tag WHERE person_id IS NOT NULL
        GROUP BY person_id
      )
      UPDATE s
      SET s.tag_count = ta.cnt
      FROM serving.person_360 s
      JOIN tag_agg ta ON ta.person_id = s.person_id
    `);

    // Lifecycle stage
    console.log('  Computing lifecycle stages...');
    await pool.request().batch(`
      UPDATE serving.person_360
      SET lifecycle_stage = CASE
        WHEN donation_count = 0 OR donation_count IS NULL THEN 'prospect'
        WHEN donation_count = 1 AND recency_days <= 90 THEN 'new'
        WHEN recency_days <= 90 THEN 'active'
        WHEN recency_days <= 180 THEN 'cooling'
        WHEN recency_days <= 365 THEN 'lapsed'
        ELSE 'lost'
      END
    `);

    // Churn risk (simple heuristic based on recency + frequency)
    await pool.request().batch(`
      UPDATE serving.person_360
      SET churn_risk = CASE
        WHEN lifecycle_stage = 'lost' THEN 0.95
        WHEN lifecycle_stage = 'lapsed' THEN 0.75
        WHEN lifecycle_stage = 'cooling' THEN 0.50
        WHEN lifecycle_stage = 'new' THEN 0.30
        WHEN lifecycle_stage = 'active' AND frequency_annual >= 4 THEN 0.05
        WHEN lifecycle_stage = 'active' AND frequency_annual >= 2 THEN 0.10
        WHEN lifecycle_stage = 'active' THEN 0.20
        ELSE 0.50
      END
    `);

    // LTV estimate (simple: avg_gift * frequency_annual * 3 years)
    await pool.request().batch(`
      UPDATE serving.person_360
      SET ltv_estimate = CASE
        WHEN frequency_annual > 0 AND avg_gift > 0
          THEN avg_gift * frequency_annual * 3
        WHEN lifetime_giving > 0
          THEN lifetime_giving * 0.5
        ELSE 0
      END
    `);

    // Fill NULLs with fallback email/phone from non-primary records
    await pool.request().batch(`
      UPDATE s
      SET s.email = e.email
      FROM serving.person_360 s
      CROSS APPLY (SELECT TOP 1 email FROM person.email WHERE person_id = s.person_id) e
      WHERE s.email IS NULL
    `);
    await pool.request().batch(`
      UPDATE s
      SET s.phone = ph.phone_display
      FROM serving.person_360 s
      CROSS APPLY (SELECT TOP 1 phone_display FROM person.phone WHERE person_id = s.person_id) ph
      WHERE s.phone IS NULL
    `);

    // Stats
    const lifecycleDist = await pool.request().query(`
      SELECT lifecycle_stage, COUNT(*) AS cnt
      FROM serving.person_360
      GROUP BY lifecycle_stage
      ORDER BY cnt DESC
    `);
    console.log('\n  Lifecycle distribution:');
    for (const r of lifecycleDist.recordset) {
      console.log(`    ${r.lifecycle_stage || 'null'}: ${r.cnt.toLocaleString()}`);
    }

    const givingStats = await pool.request().query(`
      SELECT
        SUM(lifetime_giving) AS total_giving,
        COUNT(CASE WHEN donation_count > 0 THEN 1 END) AS donors,
        AVG(CASE WHEN donation_count > 0 THEN avg_gift END) AS avg_avg_gift
      FROM serving.person_360
    `);
    const gs = givingStats.recordset[0];
    console.log(`\n  Total lifetime giving: $${(gs.total_giving || 0).toLocaleString(undefined, {minimumFractionDigits: 2})}`);
    console.log(`  Donors with gifts: ${(gs.donors || 0).toLocaleString()}`);
    console.log(`  Average gift size: $${(gs.avg_avg_gift || 0).toFixed(2)}`);
    } // end if !skipPerson

    // ═══════════════════════════════════════════════════════════════════════
    // HOUSEHOLD 360
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[2] Building serving.household_360...');
    await pool.request().batch(`TRUNCATE TABLE serving.household_360`);

    // Step 2a: Insert base records with member count (lightweight)
    console.log('  2a. Base records + member count...');
    await pool.request().batch(`
      INSERT INTO serving.household_360 (household_id, name, member_count)
      SELECT hu.id, hu.name, COUNT(hm.person_id)
      FROM household.unit hu
      LEFT JOIN household.member hm ON hm.household_id = hu.id
      GROUP BY hu.id, hu.name
    `);
    const hh360Base = await pool.request().query('SELECT COUNT(*) AS cnt FROM serving.household_360');
    console.log(`    Inserted: ${hh360Base.recordset[0].cnt.toLocaleString()}`);

    // Step 2b: Aggregate giving/subs/events/health from person_360 via household.member
    console.log('  2b. Giving + subscription + health metrics...');
    await pool.request().batch(`
      ;WITH hh_agg AS (
        SELECT
          hm.household_id,
          ISNULL(SUM(p.lifetime_giving), 0) AS giving_total,
          ISNULL(SUM(p.monetary_annual), 0) AS annual_giving,
          ISNULL(SUM(p.active_subscriptions), 0) AS active_subs,
          ISNULL(SUM(p.events_attended), 0) AS events_attended,
          ISNULL(AVG(1.0 - p.churn_risk), 0.50) AS health_score
        FROM household.member hm
        JOIN serving.person_360 p ON p.person_id = hm.person_id
        GROUP BY hm.household_id
      )
      UPDATE h
      SET
        h.household_giving_total = a.giving_total,
        h.household_annual_giving = a.annual_giving,
        h.active_subs = a.active_subs,
        h.events_attended = a.events_attended,
        h.health_score = a.health_score
      FROM serving.household_360 h
      JOIN hh_agg a ON a.household_id = h.household_id
    `);

    // Step 2c: Members JSON (separate, lighter query)
    console.log('  2c. Members JSON...');
    await pool.request().batch(`
      UPDATE h
      SET h.members_json = m.json_data
      FROM serving.household_360 h
      CROSS APPLY (
        SELECT
          p360.person_id AS id,
          p360.display_name AS name,
          p360.email,
          p360.lifecycle_stage,
          p360.lifetime_giving
        FROM household.member hm
        JOIN serving.person_360 p360 ON p360.person_id = hm.person_id
        WHERE hm.household_id = h.household_id
        FOR JSON PATH
      ) m(json_data)
    `);

    // Giving trend
    await pool.request().batch(`
      UPDATE h
      SET h.giving_trend = CASE
        WHEN h.household_annual_giving > h.household_giving_total * 0.5 THEN 'growing'
        WHEN h.household_annual_giving > h.household_giving_total * 0.25 THEN 'stable'
        WHEN h.household_giving_total > 0 THEN 'declining'
        ELSE 'none'
      END
      FROM serving.household_360 h
    `);

    // Best contact method
    await pool.request().batch(`
      UPDATE h
      SET h.best_contact_method = CASE
        WHEN EXISTS (
          SELECT 1 FROM household.member hm
          JOIN person.email pe ON pe.person_id = hm.person_id
          WHERE hm.household_id = h.household_id
        ) THEN 'email'
        WHEN EXISTS (
          SELECT 1 FROM household.member hm
          JOIN person.phone pp ON pp.person_id = hm.person_id
          WHERE hm.household_id = h.household_id
        ) THEN 'phone'
        ELSE 'mail'
      END
      FROM serving.household_360 h
    `);

    const hh360Count = await pool.request().query('SELECT COUNT(*) AS cnt FROM serving.household_360');
    console.log(`  Household 360 records: ${hh360Count.recordset[0].cnt.toLocaleString()}`);

    const hhTrendDist = await pool.request().query(`
      SELECT giving_trend, COUNT(*) AS cnt FROM serving.household_360 GROUP BY giving_trend ORDER BY cnt DESC
    `);
    console.log('\n  Household giving trends:');
    for (const r of hhTrendDist.recordset) {
      console.log(`    ${r.giving_trend}: ${r.cnt.toLocaleString()}`);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // FINAL SUMMARY
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n' + '='.repeat(60));
    console.log('Serving Layer Summary:');
    const finalP360 = await pool.request().query('SELECT COUNT(*) AS cnt FROM serving.person_360');
    console.log(`  Person 360 records:    ${finalP360.recordset[0].cnt.toLocaleString()}`);
    console.log(`  Household 360 records: ${hh360Count.recordset[0].cnt.toLocaleString()}`);

  } finally {
    await pool.close();
  }

  console.log('\nDone.');
}

main().catch(err => {
  console.error('FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
