/**
 * Create intelligence semantic views for Sozo.
 * These views pre-compute donor health, person value, capacity gaps,
 * retention funnels, event ROI, and more — using Sozo's actual serving.* tables.
 *
 * Usage: node scripts/pipeline/create_intel_views.js
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

  console.log('Creating intel semantic views...\n');

  // ── 1. vw_donor_health ─────────────────────────────────────────────────
  // RFM scoring + engagement depth + growth trajectory + risk level per person
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_donor_health AS
    WITH rfm AS (
      SELECT
        ds.person_id,
        ds.display_name,
        ds.total_given,
        ds.donation_count,
        ds.avg_gift,
        ds.first_gift_date,
        ds.last_gift_date,
        ds.days_since_last,
        ds.lifecycle_stage,
        ds.active_months,
        -- Recency score (1-5): lower days_since = higher score
        CASE
          WHEN ds.days_since_last <= 90  THEN 5
          WHEN ds.days_since_last <= 180 THEN 4
          WHEN ds.days_since_last <= 365 THEN 3
          WHEN ds.days_since_last <= 730 THEN 2
          ELSE 1
        END AS recency_score,
        -- Frequency score (1-5): based on donations per year active
        CASE
          WHEN ds.active_months < 1 THEN 1
          WHEN ds.donation_count * 1.0 / CEILING(ds.active_months / 12.0) >= 12 THEN 5
          WHEN ds.donation_count * 1.0 / CEILING(ds.active_months / 12.0) >= 6  THEN 4
          WHEN ds.donation_count * 1.0 / CEILING(ds.active_months / 12.0) >= 3  THEN 3
          WHEN ds.donation_count * 1.0 / CEILING(ds.active_months / 12.0) >= 1  THEN 2
          ELSE 1
        END AS frequency_score,
        -- Monetary score (1-5): based on average gift
        CASE
          WHEN ds.avg_gift >= 1000 THEN 5
          WHEN ds.avg_gift >= 500  THEN 4
          WHEN ds.avg_gift >= 100  THEN 3
          WHEN ds.avg_gift >= 50   THEN 2
          ELSE 1
        END AS monetary_score
      FROM serving.donor_summary ds
      WHERE ds.display_name <> 'Unknown'
    ),
    engagement AS (
      SELECT
        r.person_id,
        -- Count distinct channels active in
        (CASE WHEN r.donation_count > 0 THEN 1 ELSE 0 END
         + CASE WHEN ISNULL(ord.order_count, 0) > 0 THEN 1 ELSE 0 END
         + CASE WHEN ISNULL(evt.event_count, 0) > 0 THEN 1 ELSE 0 END
         + CASE WHEN ISNULL(sub.has_subscription, 0) > 0 THEN 1 ELSE 0 END) AS channel_count,
        ISNULL(ord.order_count, 0) AS order_count,
        ISNULL(evt.event_count, 0) AS event_count,
        ISNULL(sub.has_subscription, 0) AS has_active_subscription
      FROM rfm r
      LEFT JOIN (
        SELECT person_id, COUNT(*) AS order_count
        FROM serving.order_detail GROUP BY person_id
      ) ord ON ord.person_id = r.person_id
      LEFT JOIN (
        SELECT person_id, COUNT(*) AS event_count
        FROM serving.event_detail GROUP BY person_id
      ) evt ON evt.person_id = r.person_id
      LEFT JOIN (
        SELECT person_id, 1 AS has_subscription
        FROM serving.subscription_detail
        WHERE source_system = 'subbly' AND subscription_status = 'Active'
        GROUP BY person_id
      ) sub ON sub.person_id = r.person_id
    ),
    yoy AS (
      -- Year-over-year giving direction
      SELECT
        dm.person_id,
        SUM(CASE WHEN dm.donation_year = YEAR(GETDATE())     THEN dm.amount ELSE 0 END) AS giving_this_year,
        SUM(CASE WHEN dm.donation_year = YEAR(GETDATE()) - 1 THEN dm.amount ELSE 0 END) AS giving_last_year
      FROM serving.donor_monthly dm
      GROUP BY dm.person_id
    )
    SELECT
      r.person_id,
      r.display_name,
      r.total_given,
      r.donation_count,
      r.avg_gift,
      r.first_gift_date,
      r.last_gift_date,
      r.days_since_last,
      r.lifecycle_stage,
      r.recency_score,
      r.frequency_score,
      r.monetary_score,
      -- Composite RFM score (weighted)
      CAST(ROUND(r.recency_score * 0.4 + r.frequency_score * 0.3 + r.monetary_score * 0.3, 2) AS DECIMAL(5,2)) AS rfm_score,
      -- Engagement depth
      e.channel_count,
      e.order_count,
      e.event_count,
      e.has_active_subscription,
      CASE
        WHEN e.channel_count >= 3 THEN 'Deep'
        WHEN e.channel_count = 2  THEN 'Moderate'
        ELSE 'Shallow'
      END AS engagement_depth,
      -- Growth trajectory
      ISNULL(y.giving_this_year, 0)  AS giving_this_year,
      ISNULL(y.giving_last_year, 0)  AS giving_last_year,
      CASE
        WHEN ISNULL(y.giving_last_year, 0) = 0 AND ISNULL(y.giving_this_year, 0) > 0 THEN 'New'
        WHEN ISNULL(y.giving_last_year, 0) = 0 THEN 'Inactive'
        WHEN ISNULL(y.giving_this_year, 0) >= y.giving_last_year * 1.1 THEN 'Growing'
        WHEN ISNULL(y.giving_this_year, 0) >= y.giving_last_year * 0.9 THEN 'Stable'
        ELSE 'Declining'
      END AS growth_trajectory,
      -- Risk level
      CASE
        WHEN r.lifecycle_stage IN ('lapsed','lost') AND r.total_given >= 10000 THEN 'Critical'
        WHEN r.lifecycle_stage = 'cooling' AND r.total_given >= 5000 THEN 'High'
        WHEN r.lifecycle_stage = 'cooling' THEN 'Medium'
        WHEN r.lifecycle_stage = 'active' AND ISNULL(y.giving_this_year,0) < ISNULL(y.giving_last_year,0) * 0.5 THEN 'Medium'
        ELSE 'Low'
      END AS risk_level,
      -- Dollars at risk
      CASE
        WHEN r.lifecycle_stage IN ('cooling','lapsed','lost')
        THEN CAST(r.total_given / NULLIF(CEILING(DATEDIFF(MONTH, r.first_gift_date, GETDATE()) / 12.0), 0) AS DECIMAL(12,2))
        ELSE 0
      END AS annual_revenue_at_risk
    FROM rfm r
    LEFT JOIN engagement e ON e.person_id = r.person_id
    LEFT JOIN yoy y ON y.person_id = r.person_id
  `);
  console.log('  intel.vw_donor_health OK');

  // ── 2. vw_person_value ─────────────────────────────────────────────────
  // Ministry lifetime value combining giving + commerce + events + subscriptions
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_person_value AS
    SELECT
      ds.person_id,
      ds.display_name,
      CAST(ISNULL(ds.total_given, 0) AS DECIMAL(14,2)) AS lifetime_giving,
      CAST(ISNULL(ord.total_commerce, 0) AS DECIMAL(14,2)) AS lifetime_commerce,
      CAST(ISNULL(evt.total_events, 0) AS DECIMAL(14,2)) AS lifetime_events,
      CAST(ISNULL(ds.total_given, 0) + ISNULL(ord.total_commerce, 0) + ISNULL(evt.total_events, 0) AS DECIMAL(14,2)) AS ministry_ltv,
      ds.donation_count,
      ISNULL(ord.order_count, 0) AS order_count,
      ISNULL(evt.event_count, 0) AS event_count,
      CASE WHEN sub.person_id IS NOT NULL THEN 1 ELSE 0 END AS has_active_subscription,
      (CASE WHEN ds.donation_count > 0 THEN 1 ELSE 0 END
       + CASE WHEN ISNULL(ord.order_count, 0) > 0 THEN 1 ELSE 0 END
       + CASE WHEN ISNULL(evt.event_count, 0) > 0 THEN 1 ELSE 0 END
       + CASE WHEN sub.person_id IS NOT NULL THEN 1 ELSE 0 END) AS channel_count,
      ds.lifecycle_stage,
      ds.first_gift_date,
      ds.last_gift_date
    FROM serving.donor_summary ds
    LEFT JOIN (
      SELECT person_id, SUM(CAST(total_amount AS DECIMAL(14,2))) AS total_commerce, COUNT(*) AS order_count
      FROM serving.order_detail GROUP BY person_id
    ) ord ON ord.person_id = ds.person_id
    LEFT JOIN (
      SELECT person_id, SUM(CAST(order_total AS DECIMAL(14,2))) AS total_events, COUNT(*) AS event_count
      FROM serving.event_detail GROUP BY person_id
    ) evt ON evt.person_id = ds.person_id
    LEFT JOIN (
      SELECT DISTINCT person_id
      FROM serving.subscription_detail
      WHERE source_system = 'subbly' AND subscription_status = 'Active'
    ) sub ON sub.person_id = ds.person_id
    WHERE ds.display_name <> 'Unknown'
  `);
  console.log('  intel.vw_person_value OK');

  // ── 3. vw_capacity_gap ─────────────────────────────────────────────────
  // Annual giving vs annual capacity from wealth screening
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_capacity_gap AS
    SELECT
      ws.person_id,
      ws.display_name,
      ws.capacity_label AS tier,
      CAST(ws.giving_capacity AS DECIMAL(14,2)) AS annual_capacity,
      CAST(ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH, ds.first_gift_date, GETDATE()) / 12.0), 0), 0) AS DECIMAL(14,2)) AS avg_annual_giving,
      CAST(ws.giving_capacity - ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH, ds.first_gift_date, GETDATE()) / 12.0), 0), 0) AS DECIMAL(14,2)) AS annual_gap,
      CAST(ROUND(ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH, ds.first_gift_date, GETDATE()) / 12.0), 0), 0) / NULLIF(ws.giving_capacity, 0) * 100, 1) AS DECIMAL(5,1)) AS pct_utilized,
      ISNULL(ds.total_given, 0) AS lifetime_giving,
      ds.donation_count,
      ds.last_gift_date,
      ds.lifecycle_stage,
      CASE
        WHEN ISNULL(ds.total_given, 0) = 0 THEN 'Never Donated'
        WHEN ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH, ds.first_gift_date, GETDATE()) / 12.0), 0), 0) < ws.giving_capacity * 0.1 THEN 'Severely Undertapped'
        WHEN ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH, ds.first_gift_date, GETDATE()) / 12.0), 0), 0) < ws.giving_capacity * 0.25 THEN 'Undertapped'
        WHEN ISNULL(ds.total_given / NULLIF(CEILING(DATEDIFF(MONTH, ds.first_gift_date, GETDATE()) / 12.0), 0), 0) < ws.giving_capacity * 0.5 THEN 'Moderate'
        ELSE 'Well Utilized'
      END AS utilization_band
    FROM serving.wealth_screening ws
    LEFT JOIN serving.donor_summary ds ON ds.person_id = ws.person_id
    WHERE ws.display_name <> 'Unknown'
  `);
  console.log('  intel.vw_capacity_gap OK');

  // ── 4. vw_donor_retention_funnel ───────────────────────────────────────
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_donor_retention_funnel AS
    SELECT
      lifecycle_stage AS stage,
      COUNT(*) AS donor_count,
      SUM(CAST(total_given AS DECIMAL(14,2))) AS total_giving,
      AVG(CAST(total_given AS DECIMAL(14,2))) AS avg_giving,
      AVG(CAST(days_since_last AS DECIMAL(10,0))) AS avg_days_silent,
      -- Stage ordering for funnel
      CASE lifecycle_stage
        WHEN 'active'  THEN 1
        WHEN 'cooling' THEN 2
        WHEN 'lapsed'  THEN 3
        WHEN 'lost'    THEN 4
        ELSE 5
      END AS stage_order
    FROM serving.donor_summary
    WHERE display_name <> 'Unknown'
    GROUP BY lifecycle_stage
  `);
  console.log('  intel.vw_donor_retention_funnel OK');

  // ── 5. vw_event_giving_lift ────────────────────────────────────────────
  // Compare giving behavior of event attendees vs non-attendees
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_event_giving_lift AS
    WITH attendees AS (
      SELECT DISTINCT person_id
      FROM serving.event_detail
    ),
    giving AS (
      SELECT
        ds.person_id,
        CASE WHEN a.person_id IS NOT NULL THEN 'Event Attendee' ELSE 'Non-Attendee' END AS segment,
        ds.total_given,
        ds.donation_count,
        ds.avg_gift,
        ds.lifecycle_stage
      FROM serving.donor_summary ds
      LEFT JOIN attendees a ON a.person_id = ds.person_id
      WHERE ds.display_name <> 'Unknown'
    )
    SELECT
      segment,
      COUNT(*) AS donor_count,
      SUM(CAST(total_given AS DECIMAL(14,2))) AS total_giving,
      AVG(CAST(total_given AS DECIMAL(14,2))) AS avg_giving,
      AVG(CAST(donation_count AS DECIMAL(10,0))) AS avg_gifts,
      AVG(CAST(avg_gift AS DECIMAL(12,2))) AS avg_gift_size,
      SUM(CASE WHEN lifecycle_stage = 'active' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0) AS pct_active
    FROM giving
    GROUP BY segment
  `);
  console.log('  intel.vw_event_giving_lift OK');

  // ── 6. vw_subscription_donor_conversion ────────────────────────────────
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_subscription_donor_conversion AS
    WITH subscribers AS (
      SELECT DISTINCT person_id
      FROM serving.subscription_detail
      WHERE source_system = 'subbly'
    )
    SELECT
      CASE WHEN ds.person_id IS NOT NULL THEN 'Also Donor' ELSE 'Subscriber Only' END AS segment,
      COUNT(*) AS contact_count,
      ISNULL(SUM(CAST(ds.total_given AS DECIMAL(14,2))), 0) AS total_giving,
      ISNULL(AVG(CAST(ds.total_given AS DECIMAL(14,2))), 0) AS avg_giving
    FROM subscribers s
    LEFT JOIN serving.donor_summary ds ON ds.person_id = s.person_id AND ds.display_name <> 'Unknown'
    GROUP BY CASE WHEN ds.person_id IS NOT NULL THEN 'Also Donor' ELSE 'Subscriber Only' END
  `);
  console.log('  intel.vw_subscription_donor_conversion OK');

  // ── 7. vw_cross_channel_engagement ─────────────────────────────────────
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_cross_channel_engagement AS
    WITH channels AS (
      SELECT person_id, 'Giving' AS channel FROM serving.donor_summary WHERE donation_count > 0
      UNION
      SELECT person_id, 'Commerce' AS channel FROM serving.order_detail GROUP BY person_id
      UNION
      SELECT person_id, 'Events' AS channel FROM serving.event_detail GROUP BY person_id
      UNION
      SELECT person_id, 'Subscription' AS channel FROM serving.subscription_detail
        WHERE source_system = 'subbly' AND subscription_status = 'Active' GROUP BY person_id
    ),
    person_channels AS (
      SELECT person_id, COUNT(*) AS channel_count, STRING_AGG(channel, ', ') WITHIN GROUP (ORDER BY channel) AS channels
      FROM channels GROUP BY person_id
    )
    SELECT
      channel_count,
      COUNT(*) AS contact_count,
      channels AS channel_combo
    FROM person_channels
    GROUP BY channel_count, channels
  `);
  console.log('  intel.vw_cross_channel_engagement OK');

  // ── 8. vw_giving_pace ──────────────────────────────────────────────────
  // Current year vs last year monthly pace
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_giving_pace AS
    SELECT
      MONTH(dd.donated_at) AS gift_month,
      SUM(CASE WHEN YEAR(dd.donated_at) = YEAR(GETDATE()) THEN CAST(dd.amount AS DECIMAL(14,2)) ELSE 0 END) AS this_year,
      SUM(CASE WHEN YEAR(dd.donated_at) = YEAR(GETDATE()) - 1 THEN CAST(dd.amount AS DECIMAL(14,2)) ELSE 0 END) AS last_year,
      SUM(CASE WHEN YEAR(dd.donated_at) = YEAR(GETDATE()) THEN 1 ELSE 0 END) AS gifts_this_year,
      SUM(CASE WHEN YEAR(dd.donated_at) = YEAR(GETDATE()) - 1 THEN 1 ELSE 0 END) AS gifts_last_year
    FROM serving.donation_detail dd
    WHERE YEAR(dd.donated_at) >= YEAR(GETDATE()) - 1
    GROUP BY MONTH(dd.donated_at)
  `);
  console.log('  intel.vw_giving_pace OK');

  // ── 9. vw_concentration_risk ───────────────────────────────────────────
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_concentration_risk AS
    WITH ranked AS (
      SELECT
        person_id,
        display_name,
        total_given,
        ROW_NUMBER() OVER (ORDER BY total_given DESC) AS rank_num,
        SUM(CAST(total_given AS DECIMAL(14,2))) OVER () AS grand_total
      FROM serving.donor_summary
      WHERE display_name <> 'Unknown' AND total_given > 0
    )
    SELECT
      rank_num,
      display_name,
      CAST(total_given AS DECIMAL(14,2)) AS total_given,
      CAST(total_given / NULLIF(grand_total, 0) * 100 AS DECIMAL(5,2)) AS pct_of_total,
      CAST(SUM(CAST(total_given AS DECIMAL(14,2))) OVER (ORDER BY rank_num) / NULLIF(grand_total, 0) * 100 AS DECIMAL(5,2)) AS cumulative_pct
    FROM ranked
  `);
  console.log('  intel.vw_concentration_risk OK');

  // ── 10. vw_lybunt ──────────────────────────────────────────────────────
  // Gave Last Year But Unfortunately Not This year
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_lybunt AS
    WITH gave_last_year AS (
      SELECT DISTINCT person_id
      FROM serving.donation_detail
      WHERE YEAR(donated_at) = YEAR(GETDATE()) - 1
    ),
    gave_this_year AS (
      SELECT DISTINCT person_id
      FROM serving.donation_detail
      WHERE YEAR(donated_at) = YEAR(GETDATE())
    )
    SELECT
      ds.person_id,
      ds.display_name,
      ds.total_given,
      ds.avg_gift,
      ds.last_gift_date,
      ds.days_since_last,
      ds.lifecycle_stage,
      ws.capacity_label AS tier,
      ws.giving_capacity AS annual_capacity,
      -- What they gave last year
      ly.ly_total
    FROM gave_last_year gly
    INNER JOIN serving.donor_summary ds ON ds.person_id = gly.person_id
    LEFT JOIN gave_this_year gty ON gty.person_id = gly.person_id
    LEFT JOIN serving.wealth_screening ws ON ws.person_id = gly.person_id
    LEFT JOIN (
      SELECT person_id, SUM(CAST(amount AS DECIMAL(14,2))) AS ly_total
      FROM serving.donation_detail
      WHERE YEAR(donated_at) = YEAR(GETDATE()) - 1
      GROUP BY person_id
    ) ly ON ly.person_id = gly.person_id
    WHERE gty.person_id IS NULL  -- NOT given this year
      AND ds.display_name <> 'Unknown'
  `);
  console.log('  intel.vw_lybunt OK');

  // ── 11. vw_sybunt ─────────────────────────────────────────────────────
  // Gave Some Year But Unfortunately Not This year
  await pool.request().query(`
    CREATE OR ALTER VIEW intel.vw_sybunt AS
    WITH gave_this_year AS (
      SELECT DISTINCT person_id
      FROM serving.donation_detail
      WHERE YEAR(donated_at) = YEAR(GETDATE())
    )
    SELECT
      ds.person_id,
      ds.display_name,
      ds.total_given,
      ds.avg_gift,
      ds.last_gift_date,
      ds.days_since_last,
      ds.lifecycle_stage,
      ds.donation_count,
      ws.capacity_label AS tier,
      ws.giving_capacity AS annual_capacity
    FROM serving.donor_summary ds
    LEFT JOIN gave_this_year gty ON gty.person_id = ds.person_id
    LEFT JOIN serving.wealth_screening ws ON ws.person_id = ds.person_id
    WHERE gty.person_id IS NULL  -- NOT given this year
      AND ds.total_given > 0     -- but gave at some point
      AND ds.display_name <> 'Unknown'
  `);
  console.log('  intel.vw_sybunt OK');

  console.log('\nAll intel views created successfully.');
  await pool.close();
}

main().catch(err => { console.error(err); process.exit(1); });
