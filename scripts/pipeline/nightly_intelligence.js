/**
 * Nightly Intelligence Job for Sozo.
 *
 * Three phases:
 * 1. Metric Snapshots — compute all certified metrics, store in intel.metric_snapshot
 * 2. Anomaly Detection — detect spikes/drops using robust z-score vs prior snapshots
 * 3. Donor Risk Scoring — score donors using RFM + engagement + capacity signals
 *
 * Usage: node scripts/pipeline/nightly_intelligence.js
 * Schedule: Run nightly via cron or Azure Functions timer trigger.
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

// ── Statistical helpers ─────────────────────────────────────────────────────
function median(arr) {
  if (arr.length === 0) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function mad(arr, med) {
  return median(arr.map(x => Math.abs(x - med)));
}

function robustZ(x, med, madVal) {
  const eps = 1e-9;
  return 0.6745 * (x - med) / (madVal + eps);
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

  const today = new Date().toISOString().slice(0, 10);
  console.log(`\n=== Sozo Nightly Intelligence — ${today} ===\n`);

  // ══════════════════════════════════════════════════════════════════════════
  // PHASE 1: METRIC SNAPSHOTS
  // ══════════════════════════════════════════════════════════════════════════
  console.log('─── Phase 1: Metric Snapshots ───');

  // Load all active certified metrics
  const metricsResult = await pool.request().query(`
    SELECT metric_key, display_name, sql_expression, default_time_window
    FROM intel.metric_definition
    WHERE is_active = 1 AND is_certified = 1
  `);
  const metrics = metricsResult.recordset;
  console.log(`  Found ${metrics.length} certified metrics`);

  let snapshotCount = 0;
  let snapshotErrors = 0;

  for (const metric of metrics) {
    try {
      // Substitute date parameters
      let metricSql = metric.sql_expression;

      // Determine time window — use default or last_12_months
      const window = metric.default_time_window || 'last_12_months';
      let startDate = null;
      let endDate = today;

      if (window === 'last_30_days') {
        const d = new Date(); d.setDate(d.getDate() - 30);
        startDate = d.toISOString().slice(0, 10);
      } else if (window === 'last_12_months') {
        const d = new Date(); d.setFullYear(d.getFullYear() - 1);
        startDate = d.toISOString().slice(0, 10);
      } else if (window === 'ytd') {
        startDate = `${new Date().getFullYear()}-01-01`;
      } else if (window === 'last_90_days') {
        const d = new Date(); d.setDate(d.getDate() - 90);
        startDate = d.toISOString().slice(0, 10);
      }
      // all_time and as_of: leave startDate as null

      metricSql = metricSql
        .replace(/@start_date/g, startDate ? `'${startDate}'` : 'NULL')
        .replace(/@end_date/g, endDate ? `'${endDate}'` : 'NULL')
        .replace(/@as_of_date/g, `'${today}'`);

      // Execute the metric SQL
      const result = await pool.request().query(metricSql);
      const row = result.recordset[0];
      if (!row) {
        console.log(`  SKIP ${metric.metric_key} — no result`);
        continue;
      }

      let value = row.value != null ? Number(row.value) : null;
      if (value == null || !Number.isFinite(value)) {
        // SUM/AVG return NULL when no rows match — treat as 0
        value = 0;
      }

      // Get prior snapshot for delta calculation (30 days ago)
      const priorResult = await pool.request()
        .input('mk', sql.NVarChar, metric.metric_key)
        .query(`
          SELECT TOP 1 value_decimal
          FROM intel.metric_snapshot
          WHERE metric_key = @mk
            AND segment_key IS NULL
            AND as_of_date < CAST(GETDATE() AS DATE)
          ORDER BY as_of_date DESC
        `);
      const priorValue = priorResult.recordset[0]?.value_decimal;
      const deltaPct = (priorValue != null && priorValue !== 0)
        ? ((value - Number(priorValue)) / Math.abs(Number(priorValue))) * 100
        : null;

      // Upsert snapshot
      await pool.request()
        .input('mk', sql.NVarChar, metric.metric_key)
        .input('asOf', sql.Date, today)
        .input('startD', sql.Date, startDate)
        .input('endD', sql.Date, endDate)
        .input('val', sql.Decimal(18, 4), value)
        .input('prior', sql.Decimal(18, 4), priorValue ?? null)
        .input('delta', sql.Decimal(10, 4), deltaPct)
        .query(`
          IF EXISTS (
            SELECT 1 FROM intel.metric_snapshot
            WHERE metric_key = @mk AND as_of_date = @asOf AND segment_key IS NULL
          )
            UPDATE intel.metric_snapshot
            SET value_decimal = @val, prior_value_decimal = @prior, delta_pct = @delta,
                start_date = @startD, end_date = @endD, computed_at = SYSUTCDATETIME()
            WHERE metric_key = @mk AND as_of_date = @asOf AND segment_key IS NULL
          ELSE
            INSERT INTO intel.metric_snapshot
              (metric_key, as_of_date, start_date, end_date, value_decimal, prior_value_decimal, delta_pct)
            VALUES (@mk, @asOf, @startD, @endD, @val, @prior, @delta)
        `);

      const deltaStr = deltaPct != null ? ` (${deltaPct > 0 ? '+' : ''}${deltaPct.toFixed(1)}%)` : '';
      console.log(`  ✓ ${metric.metric_key}: ${value.toLocaleString()}${deltaStr}`);
      snapshotCount++;
    } catch (err) {
      console.error(`  ✗ ${metric.metric_key}: ${err.message}`);
      snapshotErrors++;
    }
  }
  console.log(`  Snapshots: ${snapshotCount} ok, ${snapshotErrors} errors\n`);

  // ══════════════════════════════════════════════════════════════════════════
  // PHASE 2: ANOMALY DETECTION
  // ══════════════════════════════════════════════════════════════════════════
  console.log('─── Phase 2: Anomaly Detection ───');

  // Get today's snapshots
  const todaySnaps = await pool.request().query(`
    SELECT metric_key, value_decimal, delta_pct
    FROM intel.metric_snapshot
    WHERE as_of_date = CAST(GETDATE() AS DATE)
      AND segment_key IS NULL
      AND value_decimal IS NOT NULL
  `);

  let anomalyCount = 0;

  for (const snap of todaySnaps.recordset) {
    const mk = snap.metric_key;
    const currentVal = Number(snap.value_decimal);

    // Get historical values (same metric, last 12 months)
    const histResult = await pool.request()
      .input('mk', sql.NVarChar, mk)
      .query(`
        SELECT value_decimal
        FROM intel.metric_snapshot
        WHERE metric_key = @mk
          AND segment_key IS NULL
          AND as_of_date < CAST(GETDATE() AS DATE)
          AND as_of_date >= DATEADD(YEAR, -1, CAST(GETDATE() AS DATE))
          AND value_decimal IS NOT NULL
        ORDER BY as_of_date
      `);

    const histValues = histResult.recordset.map(r => Number(r.value_decimal)).filter(v => Number.isFinite(v));
    if (histValues.length < 5) continue; // Not enough history

    const med = median(histValues);
    const madVal = mad(histValues, med);
    const z = robustZ(currentVal, med, madVal);

    const threshold = 3.0;
    if (Math.abs(z) < threshold) continue;

    const direction = z > 0 ? 'spike' : 'drop';
    const severity = Math.min(5, Math.max(1, Math.round(Math.abs(z))));
    const title = `${direction === 'spike' ? 'Unusual increase' : 'Unusual decrease'} in ${mk}`;
    const summary = `${mk} is ${currentVal.toLocaleString()} vs historical median ${med.toLocaleString()} (z-score: ${z.toFixed(1)}). This is a ${Math.abs(z).toFixed(1)}σ ${direction}.`;

    // Check if similar insight already exists today
    const existingInsight = await pool.request()
      .input('mk', sql.NVarChar, mk)
      .input('asOf', sql.Date, today)
      .query(`
        SELECT 1 FROM intel.insight
        WHERE metric_key = @mk AND as_of_date = @asOf AND insight_type = 'anomaly'
      `);

    if (existingInsight.recordset.length === 0) {
      await pool.request()
        .input('type', sql.NVarChar, 'anomaly')
        .input('severity', sql.Int, severity)
        .input('title', sql.NVarChar, title)
        .input('summary', sql.NVarChar, summary)
        .input('mk', sql.NVarChar, mk)
        .input('asOf', sql.Date, today)
        .input('curVal', sql.Decimal(18, 4), currentVal)
        .input('baseVal', sql.Decimal(18, 4), med)
        .input('delta', sql.Float, ((currentVal - med) / Math.abs(med || 1)) * 100)
        .input('evidence', sql.NVarChar, JSON.stringify({
          algorithm: 'robust_z_score',
          z_score: z,
          median: med,
          mad: madVal,
          history_count: histValues.length,
          direction,
        }))
        .query(`
          INSERT INTO intel.insight
            (insight_type, severity, title, summary, metric_key, as_of_date,
             current_value, baseline_value, delta_pct, evidence_json)
          VALUES (@type, @severity, @title, @summary, @mk, @asOf,
                  @curVal, @baseVal, @delta, @evidence)
        `);
      console.log(`  ⚠ ${title} (z=${z.toFixed(1)}, severity=${severity})`);
      anomalyCount++;
    }
  }
  console.log(`  Anomalies found: ${anomalyCount}\n`);

  // ══════════════════════════════════════════════════════════════════════════
  // PHASE 3: DONOR RISK SCORING
  // ══════════════════════════════════════════════════════════════════════════
  console.log('─── Phase 3: Donor Risk Scoring ───');

  // Use the intel.vw_donor_health view for RFM + risk signals
  const donors = await pool.request().query(`
    SELECT
      person_id,
      days_since_last,
      donation_count,
      total_given,
      recency_score,
      frequency_score,
      monetary_score,
      rfm_score,
      engagement_depth,
      growth_trajectory,
      risk_level,
      annual_revenue_at_risk
    FROM intel.vw_donor_health
  `);

  let scoredCount = 0;

  for (const d of donors.recordset) {
    // Compute composite risk score (0-100, higher = more at risk)
    let riskScore = 0;
    const drivers = [];

    // Recency risk (0-40 points)
    const recency = d.days_since_last || 0;
    if (recency > 730) { riskScore += 40; drivers.push('Lost (2+ years silent)'); }
    else if (recency > 365) { riskScore += 30; drivers.push('Lapsed (1+ year silent)'); }
    else if (recency > 180) { riskScore += 20; drivers.push('Cooling (6+ months silent)'); }
    else if (recency > 90) { riskScore += 10; drivers.push('Slowing (3+ months)'); }

    // Low frequency (0-15 points)
    const freq = d.donation_count || 0;
    if (freq === 0) { riskScore += 15; drivers.push('No gifts in 12 months'); }
    else if (freq === 1) { riskScore += 10; drivers.push('Only 1 gift in 12 months'); }
    else if (freq <= 2) { riskScore += 5; drivers.push('Low frequency (≤2 gifts)'); }

    // Declining monetary (0-15 points)
    if (d.growth_trajectory === 'Declining') { riskScore += 15; drivers.push('Declining giving trend'); }
    else if (d.growth_trajectory === 'Inactive') { riskScore += 10; drivers.push('Inactive giving'); }

    // Low engagement (0-10 points)
    if (d.engagement_depth === 'Shallow') { riskScore += 10; drivers.push('Low engagement depth'); }
    else if (d.engagement_depth === 'Moderate') { riskScore += 5; }

    // Capacity gap (0-10 points) — check if this person has wealth screening
    // Only add points if they have capacity data showing undertapped potential
    if (d.rfm_score && d.rfm_score < 6) { riskScore += 10; drivers.push('Low RFM score'); }

    // Cap at 100
    riskScore = Math.min(100, riskScore);

    // Risk label
    let scoreLabel;
    if (riskScore >= 80) scoreLabel = 'critical';
    else if (riskScore >= 60) scoreLabel = 'high';
    else if (riskScore >= 30) scoreLabel = 'medium';
    else scoreLabel = 'low';

    // Upsert into intel.person_score
    await pool.request()
      .input('pid', sql.Int, d.person_id)
      .input('scoreType', sql.NVarChar, 'churn_risk')
      .input('scoreVal', sql.Decimal(10, 4), riskScore / 100.0) // normalize to 0-1
      .input('scoreLabel', sql.NVarChar, scoreLabel)
      .input('modelVer', sql.NVarChar, 'rules_v1')
      .input('asOf', sql.Date, today)
      .input('drivers', sql.NVarChar, JSON.stringify(drivers.slice(0, 5)))
      .query(`
        IF EXISTS (
          SELECT 1 FROM intel.person_score
          WHERE person_id = @pid AND score_type = @scoreType AND as_of_date = @asOf
        )
          UPDATE intel.person_score
          SET score_value = @scoreVal, score_label = @scoreLabel,
              model_version = @modelVer, drivers_json = @drivers, created_at = SYSUTCDATETIME()
          WHERE person_id = @pid AND score_type = @scoreType AND as_of_date = @asOf
        ELSE
          INSERT INTO intel.person_score
            (person_id, score_type, score_value, score_label, model_version, as_of_date, drivers_json)
          VALUES (@pid, @scoreType, @scoreVal, @scoreLabel, @modelVer, @asOf, @drivers)
      `);
    scoredCount++;
  }
  console.log(`  Donors scored: ${scoredCount}`);

  // Generate summary insight for critical risk donors
  const criticalResult = await pool.request()
    .input('asOf', sql.Date, today)
    .query(`
      SELECT COUNT(*) AS cnt, SUM(CAST(dh.annual_revenue_at_risk AS DECIMAL(18,2))) AS total_at_risk
      FROM intel.person_score ps
      JOIN intel.vw_donor_health dh ON dh.person_id = ps.person_id
      WHERE ps.score_type = 'churn_risk'
        AND ps.as_of_date = @asOf
        AND ps.score_label IN ('critical', 'high')
    `);

  const critical = criticalResult.recordset[0];
  if (critical && critical.cnt > 0) {
    const totalAtRisk = Number(critical.total_at_risk || 0);
    const critCount = critical.cnt;

    // Check if similar insight exists
    const existing = await pool.request()
      .input('asOf', sql.Date, today)
      .query(`
        SELECT 1 FROM intel.insight
        WHERE as_of_date = @asOf AND insight_type = 'risk' AND title LIKE '%churn risk%'
      `);

    if (existing.recordset.length === 0) {
      await pool.request()
        .input('type', sql.NVarChar, 'risk')
        .input('severity', sql.Int, 4)
        .input('title', sql.NVarChar, `${critCount} donors at high/critical churn risk`)
        .input('summary', sql.NVarChar,
          `${critCount} donors are at high or critical risk of churning, representing $${totalAtRisk.toLocaleString()} in annual revenue at risk. ` +
          `Top risk factors: prolonged silence, declining giving trends, and low engagement.`)
        .input('asOf', sql.Date, today)
        .input('delta', sql.Float, null)
        .input('evidence', sql.NVarChar, JSON.stringify({
          high_risk_count: critCount,
          annual_revenue_at_risk: totalAtRisk,
          model_version: 'rules_v1',
        }))
        .query(`
          INSERT INTO intel.insight
            (insight_type, severity, title, summary, as_of_date, delta_pct, evidence_json)
          VALUES (@type, @severity, @title, @summary, @asOf, @delta, @evidence)
        `);
      console.log(`  ⚠ Generated churn risk insight: ${critCount} donors, $${totalAtRisk.toLocaleString()} at risk`);
    }
  }

  // ── Generate LYBUNT insight ───────────────────────────────────────────────
  const lybuntResult = await pool.request().query(`
    SELECT COUNT(*) AS cnt, SUM(ly_total) AS total_prior
    FROM intel.vw_lybunt
  `);

  const lybunt = lybuntResult.recordset[0];
  if (lybunt && lybunt.cnt > 0) {
    const existing = await pool.request()
      .input('asOf', sql.Date, today)
      .query(`
        SELECT 1 FROM intel.insight
        WHERE as_of_date = @asOf AND insight_type = 'opportunity' AND title LIKE '%LYBUNT%'
      `);

    if (existing.recordset.length === 0) {
      await pool.request()
        .input('type', sql.NVarChar, 'opportunity')
        .input('severity', sql.Int, 3)
        .input('title', sql.NVarChar, `${lybunt.cnt} LYBUNT donors to re-engage`)
        .input('summary', sql.NVarChar,
          `${lybunt.cnt} donors gave last year but haven't given yet this year (LYBUNT), ` +
          `representing $${Number(lybunt.total_prior || 0).toLocaleString()} in prior year giving. ` +
          `These are the highest-probability recovery targets.`)
        .input('asOf', sql.Date, today)
        .input('evidence', sql.NVarChar, JSON.stringify({
          lybunt_count: lybunt.cnt,
          prior_year_total: Number(lybunt.total_prior || 0),
        }))
        .query(`
          INSERT INTO intel.insight
            (insight_type, severity, title, summary, as_of_date, evidence_json)
          VALUES (@type, @severity, @title, @summary, @asOf, @evidence)
        `);
      console.log(`  ⚠ Generated LYBUNT insight: ${lybunt.cnt} donors, $${Number(lybunt.total_prior || 0).toLocaleString()}`);
    }
  }

  console.log(`\n=== Nightly intelligence complete ===`);
  console.log(`  Snapshots: ${snapshotCount}`);
  console.log(`  Anomalies: ${anomalyCount}`);
  console.log(`  Donors scored: ${scoredCount}`);

  await pool.close();
}

main().catch(err => { console.error(err); process.exit(1); });
