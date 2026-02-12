/**
 * Full Pipeline Orchestrator — Runs Steps 1.2 → 1.5 in sequence
 *
 * Usage: node scripts/ingest/run_full_pipeline.js
 *
 * Note: Step 1.1 (schema creation) is separate because it's destructive.
 *       Run it first: node scripts/setup/01_create_schema.js
 */

const { execSync } = require('child_process');
const fs = require('fs');
const path = require('path');
const sql = require('mssql');

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
    requestTimeout: 120000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 5, min: 0, idleTimeoutMillis: 5000 },
  };
}

const steps = [
  { name: '1.2 — Bulk Ingest Raw Data', script: 'scripts/ingest/02_ingest_raw.js' },
  { name: '1.3 — Transform Raw → Entities', script: 'scripts/ingest/03_transform.js' },
  { name: '1.4 — Identity Resolution', script: 'scripts/ingest/04_resolve_identities.js' },
  { name: '1.5 — Build Serving Layer', script: 'scripts/ingest/05_build_serving.js' },
];

async function main() {
  loadEnv();
  console.log('╔══════════════════════════════════════════════════════════╗');
  console.log('║         Sozo Full Data Pipeline                         ║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log(`Started at: ${new Date().toISOString()}\n`);

  const startTime = Date.now();

  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    const stepStart = Date.now();

    console.log(`\n${'━'.repeat(60)}`);
    console.log(`Step ${i + 1}/${steps.length}: ${step.name}`);
    console.log(`${'━'.repeat(60)}`);

    try {
      execSync(`node ${step.script}`, {
        stdio: 'inherit',
        cwd: process.cwd(),
        timeout: 60 * 60 * 1000, // 1 hour max per step
      });
    } catch (err) {
      console.error(`\n❌ FAILED at step: ${step.name}`);
      console.error(`   Exit code: ${err.status}`);
      process.exit(1);
    }

    const stepDuration = ((Date.now() - stepStart) / 1000 / 60).toFixed(1);
    console.log(`\n⏱  Step completed in ${stepDuration} minutes`);
  }

  // ── Final Validation ──
  console.log(`\n${'━'.repeat(60)}`);
  console.log('VALIDATION');
  console.log(`${'━'.repeat(60)}`);

  const pool = await sql.connect(getDbConfig());
  try {
    const checks = [
      { label: 'raw.record', query: 'SELECT COUNT(*) AS cnt FROM raw.record' },
      { label: 'staging.person_extract', query: 'SELECT COUNT(*) AS cnt FROM staging.person_extract' },
      { label: 'person.profile', query: 'SELECT COUNT(*) AS cnt FROM person.profile' },
      { label: 'person.email', query: 'SELECT COUNT(*) AS cnt FROM person.email' },
      { label: 'person.phone', query: 'SELECT COUNT(*) AS cnt FROM person.phone' },
      { label: 'person.address', query: 'SELECT COUNT(*) AS cnt FROM person.address' },
      { label: 'person.source_link', query: 'SELECT COUNT(*) AS cnt FROM person.source_link' },
      { label: 'household.unit', query: 'SELECT COUNT(*) AS cnt FROM household.unit' },
      { label: 'household.member', query: 'SELECT COUNT(*) AS cnt FROM household.member' },
      { label: 'giving.donation', query: 'SELECT COUNT(*) AS cnt FROM giving.donation' },
      { label: 'commerce.invoice', query: 'SELECT COUNT(*) AS cnt FROM commerce.invoice' },
      { label: 'commerce.payment', query: 'SELECT COUNT(*) AS cnt FROM commerce.payment' },
      { label: 'commerce.order', query: 'SELECT COUNT(*) AS cnt FROM commerce.[order]' },
      { label: 'commerce.subscription', query: 'SELECT COUNT(*) AS cnt FROM commerce.subscription' },
      { label: 'engagement.note', query: 'SELECT COUNT(*) AS cnt FROM engagement.note' },
      { label: 'engagement.communication', query: 'SELECT COUNT(*) AS cnt FROM engagement.communication' },
      { label: 'engagement.activity', query: 'SELECT COUNT(*) AS cnt FROM engagement.activity' },
      { label: 'engagement.tag', query: 'SELECT COUNT(*) AS cnt FROM engagement.tag' },
      { label: 'serving.person_360', query: 'SELECT COUNT(*) AS cnt FROM serving.person_360' },
      { label: 'serving.household_360', query: 'SELECT COUNT(*) AS cnt FROM serving.household_360' },
    ];

    console.log('\nTable counts:');
    for (const c of checks) {
      const res = await pool.request().query(c.query);
      const cnt = res.recordset[0].cnt;
      console.log(`  ${c.label.padEnd(30)} ${cnt.toLocaleString().padStart(12)}`);
    }

    // Validation checks
    console.log('\nIntegrity checks:');

    // Every staging person resolved
    const unresolvedRes = await pool.request().query(
      'SELECT COUNT(*) AS cnt FROM staging.person_extract WHERE resolved_person_id IS NULL'
    );
    const unresolved = unresolvedRes.recordset[0].cnt;
    console.log(`  Unresolved staging persons:    ${unresolved === 0 ? 'PASS (0)' : `WARN (${unresolved.toLocaleString()})`}`);

    // No duplicate emails across persons
    const dupEmailRes = await pool.request().query(`
      SELECT COUNT(*) AS cnt FROM (
        SELECT email, COUNT(DISTINCT person_id) AS pids
        FROM person.email GROUP BY email HAVING COUNT(DISTINCT person_id) > 1
      ) x
    `);
    const dupEmails = dupEmailRes.recordset[0].cnt;
    console.log(`  Duplicate emails (cross-person): ${dupEmails === 0 ? 'PASS (0)' : `WARN (${dupEmails})`}`);

    // Donations with person_id
    const donLinked = await pool.request().query(`
      SELECT
        COUNT(*) AS total,
        SUM(CASE WHEN person_id IS NOT NULL THEN 1 ELSE 0 END) AS linked
      FROM giving.donation
    `);
    const dl = donLinked.recordset[0];
    const dlPct = dl.total > 0 ? ((dl.linked / dl.total) * 100).toFixed(1) : '0.0';
    console.log(`  Donations with person_id:      ${dl.linked.toLocaleString()} / ${dl.total.toLocaleString()} (${dlPct}%)`);

    // Person 360 matches person count
    const p360Res = await pool.request().query('SELECT COUNT(*) AS cnt FROM serving.person_360');
    const ppRes = await pool.request().query('SELECT COUNT(*) AS cnt FROM person.profile');
    const p360Match = p360Res.recordset[0].cnt === ppRes.recordset[0].cnt;
    console.log(`  person_360 = person.profile:   ${p360Match ? 'PASS' : `MISMATCH (${p360Res.recordset[0].cnt} vs ${ppRes.recordset[0].cnt})`}`);

  } finally {
    await pool.close();
  }

  const totalDuration = ((Date.now() - startTime) / 1000 / 60).toFixed(1);

  console.log('\n' + '═'.repeat(60));
  console.log(`Pipeline completed in ${totalDuration} minutes`);
  console.log(`Finished at: ${new Date().toISOString()}`);
  console.log('═'.repeat(60));
}

main().catch(err => {
  console.error('FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
