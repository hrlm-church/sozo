/**
 * Materialize Serving Views → Tables
 *
 * Converts the 11 serving views into actual tables with indexes.
 * This eliminates the expensive JOINs that timeout on low DTU.
 *
 * Process: SELECT INTO temp → DROP VIEW → RENAME → ADD INDEXES
 *
 * Usage: node scripts/pipeline/materialize_serving.js
 *   --skip-tag   Skip tag_detail (3M rows, takes longest)
 *   --only=name  Only materialize one specific view
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

const wait = ms => new Promise(r => setTimeout(r, ms));

// Order matters: donation_detail must be materialized before donor_summary/donor_monthly
const VIEWS_IN_ORDER = [
  { name: 'person_360',          indexes: ['person_id', 'display_name', 'lifecycle_stage'] },
  { name: 'household_360',       indexes: ['household_id'] },
  { name: 'donation_detail',     indexes: ['person_id', 'donation_month', 'donation_year'] },
  // These two depend on donation_detail — must come after
  { name: 'donor_summary',       indexes: ['person_id', 'total_given', 'lifecycle_stage'] },
  { name: 'donor_monthly',       indexes: ['person_id', 'donation_month'] },
  { name: 'order_detail',        indexes: ['person_id', 'order_month'] },
  { name: 'payment_detail',      indexes: ['person_id', 'payment_month'] },
  { name: 'invoice_detail',      indexes: ['person_id', 'invoice_month'] },
  { name: 'subscription_detail', indexes: ['person_id', 'subscription_status'] },
  { name: 'tag_detail',          indexes: ['person_id', 'tag_group'] },
  { name: 'communication_detail', indexes: ['person_id', 'channel'] },
];

async function materializeView(pool, viewName, indexes) {
  const full = `serving.${viewName}`;
  const temp = `serving._${viewName}`;

  // Check if it's actually a view (might already be a table from a previous run)
  const typeCheck = await pool.request().query(`
    SELECT type_desc FROM sys.objects
    WHERE schema_id = SCHEMA_ID('serving') AND name = '${viewName}'
  `);
  const objType = typeCheck.recordset[0]?.type_desc;

  if (!objType) {
    console.log(`  ⏭  ${full} — does not exist, skipping`);
    return;
  }

  if (objType === 'USER_TABLE') {
    console.log(`  ✓  ${full} — already a table`);
    return;
  }

  console.log(`  →  ${full} (${objType}) — materializing...`);

  // Drop temp table if leftover from previous failed run
  await pool.request().query(`
    IF OBJECT_ID('${temp}', 'U') IS NOT NULL DROP TABLE ${temp}
  `);

  // SELECT INTO to materialize
  const t0 = Date.now();
  const result = await pool.request().query(`SELECT * INTO ${temp} FROM ${full}`);
  const rowCount = result.rowsAffected?.[0] ?? 0;
  console.log(`     ${rowCount.toLocaleString()} rows copied (${((Date.now() - t0) / 1000).toFixed(1)}s)`);

  await wait(500);

  // Drop the view
  await pool.request().query(`DROP VIEW ${full}`);

  // Rename temp table to final name
  await pool.request().query(`EXEC sp_rename '${temp}', '${viewName}'`);

  // Verify it's now under serving schema
  console.log(`     Table created, adding indexes...`);

  // Add indexes
  for (const col of indexes) {
    try {
      await pool.request().query(
        `CREATE NONCLUSTERED INDEX IX_${viewName}_${col} ON ${full}(${col})`
      );
    } catch (err) {
      // Index might already exist or column issue
      if (!err.message.includes('already exists')) {
        console.log(`     ⚠ Index on ${col}: ${err.message.substring(0, 100)}`);
      }
    }
    await wait(300);
  }

  console.log(`     ✓ Done`);
}

async function main() {
  loadEnv();
  const args = process.argv.slice(2);
  const skipTag = args.includes('--skip-tag');
  const onlyMatch = args.find(a => a.startsWith('--only='));
  const onlyName = onlyMatch ? onlyMatch.split('=')[1] : null;

  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 600000, // 10 min — tag_detail is 3M rows
  });

  console.log('Materializing serving views into tables...\n');

  for (const v of VIEWS_IN_ORDER) {
    if (onlyName && v.name !== onlyName) continue;
    if (skipTag && v.name === 'tag_detail') {
      console.log(`  ⏭  serving.tag_detail — skipped (--skip-tag)`);
      continue;
    }

    try {
      await materializeView(pool, v.name, v.indexes);
    } catch (err) {
      console.error(`  ✗  serving.${v.name} FAILED: ${err.message.substring(0, 200)}`);
    }
    await wait(1000); // breathe between views on low DTU
  }

  // Final row counts
  console.log('\nFinal row counts:');
  for (const v of VIEWS_IN_ORDER) {
    if (onlyName && v.name !== onlyName) continue;
    try {
      const cnt = await pool.request().query(`SELECT COUNT(*) n FROM serving.${v.name}`);
      console.log(`  serving.${v.name}: ${cnt.recordset[0].n.toLocaleString()}`);
    } catch (err) {
      console.log(`  serving.${v.name}: ERROR — ${err.message.substring(0, 100)}`);
    }
  }

  await pool.close();
  console.log('\nDone.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
