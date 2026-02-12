#!/usr/bin/env node
/**
 * One-off fix: Clean up display_name in person.profile and serving.person_360.
 * Removes literal "NULL" strings and rebuilds display_name from first_name + last_name.
 */
const path = require('path');
const fs = require('fs');
const sql = require('mssql');

function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(p)) { console.error('ERROR: .env.local not found'); process.exit(1); }
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

function getConfig() {
  return {
    server: process.env.SOZO_SQL_HOST,
    database: process.env.SOZO_SQL_DB,
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 300000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 3, min: 0, idleTimeoutMillis: 5000 },
  };
}

async function main() {
  loadEnv();
  console.log('Fix display_name — removing literal "NULL" strings');
  console.log('='.repeat(60));

  const pool = await sql.connect(getConfig());

  // Step 1: Fix person.profile — clean literal "NULL" from first_name, last_name, display_name
  console.log('\n[1] Cleaning person.profile...');

  // Replace literal "NULL" strings with actual NULL
  for (const col of ['first_name', 'last_name', 'display_name']) {
    const r = await pool.request().query(
      `UPDATE person.profile SET ${col} = NULL WHERE ${col} = 'NULL'`
    );
    console.log(`  ${col}: ${r.rowsAffected[0]} rows cleaned`);
  }

  // Rebuild display_name from first_name + last_name where display_name is NULL or contains "NULL"
  // Use LEFT(..., 256) to avoid truncation errors
  const r1 = await pool.request().query(`
    UPDATE person.profile
    SET display_name = LEFT(LTRIM(RTRIM(COALESCE(first_name,'') + ' ' + COALESCE(last_name,''))), 256)
    WHERE display_name IS NULL
       OR display_name LIKE '%NULL%'
       OR display_name = ''
  `);
  console.log(`  Rebuilt display_name: ${r1.rowsAffected[0]} rows`);

  // Clean up display_names that are still empty after rebuild
  const r2 = await pool.request().query(`
    UPDATE person.profile SET display_name = NULL WHERE LTRIM(RTRIM(display_name)) = ''
  `);
  console.log(`  Cleared empty display_name: ${r2.rowsAffected[0]} rows`);

  // Step 2: Propagate to serving.person_360
  console.log('\n[2] Updating serving.person_360...');
  const r3 = await pool.request().query(`
    UPDATE s
    SET s.display_name = COALESCE(p.display_name, LTRIM(RTRIM(COALESCE(p.first_name,'') + ' ' + COALESCE(p.last_name,''))), 'Unknown'),
        s.first_name = p.first_name,
        s.last_name = p.last_name
    FROM serving.person_360 s
    JOIN person.profile p ON p.id = s.person_id
    WHERE s.display_name IS NULL
       OR s.display_name LIKE '%NULL%'
       OR s.display_name = ''
  `);
  console.log(`  Updated person_360: ${r3.rowsAffected[0]} rows`);

  // Step 3: Also fix household_360.name if it has NULL issues
  console.log('\n[3] Checking serving.household_360...');
  const r4 = await pool.request().query(`
    UPDATE serving.household_360
    SET name = REPLACE(REPLACE(name, 'NULL ', ''), ' NULL', '')
    WHERE name LIKE '%NULL%'
  `);
  console.log(`  Fixed household names: ${r4.rowsAffected[0]} rows`);

  // Verify
  console.log('\n[4] Verification...');
  const check1 = await pool.request().query(
    `SELECT COUNT(*) AS cnt FROM serving.person_360 WHERE display_name IS NULL OR display_name LIKE '%NULL%'`
  );
  console.log(`  Remaining NULL/null display_names in person_360: ${check1.recordset[0].cnt}`);

  const check2 = await pool.request().query(
    `SELECT TOP (5) display_name, first_name, last_name, lifetime_giving
     FROM serving.person_360 ORDER BY lifetime_giving DESC`
  );
  console.log('  Top 5 by giving:');
  for (const r of check2.recordset) {
    console.log(`    ${r.display_name || '(null)'} — $${(r.lifetime_giving || 0).toLocaleString()}`);
  }

  await pool.close();
  console.log('\nDone!');
}

main().catch(err => { console.error(err); process.exit(1); });
