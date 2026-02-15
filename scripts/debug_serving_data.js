/**
 * debug_serving_data.js
 * Investigate data quality issues in serving views (sozov2 database).
 * Runs 5 diagnostic queries and prints readable output.
 */

const fs = require('fs');
const sql = require('mssql');

function loadEnv() {
  const text = fs.readFileSync('.env.local', 'utf8');
  for (const line of text.split('\n')) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

function printTable(rows) {
  if (!rows || rows.length === 0) {
    console.log('  (no rows)\n');
    return;
  }
  const cols = Object.keys(rows[0]);
  // Compute column widths
  const widths = {};
  for (const col of cols) {
    widths[col] = col.length;
    for (const row of rows) {
      const val = row[col] == null ? 'NULL' : String(row[col]);
      widths[col] = Math.max(widths[col], Math.min(val.length, 40));
    }
  }
  // Header
  const header = cols.map(c => c.padEnd(widths[c])).join(' | ');
  const sep = cols.map(c => '-'.repeat(widths[c])).join('-+-');
  console.log('  ' + header);
  console.log('  ' + sep);
  // Rows
  for (const row of rows) {
    const line = cols.map(c => {
      const val = row[c] == null ? 'NULL' : String(row[c]);
      return val.substring(0, 40).padEnd(widths[c]);
    }).join(' | ');
    console.log('  ' + line);
  }
  console.log('  (' + rows.length + ' rows)\n');
}

async function main() {
  loadEnv();

  console.log('Connecting to sozov2...');
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false, requestTimeout: 60000 }
  });
  console.log('Connected.\n');

  // Query 1: NULL display_name in donor_summary
  console.log('=== QUERY 1: Top 20 donors by total_given (check for NULL display_name) ===');
  try {
    const r1 = await pool.request().query(`
      SELECT TOP 20 person_id, display_name, first_name, last_name, email, total_given, donation_count
      FROM serving.donor_summary 
      ORDER BY total_given DESC
    `);
    printTable(r1.recordset);
  } catch (e) {
    console.log('  ERROR:', e.message, '\n');
  }

  // Query 2: NULL display_name donors — source details
  console.log('=== QUERY 2: Donation details for top 5 NULL display_name donors ===');
  try {
    const r2 = await pool.request().query(`
      SELECT TOP 20 d.person_id, d.display_name, d.source_system, d.amount, d.donated_at, d.donation_month
      FROM serving.donation_detail d
      WHERE d.person_id IN (
        SELECT TOP 5 person_id FROM serving.donor_summary WHERE display_name IS NULL OR display_name LIKE 'NULL%' ORDER BY total_given DESC
      )
      ORDER BY d.person_id, d.donated_at DESC
    `);
    printTable(r2.recordset);
  } catch (e) {
    console.log('  ERROR:', e.message, '\n');
  }

  // Query 3: Bad dates — donations by year
  console.log('=== QUERY 3: Donation counts and totals by year (check for bad dates) ===');
  try {
    const r3 = await pool.request().query(`
      SELECT donation_year, COUNT(*) cnt, SUM(amount) total
      FROM serving.donation_detail
      GROUP BY donation_year
      ORDER BY donation_year
    `);
    printTable(r3.recordset);
  } catch (e) {
    console.log('  ERROR:', e.message, '\n');
  }

  // Query 4: Top 20 donors last 24 months
  console.log('=== QUERY 4: Top 20 donors in last 24 months ===');
  try {
    const r4 = await pool.request().query(`
      SELECT TOP 20 person_id, display_name, total_given, donation_count, first_gift_date, last_gift_date
      FROM serving.donor_summary
      WHERE last_gift_date >= DATEADD(YEAR, -2, GETDATE())
      ORDER BY total_given DESC
    `);
    printTable(r4.recordset);
  } catch (e) {
    console.log('  ERROR:', e.message, '\n');
  }

  // Query 5: Monthly data for those top 20
  console.log('=== QUERY 5: Monthly donation data for top 20 recent donors ===');
  try {
    const r5 = await pool.request().query(`
      SELECT m.person_id, m.display_name, m.donation_month, m.donation_year, m.amount, m.gifts
      FROM serving.donor_monthly m
      WHERE m.person_id IN (
        SELECT TOP 20 person_id FROM serving.donor_summary 
        WHERE last_gift_date >= DATEADD(YEAR, -2, GETDATE())
        ORDER BY total_given DESC
      )
      ORDER BY m.donation_month
    `);
    printTable(r5.recordset);
  } catch (e) {
    console.log('  ERROR:', e.message, '\n');
  }

  await pool.close();
  console.log('Done. Connection closed.');
}

main().catch(err => {
  console.error('Fatal error:', err.message);
  process.exit(1);
});
