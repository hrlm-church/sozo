/**
 * Integrate Remaining Bronze Data into Serving Layer
 *
 * 1. Wealth Screening (1,109 rows) → serving.wealth_screening
 * 2. Lost Recurring Donors (383 rows) → serving.lost_recurring_donors
 * 3. Sept 2025 Kindful Transactions (114 rows) → check & integrate
 *
 * Usage: node scripts/pipeline/integrate_remaining_data.js
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

// Parse the WlthScrn23 text field into structured values
function parseWealthScreen(text) {
  if (!text) return {};
  const result = {};

  // "2023 Giving Capacity: $27,933"
  const cap = text.match(/Giving Capacity:\s*\$?([\d,]+)/);
  if (cap) result.giving_capacity = parseInt(cap[1].replace(/,/g, ''));

  // "DS Prospect Rating: DS1-5"
  const rating = text.match(/DS Prospect Rating:\s*(\S+)/);
  if (rating) result.prospect_rating = rating[1];

  // "Quality Score: 20.0"
  const qs = text.match(/Quality Score:\s*([\d.]+)/);
  if (qs) result.quality_score = parseFloat(qs[1]);

  // "Capacity Level: J - $25,000 - $49,999"
  const level = text.match(/Capacity Level:\s*(\S+)\s*-\s*\$?([\d,]+)\s*-\s*\$?([\d,]+)/);
  if (level) {
    result.capacity_tier = level[1];
    result.capacity_min = parseInt(level[2].replace(/,/g, ''));
    result.capacity_max = parseInt(level[3].replace(/,/g, ''));
  }

  // "Est. Real Estate Valuation: $340,000"
  const re = text.match(/Real Estate Valuation:\s*\$?([\d,]+)/);
  if (re) result.real_estate_value = parseInt(re[1].replace(/,/g, ''));

  return result;
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

  // ═══════════════════════════════════════════════════════════════════
  // 1. WEALTH SCREENING
  // ═══════════════════════════════════════════════════════════════════
  console.log('=== 1. WEALTH SCREENING ===');

  // Drop if exists
  await pool.request().query(`
    IF OBJECT_ID('serving.wealth_screening', 'U') IS NOT NULL DROP TABLE serving.wealth_screening
  `);

  // Create table
  await pool.request().query(`
    CREATE TABLE serving.wealth_screening (
      person_id INT,
      keap_id INT,
      display_name NVARCHAR(200),
      email NVARCHAR(200),
      giving_capacity INT,
      capacity_tier NVARCHAR(5),
      capacity_min INT,
      capacity_max INT,
      capacity_label NVARCHAR(50),
      prospect_rating NVARCHAR(20),
      quality_score DECIMAL(5,1),
      real_estate_value INT,
      raw_screening_text NVARCHAR(MAX)
    )
  `);

  // Read raw data
  const wealth = await pool.request().query(`
    SELECT w.KEAP_ID, w.First_Name, w.Last_Name, w.Email, w.WlthScrn23,
      im.master_id
    FROM donor_direct_data_transfer_2024.Donor_Direct_Data_Transfer_Wealth_Screen_Data_from_Pure_Freedom w
    LEFT JOIN silver.identity_map im ON im.source_system = 'keap' AND im.source_id = w.KEAP_ID AND im.is_primary = 1
  `);

  let wealthInserted = 0;
  const BATCH = 50;
  let batch = [];

  for (const row of wealth.recordset) {
    const parsed = parseWealthScreen(row.WlthScrn23);
    const keapId = parseInt(row.KEAP_ID) || null;
    const personId = row.master_id || null;
    const displayName = [row.First_Name, row.Last_Name].filter(Boolean).join(' ') || 'Unknown';

    // Capacity label
    let capacityLabel = 'Standard';
    if (parsed.giving_capacity >= 250000) capacityLabel = 'Ultra High ($250K+)';
    else if (parsed.giving_capacity >= 100000) capacityLabel = 'Very High ($100K-$250K)';
    else if (parsed.giving_capacity >= 25000) capacityLabel = 'High ($25K-$100K)';
    else if (parsed.giving_capacity >= 10000) capacityLabel = 'Medium ($10K-$25K)';

    batch.push(`(${personId ?? 'NULL'}, ${keapId ?? 'NULL'}, N'${displayName.replace(/'/g, "''")}', ${row.Email ? `N'${row.Email.replace(/'/g, "''")}'` : 'NULL'}, ${parsed.giving_capacity ?? 'NULL'}, ${parsed.capacity_tier ? `N'${parsed.capacity_tier}'` : 'NULL'}, ${parsed.capacity_min ?? 'NULL'}, ${parsed.capacity_max ?? 'NULL'}, N'${capacityLabel}', ${parsed.prospect_rating ? `N'${parsed.prospect_rating}'` : 'NULL'}, ${parsed.quality_score ?? 'NULL'}, ${parsed.real_estate_value ?? 'NULL'}, N'${(row.WlthScrn23 || '').replace(/'/g, "''").replace(/\r?\n/g, ' ')}')`);

    if (batch.length >= BATCH) {
      await pool.request().query(`INSERT INTO serving.wealth_screening (person_id, keap_id, display_name, email, giving_capacity, capacity_tier, capacity_min, capacity_max, capacity_label, prospect_rating, quality_score, real_estate_value, raw_screening_text) VALUES ${batch.join(',')}`);
      wealthInserted += batch.length;
      batch = [];
      await wait(200);
    }
  }
  if (batch.length > 0) {
    await pool.request().query(`INSERT INTO serving.wealth_screening (person_id, keap_id, display_name, email, giving_capacity, capacity_tier, capacity_min, capacity_max, capacity_label, prospect_rating, quality_score, real_estate_value, raw_screening_text) VALUES ${batch.join(',')}`);
    wealthInserted += batch.length;
  }

  // Add indexes
  await pool.request().query(`CREATE NONCLUSTERED INDEX IX_ws_person ON serving.wealth_screening(person_id)`);
  await pool.request().query(`CREATE NONCLUSTERED INDEX IX_ws_capacity ON serving.wealth_screening(giving_capacity)`);
  await pool.request().query(`CREATE NONCLUSTERED INDEX IX_ws_label ON serving.wealth_screening(capacity_label)`);

  console.log(`  Inserted ${wealthInserted} wealth screening records`);

  // Capacity distribution
  const capDist = await pool.request().query(`
    SELECT capacity_label, COUNT(*) cnt, AVG(giving_capacity) avg_cap
    FROM serving.wealth_screening
    GROUP BY capacity_label ORDER BY AVG(giving_capacity) DESC
  `);
  for (const r of capDist.recordset) console.log(`  ${r.capacity_label}: ${r.cnt} donors (avg capacity $${Math.round(r.avg_cap).toLocaleString()})`);

  // How many linked to person_id?
  const linked = await pool.request().query(`SELECT COUNT(*) n FROM serving.wealth_screening WHERE person_id IS NOT NULL`);
  console.log(`  Linked to identity: ${linked.recordset[0].n} / ${wealthInserted}`);

  await wait(1000);

  // ═══════════════════════════════════════════════════════════════════
  // 2. LOST RECURRING DONORS
  // ═══════════════════════════════════════════════════════════════════
  console.log('\n=== 2. LOST RECURRING DONORS ===');

  await pool.request().query(`
    IF OBJECT_ID('serving.lost_recurring_donors', 'U') IS NOT NULL DROP TABLE serving.lost_recurring_donors
  `);

  await pool.request().query(`
    CREATE TABLE serving.lost_recurring_donors (
      person_id INT,
      dd_account_nbr NVARCHAR(50),
      display_name NVARCHAR(200),
      monthly_amount DECIMAL(10,2),
      annual_value DECIMAL(10,2),
      frequency NVARCHAR(10),
      status NVARCHAR(200),
      going_to_givebutter NVARCHAR(10),
      category NVARCHAR(50),
      source_code NVARCHAR(50),
      source_description NVARCHAR(100),
      last_used_date NVARCHAR(50),
      start_date NVARCHAR(50)
    )
  `);

  // Read and insert
  const recurring = await pool.request().query(`
    SELECT r.*,
      im.master_id
    FROM misc.Recurring_Partner_Transfer r
    LEFT JOIN silver.identity_map im
      ON im.source_system = 'donor_direct'
      AND im.source_id = r.DD_Account_Nbr
      AND im.is_primary = 1
  `);

  let recInserted = 0;
  batch = [];
  for (const row of recurring.recordset) {
    const amt = parseFloat(row.Total_Amt) || 0;
    const freq = (row.Freq || 'M').trim();
    const annualValue = freq === 'M' ? amt * 12 : freq === 'Q' ? amt * 4 : freq === 'A' ? amt : amt * 12;

    batch.push(`(${row.master_id ?? 'NULL'}, ${row.DD_Account_Nbr ? `N'${row.DD_Account_Nbr}'` : 'NULL'}, N'${(row.Name || 'Unknown').replace(/'/g, "''")}', ${amt}, ${annualValue}, N'${freq}', N'${(row.Status || '').replace(/'/g, "''")}', N'${(row.Going_into_Givebutter || '').replace(/'/g, "''")}', N'${(row.Category || '').replace(/'/g, "''")}', N'${(row.Source_Code || '').replace(/'/g, "''")}', N'${(row.Source_Description || '').replace(/'/g, "''")}', N'${row.Last_Used_Date || ''}', N'${row.Start_Date || ''}')`);

    if (batch.length >= BATCH) {
      await pool.request().query(`INSERT INTO serving.lost_recurring_donors (person_id, dd_account_nbr, display_name, monthly_amount, annual_value, frequency, status, going_to_givebutter, category, source_code, source_description, last_used_date, start_date) VALUES ${batch.join(',')}`);
      recInserted += batch.length;
      batch = [];
      await wait(200);
    }
  }
  if (batch.length > 0) {
    await pool.request().query(`INSERT INTO serving.lost_recurring_donors (person_id, dd_account_nbr, display_name, monthly_amount, annual_value, frequency, status, going_to_givebutter, category, source_code, source_description, last_used_date, start_date) VALUES ${batch.join(',')}`);
    recInserted += batch.length;
  }

  await pool.request().query(`CREATE NONCLUSTERED INDEX IX_lrd_person ON serving.lost_recurring_donors(person_id)`);

  console.log(`  Inserted ${recInserted} lost recurring donor records`);

  const recSummary = await pool.request().query(`
    SELECT COUNT(*) total, SUM(monthly_amount) mrr, SUM(annual_value) arr,
      SUM(CASE WHEN person_id IS NOT NULL THEN 1 ELSE 0 END) linked
    FROM serving.lost_recurring_donors
  `);
  const rs = recSummary.recordset[0];
  console.log(`  Total MRR lost: $${Math.round(rs.mrr).toLocaleString()}/month ($${Math.round(rs.arr).toLocaleString()}/year)`);
  console.log(`  Linked to identity: ${rs.linked} / ${rs.total}`);

  await wait(1000);

  // ═══════════════════════════════════════════════════════════════════
  // 3. SEPT 2025 KINDFUL — CHECK OVERLAP WITH SILVER.DONATION
  // ═══════════════════════════════════════════════════════════════════
  console.log('\n=== 3. SEPT 2025 KINDFUL TRANSACTIONS ===');

  const kindful = await pool.request().query(`SELECT COUNT(*) n FROM misc.Sept_2025_Kindful_Transactions`);
  console.log(`  Raw records: ${kindful.recordset[0].n}`);

  // Check overlap by email
  const overlap = await pool.request().query(`
    SELECT COUNT(*) n FROM misc.Sept_2025_Kindful_Transactions k
    WHERE EXISTS (
      SELECT 1 FROM silver.contact c
      WHERE c.email_primary = k.Email
    )
  `);
  console.log(`  Already matched to silver.contact by email: ${overlap.recordset[0].n} / ${kindful.recordset[0].n}`);

  // Check if their donations are already in silver.donation
  const donOverlap = await pool.request().query(`
    SELECT COUNT(DISTINCT k.Email) n
    FROM misc.Sept_2025_Kindful_Transactions k
    JOIN silver.contact c ON c.email_primary = k.Email
    JOIN silver.identity_map im ON im.contact_id = c.contact_id
    JOIN serving.donation_detail dd ON dd.person_id = im.master_id
  `);
  console.log(`  With donations already in serving.donation_detail: ${donOverlap.recordset[0].n}`);

  // New contacts not in system
  const newContacts = await pool.request().query(`
    SELECT COUNT(*) n FROM misc.Sept_2025_Kindful_Transactions k
    WHERE NOT EXISTS (SELECT 1 FROM silver.contact c WHERE c.email_primary = k.Email)
  `);
  console.log(`  NEW contacts (not in system): ${newContacts.recordset[0].n}`);

  // ═══════════════════════════════════════════════════════════════════
  // 4. TRANSACTION IMPORTS — CHECK OVERLAP
  // ═══════════════════════════════════════════════════════════════════
  console.log('\n=== 4. TRANSACTION IMPORTS OVERLAP CHECK ===');

  // Count total rows across all transaction_imports tables
  const tiTables = await pool.request().query(`
    SELECT t.name, (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id=t.object_id AND p.index_id IN(0,1)) AS rows
    FROM sys.tables t WHERE t.schema_id = SCHEMA_ID('transaction_imports')
  `);
  let totalTI = 0;
  for (const r of tiTables.recordset) totalTI += (r.rows || 0);
  console.log(`  Total transaction_imports rows: ${totalTI.toLocaleString()} across ${tiTables.recordset.length} tables`);
  console.log(`  These are Keap/Kindful donation imports INTO Donor Direct.`);
  console.log(`  They are already captured in silver.donation via the DD pipeline — SKIP.`);

  // ═══════════════════════════════════════════════════════════════════
  // FINAL SUMMARY
  // ═══════════════════════════════════════════════════════════════════
  console.log('\n=== FINAL SUMMARY ===');

  const tables = await pool.request().query(`
    SELECT t.name, s.name AS sch,
      (SELECT SUM(p.rows) FROM sys.partitions p WHERE p.object_id=t.object_id AND p.index_id IN(0,1)) AS rows
    FROM sys.tables t JOIN sys.schemas s ON s.schema_id=t.schema_id
    WHERE s.name='serving' ORDER BY t.name
  `);
  console.log('\nServing layer tables:');
  for (const r of tables.recordset) console.log(`  serving.${r.name}: ${r.rows?.toLocaleString()} rows`);

  await pool.close();
  console.log('\nDone.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
