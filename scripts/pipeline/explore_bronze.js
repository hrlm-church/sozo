/**
 * Explore bronze data in sozov2 — sample all key tables
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
    server: process.env.SOZO_SQL_HOST, database: 'sozov2',
    user: process.env.SOZO_SQL_USER, password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 60000
  });

  async function sample(label, query, maxValLen = 100) {
    console.log('\n' + '='.repeat(80));
    console.log(label);
    console.log('='.repeat(80));
    const res = await pool.request().query(query);
    for (let i = 0; i < res.recordset.length; i++) {
      const row = res.recordset[i];
      console.log(`\n--- Row ${i + 1} ---`);
      for (const [k, v] of Object.entries(row)) {
        if (k === '_row_id') continue;
        const s = v == null ? 'NULL' : String(v).substring(0, maxValLen);
        console.log(`  ${k}: ${s}`);
      }
    }
  }

  async function columns(label, schema, table) {
    console.log('\n' + '='.repeat(80));
    console.log(label);
    console.log('='.repeat(80));
    const res = await pool.request().query(`
      SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA='${schema}' AND TABLE_NAME='${table}'
      ORDER BY ORDINAL_POSITION
    `);
    console.log(res.recordset.map(r => r.COLUMN_NAME).join(', '));
    console.log(`(${res.recordset.length} columns)`);
  }

  // ── KEAP ──────────────────────────────────────────
  await columns('KEAP CONTACT COLUMNS', 'keap', 'hb840_Contact');
  await sample('KEAP CONTACT (2 rows)', 'SELECT TOP 2 * FROM keap.hb840_Contact', 60);
  await sample('KEAP INVOICE (2 rows)', 'SELECT TOP 2 * FROM keap.hb840_Invoice');
  await sample('KEAP PAYMENT (2 rows)', 'SELECT TOP 2 * FROM keap.hb840_Payment');
  await sample('KEAP NOTES (2 rows)', 'SELECT TOP 2 * FROM keap.hb840_Notes', 120);
  await sample('KEAP ORDERS (2 rows)', 'SELECT TOP 2 * FROM keap.hb840_Orders_known_as_Jobs');
  await sample('KEAP TAGS (5 rows)', 'SELECT TOP 5 * FROM keap.hb840_Tags');
  await sample('KEAP PRODUCTS (3 rows)', 'SELECT TOP 3 * FROM keap.hb840_Products');
  await sample('KEAP SUBSCRIPTIONS (3 rows)', 'SELECT TOP 3 * FROM keap.hb840_Subscriptions_known_as_JobRecurring');

  // ── DONOR DIRECT ──────────────────────────────────
  await columns('DD TRANSACTIONS COLUMNS', 'original_files_from_donor_direct', 'Data_Entered_PFM_Transactions');
  await sample('DD ACCOUNTS (3 rows)', 'SELECT TOP 3 * FROM original_files_from_donor_direct.Data_Entered_PFM_Accounts');
  await sample('DD TRANSACTIONS (3 rows)', 'SELECT TOP 3 * FROM original_files_from_donor_direct.Data_Entered_PFM_Transactions', 60);
  await sample('DD EMAILS (3 rows)', 'SELECT TOP 3 * FROM original_files_from_donor_direct.Data_Entered_PFM_AccountEmails');
  await sample('DD PHONES (3 rows)', 'SELECT TOP 3 * FROM original_files_from_donor_direct.Data_Entered_PFM_AccountPhones_csv');
  await sample('DD ADDRESSES (3 rows)', 'SELECT TOP 3 * FROM original_files_from_donor_direct.Data_Entered_PFM_AccountAddresses_csv');
  await sample('DD NOTES (3 rows)', 'SELECT TOP 3 * FROM original_files_from_donor_direct.Data_Entered_PFM_AccountNotes');
  await sample('DD COMMS (3 rows)', 'SELECT TOP 3 * FROM original_files_from_donor_direct.Data_Entered_PFM_AccountCommunications');
  await sample('DD ANNUAL SUMMARY (3 rows)', 'SELECT TOP 3 * FROM original_files_from_donor_direct.Account_Annual_Summary_1');

  // ── GIVEBUTTER ────────────────────────────────────
  await sample('GB TRANSACTIONS (3 rows)', 'SELECT TOP 3 * FROM data_files_to_send_to_givebutter.Sent_to_Givebutter_Transactions_Data_Completed', 60);
  await sample('GB CONTACTS (3 rows)', 'SELECT TOP 3 * FROM data_files_to_send_to_givebutter.Sent_to_Givebutter_Contact_Data', 60);
  await sample('GB ACTIVITY (3 rows)', 'SELECT TOP 3 * FROM data_files_to_send_to_givebutter.Activity_Data_Completed');

  // ── STRIPE ────────────────────────────────────────
  await sample('STRIPE CUSTOMER IDS (3 rows)', 'SELECT TOP 3 * FROM stripe_import.Stripe_Customer_IDS');
  await sample('STRIPE UNIFIED (3 rows)', 'SELECT TOP 3 * FROM stripe_import.unified_customers_1');

  // ── BLOOMERANG ────────────────────────────────────
  await sample('BLOOMERANG COPY (3 rows)', 'SELECT TOP 3 * FROM bloomerang.copy_from_acct_1BENjxH9QytxOWwp_Bloomerang_to_acct_103CTQ2UU2FcRJxQ_TrueGirl_migreq_1SLnkR2UU2FcRJxQGNTFgzbF_1');

  // ── TRANSACTION IMPORTS ───────────────────────────
  await sample('TXN IMPORTS: Keap Apr-Aug 2025 (3 rows)', 'SELECT TOP 3 * FROM transaction_imports.Donation_Imports_Keap_Final_Keap_Donations_April_3_to_Aug_13_2025', 60);
  await sample('TXN IMPORTS: Kindful Jan-Mar 2025 (3 rows)', 'SELECT TOP 3 * FROM transaction_imports.Donation_Imports_Kindful_Imported_Successfully_Kindful_Report_Jan_1_2025_to_March_31_2025', 60);

  // ── MISC ──────────────────────────────────────────
  await sample('MISC: Recurring Partner Transfer (3 rows)', 'SELECT TOP 3 * FROM misc.Recurring_Partner_Transfer', 60);
  await sample('MISC: Sept 2025 Kindful (3 rows)', 'SELECT TOP 3 * FROM misc.Sept_2025_Kindful_Transactions');

  // ── KEY COUNTS ────────────────────────────────────
  console.log('\n' + '='.repeat(80));
  console.log('KEY ENTITY COUNTS');
  console.log('='.repeat(80));
  const counts = [
    ['Keap Contacts', 'SELECT COUNT(*) AS n FROM keap.hb840_Contact'],
    ['DD Accounts', 'SELECT COUNT(*) AS n FROM original_files_from_donor_direct.Data_Entered_PFM_Accounts'],
    ['DD Transactions', 'SELECT COUNT(*) AS n FROM original_files_from_donor_direct.Data_Entered_PFM_Transactions'],
    ['Keap Invoices', 'SELECT COUNT(*) AS n FROM keap.hb840_Invoice'],
    ['Keap Payments', 'SELECT COUNT(*) AS n FROM keap.hb840_Payment'],
    ['Keap Notes', 'SELECT COUNT(*) AS n FROM keap.hb840_Notes'],
    ['Keap Orders', 'SELECT COUNT(*) AS n FROM keap.hb840_Orders_known_as_Jobs'],
    ['GB Transactions', 'SELECT COUNT(*) AS n FROM data_files_to_send_to_givebutter.Sent_to_Givebutter_Transactions_Data_Completed'],
    ['GB Contacts', 'SELECT COUNT(*) AS n FROM data_files_to_send_to_givebutter.Sent_to_Givebutter_Contact_Data'],
  ];
  for (const [label, q] of counts) {
    const r = await pool.request().query(q);
    console.log(`  ${label}: ${r.recordset[0].n.toLocaleString()}`);
  }

  await pool.close();
  console.log('\nDone.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
