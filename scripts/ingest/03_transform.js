/**
 * Step 1.3 — Transform Raw Records into Domain Entities (Client-Side)
 *
 * Strategy: SELECT raw JSON → parse in Node.js (zero DTU) → INSERT VALUES.
 * All JSON parsing happens in JS (free), SQL Server just does simple writes.
 *
 * Run: node scripts/ingest/03_transform.js
 */

const fs = require('fs');
const path = require('path');
const sql = require('mssql');

// ── env & config ─────────────────────────────────────────────────────────────
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(p)) { console.error('.env.local not found'); process.exit(1); }
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

function dbConfig() {
  return {
    server: process.env.SOZO_SQL_HOST,
    database: process.env.SOZO_SQL_DB,
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 300000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 5, min: 1, idleTimeoutMillis: 30000, acquireTimeoutMillis: 300000 },
  };
}

// ── helpers ───────────────────────────────────────────────────────────────────
const BATCH = 2000;
const CHUNK = 100;   // rows per INSERT VALUES statement
const wait = ms => new Promise(r => setTimeout(r, ms));
const S = (v, n) => v != null && String(v).trim() ? String(v).trim().substring(0, n) : null;
const Lo = v => v != null && String(v).trim() ? String(v).toLowerCase().trim().substring(0, 256) : null;
function amt(v) {
  if (v == null) return null;
  const n = parseFloat(String(v).replace(/[$,]/g, ''));
  if (isNaN(n)) return null;
  // Reject (not clamp) values outside reasonable nonprofit range
  if (Math.abs(n) > 1000000) return null;
  return Math.round(n * 100) / 100;
}
function dt(v) {
  if (v == null || v === '') return null;
  const d = new Date(v);
  if (isNaN(d.getTime())) return null;
  const y = d.getUTCFullYear();
  if (y < 1753 || y > 9999) return null; // SQL Server DATETIME2 safe range
  // Return formatted string to avoid toISOString() Z suffix issues
  const pad = (n, w = 2) => String(n).padStart(w, '0');
  return `${pad(y, 4)}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

// SQL literal encoding
function lit(val) {
  if (val === null || val === undefined) return 'NULL';
  if (typeof val === 'number') return String(val);
  if (val instanceof Date) return `'${val.toISOString()}'`;
  return `N'${String(val).replace(/'/g, "''")}'`;
}

// ── generic batch INSERT processor ───────────────────────────────────────────
async function retryQuery(pool, queryStr, label, maxRetries = 5) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await pool.request().query(queryStr);
    } catch (err) {
      if (attempt === maxRetries) throw err;
      const backoff = Math.min(15000 * attempt, 60000);
      console.log(`\n    Query retry ${attempt}/${maxRetries} "${label}": ${err.message.substring(0, 80)}`);
      await wait(backoff);
    }
  }
}

async function batchInsert(pool, label, lineageIds, tableName, colNames, mapFn) {
  if (!lineageIds.length) { console.log(`  - ${label}: skip (no files)`); return 0; }

  const lidList = lineageIds.map(l => `'${l}'`).join(',');
  const colList = colNames.join(', ');
  let lastId = 0, total = 0, batch = 0;

  while (true) {
    const res = await retryQuery(pool,
      `SELECT TOP ${BATCH} id, source_id, data FROM raw.record ` +
      `WHERE lineage_id IN (${lidList}) AND id > ${lastId} ORDER BY id`,
      label);
    if (!res.recordset.length) break;
    lastId = res.recordset[res.recordset.length - 1].id;

    // Parse JSON in JS → build mapped rows
    const mapped = [];
    for (const raw of res.recordset) {
      let d;
      try { d = JSON.parse(raw.data); } catch { continue; }
      const rows = mapFn(d, raw);
      if (!rows) continue;
      for (const row of (Array.isArray(rows) ? rows : [rows])) {
        mapped.push(row);
      }
    }

    // INSERT in chunks
    for (let i = 0; i < mapped.length; i += CHUNK) {
      const chunk = mapped.slice(i, i + CHUNK);
      const values = chunk.map(row =>
        '(' + colNames.map(c => lit(row[c])).join(',') + ')'
      ).join(',\n');

      if (values) {
        for (let attempt = 1; attempt <= 5; attempt++) {
          try {
            await pool.request().batch(`INSERT INTO ${tableName} (${colList}) VALUES ${values}`);
            break;
          } catch (err) {
            if (attempt === 5) throw err;
            const backoff = Math.min(15000 * attempt, 60000);
            console.log(`\n    Insert retry ${attempt}/5 "${label}": ${err.message.substring(0, 80)}`);
            await wait(backoff);
          }
        }
      }
    }

    total += mapped.length;
    batch++;
    process.stdout.write(`\r  ${label}: ${total.toLocaleString()} rows (batch ${batch})    `);

    if (res.recordset.length < BATCH) break;
    await wait(500);
  }

  console.log(`\r  ✓ ${label}: ${total.toLocaleString()} rows                         `);
  return total;
}

// ── read raw records into JS array ───────────────────────────────────────────
async function readRaw(pool, lineageIds, debugLabel) {
  if (!lineageIds.length) return [];
  const lidList = lineageIds.map(l => `'${l}'`).join(',');
  const rows = [];
  let lastId = 0, logged = false;
  while (true) {
    const res = await pool.request().query(
      `SELECT TOP ${BATCH} id, source_id, data FROM raw.record ` +
      `WHERE lineage_id IN (${lidList}) AND id > ${lastId} ORDER BY id`
    );
    if (!res.recordset.length) break;
    lastId = res.recordset[res.recordset.length - 1].id;
    for (const r of res.recordset) {
      try {
        const d = JSON.parse(r.data);
        if (!logged && debugLabel) {
          console.log(`    [${debugLabel}] Sample keys: ${Object.keys(d).slice(0, 10).join(', ')}...`);
          logged = true;
        }
        rows.push({ id: r.id, source_id: r.source_id, d });
      } catch {}
    }
    if (res.recordset.length < BATCH) break;
    await wait(300);
  }
  return rows;
}

// ── column name lists ────────────────────────────────────────────────────────
const personCN = ['source_id', 'source_ref', 'blob_path', 'first_name', 'last_name', 'display_name',
  'email', 'email2', 'email3', 'phone', 'phone2', 'phone3',
  'address_line1', 'address_line2', 'city', 'state', 'zip', 'country', 'company', 'raw_record_id'];

const donationCN = ['person_id', 'amount', 'currency', 'donated_at', 'source_id', 'source_ref',
  'payment_method', 'fund', 'appeal', 'designation'];

const invoiceCN = ['person_id', 'invoice_number', 'total', 'status', 'issued_at', 'source_id', 'source_ref'];

const paymentCN = ['person_id', 'amount', 'payment_date', 'method', 'source_id', 'source_ref'];

const orderCN = ['person_id', 'order_number', 'order_date', 'status', 'source_id', 'source_ref'];

const subCN = ['person_id', 'amount', 'cadence', 'status', 'start_date', 'next_renewal', 'source_id', 'source_ref'];

const noteCN = ['person_id', 'note_text', 'author', 'created_at', 'source_id', 'source_ref'];

const commCN = ['person_id', 'channel', 'direction', 'subject', 'sent_at', 'source_id', 'source_ref'];

const actCN = ['person_id', 'activity_type', 'subject', 'body', 'occurred_at', 'source_id', 'source_ref'];

const tagCN = ['person_id', 'tag_value', 'source_id', 'source_ref'];

// ── main ─────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();
  const fromStep = parseInt(process.argv.find(a => a.startsWith('--from='))?.split('=')[1] || '0', 10);
  const engFrom = parseInt(process.argv.find(a => a.startsWith('--eng-from='))?.split('=')[1] || '0', 10);
  console.log('Step 1.3 — Transform Raw Records into Entities');
  if (fromStep > 0) console.log(`  (resuming from step ${fromStep})`);
  if (engFrom > 0) console.log(`  (engagement sub-step from ${engFrom})`);
  console.log('='.repeat(60));

  const pool = await sql.connect(dbConfig());

  try {
    // ── [0] Ensure source_ref on engagement tables ──
    console.log('\n[0] Ensuring engagement tables have source_ref column...');
    for (const tbl of ['engagement.note', 'engagement.communication', 'engagement.activity', 'engagement.tag']) {
      const [schema, table] = tbl.split('.');
      const check = await pool.request()
        .input('s', sql.VarChar, schema).input('t', sql.VarChar, table)
        .query(`SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA=@s AND TABLE_NAME=@t AND COLUMN_NAME='source_ref'`);
      if (!check.recordset.length) {
        await pool.request().batch(`ALTER TABLE ${tbl} ADD source_ref VARCHAR(256) NULL`);
        console.log(`  Added source_ref to ${tbl}`);
      }
    }

    // ── [1] Clear previous data ──
    if (fromStep > 0) {
      console.log('\n[1] Skipping clear (resume mode)');
    } else {
      console.log('\n[1] Clearing previous transform data...');
      const allClearTables = [
        'commerce.payment', 'commerce.order_line', 'commerce.[order]',
        'commerce.invoice', 'commerce.subscription',
        'giving.donation', 'giving.recurring_plan', 'giving.pledge',
        'engagement.tag', 'engagement.note', 'engagement.communication', 'engagement.activity',
        'staging.person_extract',
      ];
      for (const t of allClearTables) {
        try { await pool.request().query(`DELETE FROM ${t}`); console.log(`  Cleared ${t}`); }
        catch (e) { console.log(`  Skip ${t}: ${e.message.substring(0, 80)}`); }
      }
    }

    // ── Source system IDs ──
    const srcRes = await pool.request().query('SELECT source_id, name FROM meta.source_system');
    const src = {};
    for (const r of srcRes.recordset) src[r.name] = r.source_id;

    // ── File lineage ──
    console.log('  Loading file lineage...');
    const flRes = await pool.request().query(
      `SELECT lineage_id, blob_path, source_id, row_count FROM meta.file_lineage WHERE status = 'loaded'`
    );
    const allFiles = flRes.recordset;
    console.log(`  Files: ${allFiles.length}`);

    const find = (sid, test) => allFiles.filter(f =>
      f.source_id === sid && (typeof test === 'string' ? f.blob_path === test : test(f.blob_path))
    );
    const lids = files => files.map(f => f.lineage_id);
    const trows = files => files.reduce((s, f) => s + f.row_count, 0);

    // ── [1.5] Pre-read DD supplementary data for enrichment ──
    console.log('\n[1.5] Pre-reading DD supplementary data...');
    const ddEmails = find(src.donor_direct, p => p.includes('AccountEmails') || p.includes('Emails'));
    const ddPhones = find(src.donor_direct, p => p.includes('AccountPhones') || p.includes('Phones'));
    const ddAddrs = find(src.donor_direct, p => p.includes('AccountAddresses') || p.includes('Addresses'));

    const emailMap = {};
    const phoneMap = {};
    const addrMap = {};

    if (ddEmails.length) {
      console.log(`  Reading DD Emails (${trows(ddEmails).toLocaleString()} rows from ${ddEmails.length} files)...`);
      const rows = await readRaw(pool, lids(ddEmails), 'DD Emails');
      for (const r of rows) {
        // Try multiple possible field names
        const acct = r.d.AccountNumber || r.d['Account Number'] || r.d.accountNumber;
        const email = Lo(r.d.EmailAddress || r.d['Email Address'] || r.d.email || r.d.Email);
        if (!acct || !email) continue;
        const isPrimary = (r.d.UseAsPrimary || r.d.IsPrimary || '') === 'True';
        const isActive = (r.d.Active || r.d.IsActive || 'True') !== 'False';
        if (!isActive) continue;
        if (!emailMap[acct] || isPrimary) emailMap[acct] = email;
      }
      console.log(`  ✓ Email map: ${Object.keys(emailMap).length} accounts`);
    }

    if (ddPhones.length) {
      console.log(`  Reading DD Phones (${trows(ddPhones).toLocaleString()} rows from ${ddPhones.length} files)...`);
      const rows = await readRaw(pool, lids(ddPhones), 'DD Phones');
      for (const r of rows) {
        const acct = r.d.AccountNumber || r.d['Account Number'] || r.d.accountNumber;
        const phone = S(r.d.NumericTelephoneNumber || r.d.PhoneNumber || r.d['Phone Number'] || r.d.phone || r.d.Phone, 32);
        if (!acct || !phone) continue;
        const isPrimary = (r.d.UseAsPrimary || r.d.IsPrimary || '') === 'True';
        const isActive = (r.d.Active || r.d.IsActive || 'True') !== 'False';
        if (!isActive) continue;
        if (!phoneMap[acct] || isPrimary) phoneMap[acct] = phone;
      }
      console.log(`  ✓ Phone map: ${Object.keys(phoneMap).length} accounts`);
    }

    if (ddAddrs.length) {
      console.log(`  Reading DD Addresses (${trows(ddAddrs).toLocaleString()} rows from ${ddAddrs.length} files)...`);
      const rows = await readRaw(pool, lids(ddAddrs), 'DD Addresses');
      for (const r of rows) {
        const acct = r.d.AccountNumber || r.d['Account Number'] || r.d.accountNumber;
        if (!acct) continue;
        const isPrimary = (r.d.UseAsPrimary || r.d.IsPrimary || '') === 'True';
        const isActive = (r.d.Active || r.d.IsActive || 'True') !== 'False';
        if (!isActive) continue;
        if (!addrMap[acct] || isPrimary) {
          addrMap[acct] = {
            line1: S(r.d.AddressLine1 || r.d['Address Line 1'] || r.d.Street1, 256),
            line2: S(r.d.AddressLine2 || r.d['Address Line 2'] || r.d.Street2, 256),
            city: S(r.d.City || r.d.city, 128),
            state: S(r.d.State || r.d.state || r.d.StateProvince, 64),
            zip: S(r.d.ZipPostal || r.d.Zip || r.d['Postal Code'] || r.d.PostalCode, 20),
            country: S(r.d.Country || r.d.country, 64),
          };
        }
      }
      console.log(`  ✓ Address map: ${Object.keys(addrMap).length} accounts`);
    }

    // ══════════════════════════════════════════════════════════════════════
    if (fromStep <= 2) {
      console.log('\n[2] Extracting persons...');

    // ── KEAP CONTACTS ──
    const keapContacts = find(src.keap, p => p.endsWith('Contact.csv'));
    const kcBP = keapContacts[0] ? keapContacts[0].blob_path : '';
    console.log(`  Keap Contacts: ${keapContacts.length} files, ${trows(keapContacts).toLocaleString()} rows`);
    await batchInsert(pool, 'Keap Contacts', lids(keapContacts),
      'staging.person_extract', personCN, (d, raw) => ({
        source_id: raw.source_id,
        source_ref: S('keap:contact:' + (d.Id || ''), 256),
        blob_path: kcBP,
        first_name: S(d.FirstName, 128), last_name: S(d.LastName, 128),
        display_name: S(((d.FirstName || '') + ' ' + (d.LastName || '')).trim(), 256),
        email: Lo(d.Email), email2: Lo(d.EmailAddress2), email3: Lo(d.EmailAddress3),
        phone: S(d.Phone1, 32), phone2: S(d.Phone2, 32), phone3: S(d.Phone3, 32),
        address_line1: S(d.StreetAddress1, 256), address_line2: S(d.StreetAddress2, 256),
        city: S(d.City, 128), state: S(d.State, 64), zip: S(d.PostalCode, 20),
        country: S(d.Country, 64), company: S(d.Company, 256),
        raw_record_id: raw.id,
      }));

    // ── DD ACCOUNTS (with enrichment) ──
    const ddAccounts = find(src.donor_direct, p => p.includes('PFM_Accounts'));
    await batchInsert(pool, 'DD Accounts', lids(ddAccounts),
      'staging.person_extract', personCN, (d, raw) => {
        const acct = d.AccountNumber || d['Account Number'] || '';
        const addr = addrMap[acct] || {};
        return {
          source_id: raw.source_id,
          source_ref: S('dd:account:' + acct, 256),
          blob_path: 'donor_direct/PFM_Accounts.csv',
          first_name: S(d.FirstName || d['First Name'], 128),
          last_name: S(d.LastName || d['Last Name'], 128),
          display_name: S([(d.Title||''), (d.FirstName||d['First Name']||''), (d.LastName||d['Last Name']||''), (d.Suffix||'')].join(' ').trim(), 256),
          email: emailMap[acct] || null, email2: null, email3: null,
          phone: phoneMap[acct] || null, phone2: null, phone3: null,
          address_line1: addr.line1 || null, address_line2: addr.line2 || null,
          city: addr.city || null, state: addr.state || null,
          zip: addr.zip || null, country: addr.country || null,
          company: S(d.OrganizationName || d['Organization Name'], 256),
          raw_record_id: raw.id,
        };
      });

    // ── DD KINDFUL DONORS ──
    const ddKindful = find(src.donor_direct, p => p.includes('Kindful Donors'));
    await batchInsert(pool, 'DD Kindful Donors', lids(ddKindful),
      'staging.person_extract', personCN, (d, raw) => ({
        source_id: raw.source_id,
        source_ref: S('dd:kindful:' + (d.Id || raw.id), 256),
        blob_path: 'donor_direct/Kindful Donors',
        first_name: S(d['First Name'] || d.FirstName, 128),
        last_name: S(d['Last Name'] || d.LastName, 128),
        display_name: S(d.Name || d['Display Name'], 256),
        email: Lo(d.Email), email2: Lo(d['Email Address 2']), email3: Lo(d['Email Address 3']),
        phone: S(d['Phone 1'] || d.Phone, 32), phone2: S(d['Phone 2'], 32), phone3: S(d['Phone 3'], 32),
        address_line1: S(d['Street Address 1'] || d.Address1, 256),
        address_line2: S(d['Street Address 2'] || d.Address2, 256),
        city: S(d.City, 128), state: S(d.State, 64),
        zip: S(d['Postal Code'] || d.Zip, 20), country: S(d.Country, 64),
        company: S(d['Company Name'] || d.Company, 256),
        raw_record_id: raw.id,
      }));

    // ── STRIPE CUSTOMERS ──
    const stripeCust = find(src.stripe, p => p.includes('Customers') || p.includes('customers'));
    await batchInsert(pool, 'Stripe Customers', lids(stripeCust),
      'staging.person_extract', personCN, (d, raw) => {
        const name = d.name || d.Name || '';
        const email = Lo(d.email || d.Email);
        const phone = S(d.phone || d['phone (metadata)'], 32);
        if (!d.id && !email && !name) return null;
        const parts = name.split(/\s+/);
        return {
          source_id: raw.source_id,
          source_ref: S('stripe:customer:' + (d.id || ''), 256),
          blob_path: 'stripe/Customers',
          first_name: S(parts[0], 128), last_name: S(parts.slice(1).join(' '), 128),
          display_name: S(name, 256),
          email, email2: null, email3: null, phone, phone2: null, phone3: null,
          address_line1: null, address_line2: null, city: null, state: null,
          zip: null, country: null, company: null,
          raw_record_id: raw.id,
        };
      });

    // ── BLOOMERANG TOKENS ──
    const bloomTokens = find(src.bloomerang, p => p.includes('Token-Account Match'));
    await batchInsert(pool, 'Bloomerang Tokens', lids(bloomTokens),
      'staging.person_extract', personCN, (d, raw) => {
        const email = Lo(d['Email Address'] || d.Email);
        if (!email) return null;
        return {
          source_id: raw.source_id,
          source_ref: S('bloomerang:token:' + (d.Token || raw.id), 256),
          blob_path: 'bloomerang/Token-Account Match',
          first_name: null, last_name: S(d['Last Name'] || d.LastName, 128),
          display_name: null, email, email2: null, email3: null,
          phone: null, phone2: null, phone3: null,
          address_line1: null, address_line2: null, city: null, state: null,
          zip: null, country: null, company: null,
          raw_record_id: raw.id,
        };
      });

    // ── KINDFUL REPORTS (non-Leaving) ──
    const kindfulReports = find(src.kindful, p => !p.includes('Leaving'));
    await batchInsert(pool, 'Kindful Reports', lids(kindfulReports),
      'staging.person_extract', personCN, (d, raw) => {
        const email = Lo(d.Email);
        const fn = d['First Name/Org Name'] || d['First Name'];
        if (!email && !fn) return null;
        return {
          source_id: raw.source_id,
          source_ref: S('kindful:' + (email || 'row:' + raw.id), 256),
          blob_path: 'kindful/report',
          first_name: S(fn, 128), last_name: S(d['Last Name'], 128),
          display_name: null, email, email2: null, email3: null,
          phone: S(d.Phone, 32), phone2: null, phone3: null,
          address_line1: null, address_line2: null, city: null, state: null,
          zip: null, country: null, company: null,
          raw_record_id: raw.id,
        };
      });

    // ── KINDFUL LEAVING REPORTS ──
    const kindfulLeaving = find(src.kindful, p => p.includes('Leaving'));
    await batchInsert(pool, 'Kindful Leaving', lids(kindfulLeaving),
      'staging.person_extract', personCN, (d, raw) => {
        const email = Lo(d.Email);
        if (!email) return null;
        return {
          source_id: raw.source_id,
          source_ref: S('kindful:leaving:' + raw.id, 256),
          blob_path: 'kindful/leaving',
          first_name: S(d['First Name'], 128), last_name: S(d['Last Name'], 128),
          display_name: null, email, email2: null, email3: null,
          phone: null, phone2: null, phone3: null,
          address_line1: null, address_line2: null, city: null, state: null,
          zip: null, country: null, company: null,
          raw_record_id: raw.id,
        };
      });

    // ── TRANSACTION IMPORTS ──
    const txnFiles = find(src.transactions_imports, () => true);
    await batchInsert(pool, 'Txn Import persons', lids(txnFiles),
      'staging.person_extract', personCN, (d, raw) => {
        const email = Lo(d.Email);
        const fn = d['First Name/Org Name'] || d['First Name'];
        if (!email && !fn) return null;
        return {
          source_id: raw.source_id,
          source_ref: S('txn_import:' + (email || 'row:' + raw.id), 256),
          blob_path: 'transactions_imports',
          first_name: S(fn, 128), last_name: S(d['Last Name'], 128),
          display_name: null, email, email2: null, email3: null,
          phone: S(d.Phone, 32), phone2: null, phone3: null,
          address_line1: null, address_line2: null, city: null, state: null,
          zip: null, country: null, company: null,
          raw_record_id: raw.id,
        };
      });

    const pCnt = await pool.request().query('SELECT COUNT(*) AS c FROM staging.person_extract');
    console.log(`  Total person extracts: ${pCnt.recordset[0].c.toLocaleString()}`);

    } else { console.log('\n[2] Skipping persons (resume mode)'); }

    // ══════════════════════════════════════════════════════════════════════
    if (fromStep <= 3) {
    console.log('\n[3] Extracting donations...');

    // ── DD TRANSACTIONS ──
    const ddTxn = find(src.donor_direct, p => p.includes('PFM_Transactions'));
    await batchInsert(pool, 'DD Transactions', lids(ddTxn),
      'giving.donation', donationCN, (d, raw) => {
        const a = amt(d.Amount);
        if (a == null) return null;
        return {
          person_id: null, amount: a, currency: d.CurrencyCode || 'USD',
          donated_at: dt(d.Date), source_id: raw.source_id,
          source_ref: S('dd:txn:' + (d.RecordId || '') + ':acct:' + (d.AccountNumber || ''), 256),
          payment_method: S(d.PaymentType, 64),
          fund: S(d.ProjectCode, 256), appeal: S(d.SourceCode, 256),
          designation: S(d.Subaccount, 256),
        };
      });

    // ── DD DONATION ORDERS ──
    const ddDonOrders = find(src.donor_direct, p => p.includes('Donation Orders'));
    await batchInsert(pool, 'DD Donation Orders', lids(ddDonOrders),
      'giving.donation', donationCN, (d, raw) => {
        const a = amt(d.Amount || d.Total || d['Total Amount']);
        if (a == null) return null;
        return {
          person_id: null, amount: a, currency: 'USD',
          donated_at: dt(d.Date || d['Order Date']), source_id: raw.source_id,
          source_ref: S('dd:donation:' + raw.id, 256),
          payment_method: null, fund: null, appeal: null, designation: null,
        };
      });

    // ── KINDFUL / TXN IMPORTS DONATIONS ──
    const donFiles = [...find(src.kindful, p => !p.includes('Leaving')), ...find(src.transactions_imports, () => true)];
    await batchInsert(pool, 'Kindful/Import Donations', lids(donFiles),
      'giving.donation', donationCN, (d, raw) => {
        const a = amt(d.Amount);
        if (a == null) return null;
        const prefix = raw.source_id === src.kindful ? 'kindful:txn:' : 'txn_import:txn:';
        return {
          person_id: null, amount: a, currency: 'USD',
          donated_at: dt(d['Created At'] || d.Date), source_id: raw.source_id,
          source_ref: S(prefix + raw.id, 256),
          payment_method: null, fund: S(d['Fund Name'] || d.Campaigns, 256),
          appeal: null, designation: null,
        };
      });

    const dCnt = await pool.request().query('SELECT COUNT(*) AS c FROM giving.donation');
    console.log(`  Total donations: ${dCnt.recordset[0].c.toLocaleString()}`);

    } else { console.log('\n[3] Skipping donations (resume mode)'); }

    // ══════════════════════════════════════════════════════════════════════
    if (fromStep <= 4) {
    console.log('\n[4] Extracting commerce records...');

    // ── KEAP INVOICES ──
    const keapInv = find(src.keap, p => p.includes('Invoice') && !p.includes('InvoiceItem') && !p.includes('InvoicePayment'));
    await batchInsert(pool, 'Keap Invoices', lids(keapInv),
      'commerce.invoice', invoiceCN, (d, raw) => ({
        person_id: null, invoice_number: S(d.Id, 64),
        total: amt(d.InvoiceTotal), status: S(d.PayStatus, 32),
        issued_at: dt(d.DateCreated), source_id: raw.source_id,
        source_ref: S('keap:invoice:' + (d.Id || '') + ':contact:' + (d.ContactId || ''), 256),
      }));

    // ── KEAP PAYMENTS ──
    const keapPay = find(src.keap, p => p.includes('Payment') && !p.includes('InvoicePayment') && !p.includes('Saved'));
    await batchInsert(pool, 'Keap Payments', lids(keapPay),
      'commerce.payment', paymentCN, (d, raw) => ({
        person_id: null, amount: amt(d.PayAmt),
        payment_date: dt(d.PayDate), method: S(d.PayType, 64),
        source_id: raw.source_id,
        source_ref: S('keap:payment:' + (d.Id || '') + ':contact:' + (d.ContactId || ''), 256),
      }));

    // ── KEAP ORDERS ──
    const keapOrd = find(src.keap, p => p.includes('Orders known as Jobs'));
    await batchInsert(pool, 'Keap Orders', lids(keapOrd),
      'commerce.[order]', orderCN, (d, raw) => ({
        person_id: null, order_number: S(d.Id, 64),
        order_date: dt(d.DateCreated), status: S(d.OrderStatus, 32),
        source_id: raw.source_id,
        source_ref: S('keap:order:' + (d.Id || '') + ':contact:' + (d.ContactId || ''), 256),
      }));

    // ── KEAP SUBSCRIPTIONS ──
    const keapSubs = find(src.keap, p => p.includes('Subscriptions known as JobRecurring'));
    await batchInsert(pool, 'Keap Subscriptions', lids(keapSubs),
      'commerce.subscription', subCN, (d, raw) => ({
        person_id: null, amount: amt(d.BillingAmt),
        cadence: S(d.BillingCycle || d.Frequency, 32), status: S(d.Status, 32),
        start_date: dt(d.StartDate), next_renewal: dt(d.NextBillDate),
        source_id: raw.source_id,
        source_ref: S('keap:subscription:' + (d.Id || '') + ':contact:' + (d.ContactId || ''), 256),
      }));

    // ── DD RECURRING ──
    const ddRec = find(src.donor_direct, p => p.includes('PFM_Recurring') || p.includes('recurringTransactions'));
    await batchInsert(pool, 'DD Recurring', lids(ddRec),
      'commerce.subscription', subCN, (d, raw) => ({
        person_id: null,
        amount: amt(d.Amount || d.RecurringAmount),
        cadence: S(d.Frequency || d.RecurringFrequency, 32),
        status: S(d.Status || d.RecurringStatus, 32),
        start_date: dt(d.StartDate || d.DateCreated),
        next_renewal: dt(d.NextDate || d.NextTransactionDate),
        source_id: raw.source_id,
        source_ref: S('dd:recurring:' + raw.id, 256),
      }));

    } else { console.log('\n[4] Skipping commerce (resume mode)'); }

    // ══════════════════════════════════════════════════════════════════════
    // engFrom: 0=all, 1=after keap notes, 2=after dd notes, 3=after dd comms,
    //          4=after gb comms, 5=after kindful activity, 6=after keap tags
    if (fromStep <= 5) {
    console.log('\n[5] Extracting engagement records...');
    if (engFrom > 0) {
      console.log(`  Cleaning partial data before resuming from eng sub-step ${engFrom}...`);
      if (engFrom <= 4) await retryQuery(pool, `DELETE FROM engagement.activity WHERE source_ref LIKE 'kindful:%'`, 'clean kindful');
      if (engFrom <= 5) await retryQuery(pool, `DELETE FROM engagement.tag WHERE source_ref LIKE 'keap:tag:%'`, 'clean keap tags');
      if (engFrom <= 6) await retryQuery(pool, `DELETE FROM engagement.tag WHERE source_ref LIKE 'dd:kindful:%'`, 'clean dd tags');
      console.log('  Partial data cleaned.');
    }

    // Ensure ddKindful is available (may have been skipped in resume mode)
    const ddKindfulForTags = (typeof ddKindful !== 'undefined') ? ddKindful : find(src.donor_direct, p => p.includes('Kindful Donors'));

    // ── KEAP NOTES (eng sub-step 0) ──
    if (engFrom < 1) {
    const keapNotes = find(src.keap, p => p.includes('Notes'));
    await batchInsert(pool, 'Keap Notes', lids(keapNotes),
      'engagement.note', noteCN, (d, raw) => {
        const text = [d.ActionDescription, d.CreationNotes].filter(Boolean).join('\n');
        if (!text) return null;
        return {
          person_id: null, note_text: S(text, 4000),
          author: S(((d['First Name'] || '') + ' ' + (d['Last Name'] || '')).trim(), 128),
          created_at: dt(d.ActionDate) || dt(d.CreationDate) || '1900-01-01T00:00:00', source_id: raw.source_id,
          source_ref: S('keap:note:' + raw.id + ':contact:' + (d.ContactId || ''), 256),
        };
      });
    } else { console.log('  Skipping Keap Notes (already done)'); }

    // ── DD ACCOUNT NOTES (eng sub-step 1) ──
    if (engFrom < 2) {
    const ddNotes = find(src.donor_direct, p => p.includes('AccountNotes'));
    await batchInsert(pool, 'DD Account Notes', lids(ddNotes),
      'engagement.note', noteCN, (d, raw) => {
        const text = [d.ShortComment, d.LongComment, d.Description].filter(Boolean).join('\n');
        return {
          person_id: null, note_text: S(text, 4000), author: null,
          created_at: dt(d.Date) || dt(d.CreatedDate) || '1900-01-01T00:00:00', source_id: raw.source_id,
          source_ref: S('dd:note:' + raw.id + ':acct:' + (d.AccountNumber || ''), 256),
        };
      });
    } else { console.log('  Skipping DD Account Notes (already done)'); }

    // ── DD COMMUNICATIONS (eng sub-step 2) ──
    if (engFrom < 3) {
    const ddComms = find(src.donor_direct, p => p.includes('AccountCommunications') || p.includes('Communications'));
    await batchInsert(pool, 'DD Communications', lids(ddComms),
      'engagement.communication', commCN, (d, raw) => ({
        person_id: null, channel: S(d.CommunicationType, 32),
        direction: S(d.InboundOrOutbound, 16), subject: S(d.ShortComment, 512),
        sent_at: dt(d.Date), source_id: raw.source_id,
        source_ref: S('dd:comm:' + raw.id + ':acct:' + (d.AccountNumber || ''), 256),
      }));
    } else { console.log('  Skipping DD Communications (already done)'); }

    // ── GIVEBUTTER COMMUNICATIONS (eng sub-step 3) ──
    if (engFrom < 4) {
    const gbComms = find(src.givebutter, p => p.includes('Communication'));
    if (gbComms.length > 0) {
      await batchInsert(pool, 'Givebutter Communications', lids(gbComms),
        'engagement.communication', commCN, (d, raw) => ({
          person_id: null, channel: S(d.CommunicationType, 32),
          direction: S(d.InboundOrOutbound, 16), subject: S(d.ShortComment, 512),
          sent_at: dt(d.Date), source_id: raw.source_id,
          source_ref: S('gb:comm:' + raw.id, 256),
        }));
    }
    } else { console.log('  Skipping Givebutter Communications (already done)'); }

    // ── KINDFUL ACTIVITY (eng sub-step 4) ──
    if (engFrom < 5) {
    const ddActivity = find(src.donor_direct, p => p.includes('Kindful Activity'));
    if (ddActivity.length > 0) {
      await batchInsert(pool, 'Kindful Activity', lids(ddActivity),
        'engagement.activity', actCN, (d, raw) => {
          if (!d['Activity Type'] && !d['Note Content']) return null;
          return {
            person_id: null, activity_type: S(d['Activity Type'], 64),
            subject: S(d['Note Subject'], 512),
            body: S(d['Note Content'] || d.Comments, 4000),
            occurred_at: dt(d['Created At']), source_id: raw.source_id,
            source_ref: S('kindful:activity:' + raw.id + (d.Email ? ':email:' + Lo(d.Email) : ''), 256),
          };
        });
    }
    } else { console.log('  Skipping Kindful Activity (already done)'); }

    // ── KEAP TAGS (eng sub-step 5) ──
    if (engFrom < 6) {
    const keapTags = find(src.keap, p => p.includes('Tags'));
    await batchInsert(pool, 'Keap Tags', lids(keapTags),
      'engagement.tag', tagCN, (d, raw) => {
        const tagName = S(d.GroupName || d.TagName || d.Name || d.TagCategory, 512);
        if (!tagName) return null;
        return {
          person_id: null, tag_value: tagName,
          source_id: raw.source_id,
          source_ref: S('keap:tag:' + (d.Id || raw.id) + ':contact:' + (d.ContactId || ''), 256),
        };
      });
    } else { console.log('  Skipping Keap Tags (already done)'); }

    // ── DD KINDFUL TAGS (eng sub-step 6, comma-separated in Tags field) ──
    await batchInsert(pool, 'DD Kindful Tags', lids(ddKindfulForTags),
      'engagement.tag', tagCN, (d, raw) => {
        const tags = d.Tags;
        if (!tags) return null;
        const items = String(tags).split(',').map(t => t.trim()).filter(Boolean);
        if (!items.length) return null;
        return items.map(t => ({
          person_id: null, tag_value: S(t, 512),
          source_id: src.donor_direct,
          source_ref: S('dd:kindful:' + (d.Id || raw.id), 256),
        }));
      });

    } else { console.log('\n[5] Skipping engagement (resume mode)'); }

    // ══════════════════════════════════════════════════════════════════════
    console.log('\n' + '='.repeat(60));
    console.log('Transform complete:');
    for (const [tbl, label] of [
      ['staging.person_extract', 'Persons'],
      ['giving.donation', 'Donations'],
      ['commerce.invoice', 'Invoices'],
      ['commerce.payment', 'Payments'],
      ['commerce.[order]', 'Orders'],
      ['commerce.subscription', 'Subscriptions'],
      ['engagement.note', 'Notes'],
      ['engagement.communication', 'Communications'],
      ['engagement.activity', 'Activities'],
      ['engagement.tag', 'Tags'],
    ]) {
      const res = await pool.request().query(`SELECT COUNT(*) AS c FROM ${tbl}`);
      console.log(`  ${label}: ${res.recordset[0].c.toLocaleString()}`);
    }

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
