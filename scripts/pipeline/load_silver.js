/**
 * Load Silver Layer — Bronze → Silver ETL for sozov2
 *
 * Reads from bronze NVARCHAR(MAX) tables, cleans/types, writes to silver.
 * Supports --from=N to resume from step N.
 *
 * Usage: node scripts/pipeline/load_silver.js [--from=N]
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
const BATCH_READ = 1000;   // rows per SELECT
const BATCH_WRITE = 100;   // rows per INSERT
const WAIT_MS = 200;       // ms between INSERTs

// ── Helpers ───────────────────────────────────────────────

/** SQL literal — handles NULL, numbers, strings (N'escaped') */
function lit(v) {
  if (v == null || v === '') return 'NULL';
  if (typeof v === 'number') return String(v);
  if (typeof v === 'boolean') return v ? '1' : '0';
  const s = String(v).replace(/'/g, "''").substring(0, 4000);
  return `N'${s}'`;
}

/** Parse numeric, clamp to DECIMAL(12,2) range */
function amt(v) {
  if (v == null || v === '') return 'NULL';
  const n = parseFloat(String(v).replace(/[$,]/g, ''));
  if (isNaN(n)) return 'NULL';
  const clamped = Math.max(-9999999999.99, Math.min(9999999999.99, n));
  return clamped.toFixed(2);
}

/** Parse integer */
function int(v) {
  if (v == null || v === '') return 'NULL';
  const n = parseInt(String(v), 10);
  if (isNaN(n)) return 'NULL';
  return String(n);
}

/** Parse bit (0/1) */
function bit(v) {
  if (v == null || v === '') return 'NULL';
  const s = String(v).toLowerCase().trim();
  if (s === '1' || s === 'true' || s === 'yes' || s === 'y') return '1';
  if (s === '0' || s === 'false' || s === 'no' || s === 'n') return '0';
  return 'NULL';
}

/** Parse datetime string → 'YYYY-MM-DDThh:mm:ss' or NULL */
function dt(v) {
  if (v == null || v === '') return 'NULL';
  const s = String(v).trim();
  const d = new Date(s);
  if (isNaN(d.getTime())) return 'NULL';
  const y = d.getFullYear();
  if (y < 1753 || y > 9999) return 'NULL';
  const pad = n => String(n).padStart(2, '0');
  return `N'${y}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}'`;
}

/** Parse date-only string → 'YYYY-MM-DD' or NULL */
function dtDate(v) {
  if (v == null || v === '') return 'NULL';
  const s = String(v).trim();
  // Handle MM/DD/YYYY
  const mdy = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (mdy) {
    return `N'${mdy[3]}-${mdy[1].padStart(2, '0')}-${mdy[2].padStart(2, '0')}'`;
  }
  const d = new Date(s);
  if (isNaN(d.getTime())) return 'NULL';
  const y = d.getFullYear();
  if (y < 1753 || y > 9999) return 'NULL';
  const pad = n => String(n).padStart(2, '0');
  return `N'${y}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}'`;
}

/** Convert Excel serial number to YYYY-MM-DD */
function excelDate(v) {
  if (v == null || v === '') return 'NULL';
  const n = parseInt(String(v), 10);
  if (isNaN(n) || n < 1 || n > 200000) return 'NULL';
  // Excel epoch: 1899-12-30
  const d = new Date(Date.UTC(1899, 11, 30 + n));
  const y = d.getUTCFullYear();
  if (y < 1900 || y > 2100) return 'NULL';
  const pad = x => String(x).padStart(2, '0');
  return `N'${y}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}'`;
}

/** Truncate string to N chars */
function trunc(v, n) {
  if (v == null) return null;
  return String(v).substring(0, n) || null;
}

/** Strip leading/trailing whitespace, return null if empty */
function clean(v) {
  if (v == null) return null;
  const s = String(v).trim();
  return s === '' ? null : s;
}

// ── Batch insert engine ──────────────────────────────────

async function batchInsert(pool, table, columns, mapFn, query, opts = {}) {
  const skipRows = opts.skipRows || 0; // skip N header rows
  let lastId = 0;
  let total = 0;
  let skipped = 0;

  while (true) {
    const res = await pool.request().query(
      `SELECT TOP ${BATCH_READ} * FROM ${query} WHERE _row_id > ${lastId} ORDER BY _row_id`
    );
    if (res.recordset.length === 0) break;
    lastId = res.recordset[res.recordset.length - 1]._row_id;

    const rows = [];
    for (const r of res.recordset) {
      if (skipped < skipRows) { skipped++; continue; }
      const mapped = mapFn(r);
      if (mapped) rows.push(mapped);
    }

    // Insert in chunks
    for (let i = 0; i < rows.length; i += BATCH_WRITE) {
      const chunk = rows.slice(i, i + BATCH_WRITE);
      const vals = chunk.map(r => `(${r})`).join(',\n');
      try {
        await pool.request().query(
          `INSERT INTO ${table} (${columns}) VALUES ${vals}`
        );
      } catch (err) {
        // On error, try one at a time
        for (const r of chunk) {
          try {
            await pool.request().query(
              `INSERT INTO ${table} (${columns}) VALUES (${r})`
            );
          } catch (e2) {
            // skip bad row
          }
        }
      }
      await wait(WAIT_MS);
    }
    total += rows.length;
    if (total % 5000 < BATCH_READ) {
      process.stdout.write(`    ${total.toLocaleString()} rows...\r`);
    }
  }
  return total;
}

// ── Step definitions ─────────────────────────────────────

const STEPS = [
  // ── STEP 1: Keap Contacts → silver.contact ──
  {
    name: 'Keap Contacts → silver.contact',
    run: async (pool) => {
      const cols = 'source_system, source_id, first_name, last_name, middle_name, suffix, title, organization_name, email_primary, email_2, email_3, phone_primary, phone_2, address_line1, address_line2, city, state, postal_code, country, date_of_birth, gender, spouse_name, company_id, accepts_marketing, created_at, updated_at';
      return batchInsert(pool, 'silver.contact', cols, (r) => {
        const fn = clean(r.FirstName);
        const ln = clean(r.LastName);
        if (!fn && !ln) return null; // skip empty contacts
        return [
          lit('keap'), lit(r.Id), lit(fn), lit(ln),
          lit(clean(r.MiddleName)), lit(clean(r.Suffix)), lit(clean(r.Title)),
          'NULL', // org name
          lit(clean(r.Email)), lit(clean(r.EmailAddress2)), lit(clean(r.EmailAddress3)),
          lit(clean(r.Phone1)), lit(clean(r.Phone2)),
          lit(trunc(r.StreetAddress1, 500)), lit(trunc(r.StreetAddress2, 500)),
          lit(clean(r.City)), lit(clean(r.State)), lit(clean(r.PostalCode)), lit(clean(r.Country)),
          dtDate(r.Birthday),
          lit(clean(r.MaleorFemale)),
          lit(clean(r.SpouseName)),
          lit(clean(r.CompanyID) === '0' ? null : clean(r.CompanyID)),
          bit(r.AcceptsMarketing),
          dt(r.DateCreated), dt(r.LastUpdated)
        ].join(', ');
      }, 'keap.hb840_Contact');
    }
  },

  // ── STEP 2: DD Accounts → silver.contact ──
  {
    name: 'DD Accounts → silver.contact',
    run: async (pool) => {
      const cols = 'source_system, source_id, first_name, last_name, middle_name, suffix, title, organization_name';
      return batchInsert(pool, 'silver.contact', cols, (r) => {
        return [
          lit('donor_direct'), lit(r.AccountNumber),
          lit(clean(r.FirstName)), lit(clean(r.LastName)),
          lit(clean(r.MiddleName)), lit(clean(r.Suffix)), lit(clean(r.Title)),
          lit(clean(r.OrganizationName))
        ].join(', ');
      }, 'original_files_from_donor_direct.Data_Entered_PFM_Accounts');
    }
  },

  // ── STEP 3: GB Contacts → silver.contact ──
  {
    name: 'GB Contacts → silver.contact',
    run: async (pool) => {
      // Column mapping (by position, skip row 1 which is header descriptions)
      const cols = 'source_system, source_id, first_name, last_name, middle_name, suffix, email_primary, phone_primary, address_line1, address_line2, city, state, postal_code, country, date_of_birth, gender, household_name, dd_number, keap_number, gb_external_id';
      return batchInsert(pool, 'silver.contact', cols, (r) => {
        const vals = Object.values(r);
        // vals[0]=_row_id, [1]=DD#, [2]=Dependents, [3]=Keap#, [4]=Prefix,
        // [5]=FirstName, [6]=MiddleName, [7]=LastName, [8]=Suffix,
        // [9]=AddlEmails, [10]=AddlPhones, [11]=Addr1, [12]=Addr2,
        // [13]=City, [14]=State, [15]=Zip, [16]=PrimaryPhone, [17]=PrimaryEmail,
        // [18]=Country, [19]=Gender, [20]=HouseholdName, [21]=HouseholdEnvelope,
        // [22]=IsPrimary, [23]=Tags, [24]=DOB, [25]=Employer, [26]=JobTitle,
        // [27]=Twitter, [28]=LinkedIn, [29]=Facebook, [30]=Notes,
        // [31]=EmailSub, [32]=PhoneSub
        const fn = clean(vals[5]);
        const ln = clean(vals[7]);
        if (!fn && !ln) return null;
        const ddNum = clean(vals[1]);
        const keapNum = clean(vals[3]);
        return [
          lit('givebutter'),
          lit(ddNum || keapNum || `gb_row_${vals[0]}`), // use DD# or Keap# as source_id
          lit(fn), lit(ln), lit(clean(vals[6])), lit(clean(vals[8])),
          lit(clean(vals[17])), // primary email
          lit(clean(vals[16])), // primary phone
          lit(trunc(vals[11], 500)), lit(trunc(vals[12], 500)),
          lit(clean(vals[13])), lit(clean(vals[14])), lit(clean(vals[15])),
          lit(clean(vals[18])),
          excelDate(vals[24]),    // DOB as Excel serial
          lit(clean(vals[19])),   // gender
          lit(clean(vals[20])),   // household name
          lit(ddNum), lit(keapNum),
          lit(ddNum || keapNum)   // gb external ID
        ].join(', ');
      }, 'data_files_to_send_to_givebutter.Sent_to_Givebutter_Contact_Data',
      { skipRows: 1 }); // skip header row
    }
  },

  // ── STEP 4: Stripe Customers → silver.stripe_customer ──
  {
    name: 'Stripe Customers → silver.stripe_customer',
    run: async (pool) => {
      const cols = 'stripe_id, email, name, phone, old_id, total_spend, payment_count, created_at';
      return batchInsert(pool, 'silver.stripe_customer', cols, (r) => {
        return [
          lit(clean(r.id)), lit(clean(r.Email)), lit(clean(r.Name)),
          lit(clean(r.phone_metadata)), lit(clean(r.old_id_metadata)),
          amt(r.Total_Spend), int(r.Payment_Count),
          dt(r.Created_UTC)
        ].join(', ');
      }, 'stripe_import.Stripe_Customer_IDS');
    }
  },

  // ── STEP 5: DD Transactions → silver.donation ──
  {
    name: 'DD Transactions → silver.donation',
    run: async (pool) => {
      const cols = 'source_system, source_id, contact_source_id, donated_at, amount, currency, fund_code, project_code, source_code, payment_type, short_comment, is_anonymous, is_deductible';
      return batchInsert(pool, 'silver.donation', cols, (r) => {
        const a = parseFloat(String(r.Amount || '0').replace(/[$,]/g, ''));
        if (isNaN(a) || a <= 0) return null; // skip non-positive amounts
        return [
          lit('donor_direct'), lit(r.DocumentNumber || r.RecordId),
          lit(r.AccountNumber),
          dtDate(r.Date), amt(r.Amount), lit('USD'),
          'NULL', // fund_code
          lit(clean(r.ProjectCode)), lit(clean(r.SourceCode)),
          lit(clean(r.PaymentType)),
          lit(trunc(r.ShortComment, 1000)),
          bit(r.Anonymous), bit(r.Deductible || '1')
        ].join(', ');
      }, 'original_files_from_donor_direct.Data_Entered_PFM_Transactions');
    }
  },

  // ── STEP 6: GB Transactions → silver.donation ──
  {
    name: 'GB Transactions → silver.donation',
    run: async (pool) => {
      const cols = 'source_system, source_id, contact_source_id, donated_at, amount, currency, fund_code, campaign_name, payment_type, description';
      return batchInsert(pool, 'silver.donation', cols, (r) => {
        const vals = Object.values(r);
        // vals[0]=_row_id, [1]=CampaignCode, [2]=CampaignTitle, [3]=TeamID, [4]=TeamMemberID
        // [5]=FirstName, [6]=LastName, [7]=Amount, [8]=FundCode,
        // [9]=PaymentMethod, [10]=TransactionDate, [11]=ContactID,
        // [12]=ExternalContactID, [13]=Employer, [14]=Email,
        // [15]=Phone, [16]=Addr1, [17]=Addr2, [18]=City, [19]=State,
        // [20]=Zip, [21]=Country, [22]=DedicationType, [23]=HonoreeName,
        // [24]=RecipName, [25]=RecipEmail, [26]=ThankYouMsg
        const amount = parseFloat(String(vals[7] || '0').replace(/[$,]/g, ''));
        if (isNaN(amount) || amount <= 0) return null;
        return [
          lit('givebutter'), lit(vals[12] || `gb_txn_${vals[0]}`),
          lit(vals[12] || vals[14]), // external contact ID or email as lookup
          excelDate(vals[10]), amt(vals[7]), lit('USD'),
          lit(clean(vals[8])),
          lit(clean(vals[2])), // campaign title
          lit(clean(vals[9])), // payment method
          lit(clean(vals[26]))  // thank you message as description
        ].join(', ');
      }, 'data_files_to_send_to_givebutter.Sent_to_Givebutter_Transactions_Data_Completed',
      { skipRows: 1 });
    }
  },

  // ── STEP 7: Kindful TXN Imports → silver.donation ──
  {
    name: 'Kindful TXN Imports → silver.donation',
    run: async (pool) => {
      // Find all Kindful import tables
      const tables = await pool.request().query(`
        SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS full_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'transaction_imports'
          AND TABLE_NAME LIKE '%Kindful%'
        ORDER BY TABLE_NAME
      `);
      const cols = 'source_system, source_id, contact_source_id, donated_at, amount, currency, project_code, source_code, payment_type, description, short_comment';
      let total = 0;
      for (const t of tables.recordset) {
        console.log(`      ${t.full_name}...`);
        // Check if this table has proper Kindful columns
        const colCheck = await pool.request().query(`
          SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA + '.' + TABLE_NAME = '${t.full_name}'
          AND COLUMN_NAME = 'SourceCode'
        `);
        if (colCheck.recordset.length === 0) {
          console.log(`      (skipping — no SourceCode column)`);
          continue;
        }
        const n = await batchInsert(pool, 'silver.donation', cols, (r) => {
          const a = parseFloat(String(r.TranAmount || r.GiftTotalAmount || '0').replace(/[$,]/g, ''));
          if (isNaN(a) || a <= 0) return null;
          return [
            lit('kindful'), lit(`kindful_${r._row_id}`),
            lit(clean(r.KEAP)), // Keap contact ID as cross-reference
            excelDate(r.DATE),
            amt(r.TranAmount || r.GiftTotalAmount),
            lit(clean(r.TranCurrencyCode) || 'USD'),
            lit(clean(r.GiftProjectCode)),
            lit(clean(r.SourceCode)),
            lit(clean(r.PaymentType)),
            lit(trunc(r.Transaction_Note_Long_Comment, 1000)),
            lit(trunc(r.Transaction_Note_Short_Comment, 1000))
          ].join(', ');
        }, t.full_name);
        total += n;
      }
      return total;
    }
  },

  // ── STEP 8: Keap TXN Imports → silver.donation (positional) ──
  {
    name: 'Keap TXN Imports → silver.donation',
    run: async (pool) => {
      const tables = await pool.request().query(`
        SELECT TABLE_SCHEMA + '.' + TABLE_NAME AS full_name
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'transaction_imports'
          AND TABLE_NAME LIKE '%Keap%'
          AND TABLE_NAME NOT LIKE '%Kindful%'
        ORDER BY TABLE_NAME
      `);
      const cols = 'source_system, source_id, contact_source_id, donated_at, amount, currency, project_code, source_code, payment_type, campaign_name';
      let total = 0;
      for (const t of tables.recordset) {
        console.log(`      ${t.full_name}...`);
        // Check if this has proper SourceCode column (Kindful-style) vs mangled
        const colCheck = await pool.request().query(`
          SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
          WHERE TABLE_SCHEMA + '.' + TABLE_NAME = '${t.full_name}'
          AND COLUMN_NAME = 'SourceCode'
        `);
        if (colCheck.recordset.length > 0) {
          // Has proper column names — treat like Kindful
          const n = await batchInsert(pool, 'silver.donation', cols, (r) => {
            const a = parseFloat(String(r.TranAmount || r.GiftTotalAmount || '0').replace(/[$,]/g, ''));
            if (isNaN(a) || a <= 0) return null;
            return [
              lit('keap_import'), lit(`keap_imp_${r._row_id}`),
              lit(clean(r.KEAP)),
              excelDate(r.DATE), amt(r.TranAmount || r.GiftTotalAmount),
              lit(clean(r.TranCurrencyCode) || 'USD'),
              lit(clean(r.GiftProjectCode)), lit(clean(r.SourceCode)),
              lit(clean(r.PaymentType)), 'NULL'
            ].join(', ');
          }, t.full_name);
          total += n;
        } else {
          // Mangled column names — use positional mapping
          const n = await batchInsert(pool, 'silver.donation', cols, (r) => {
            const vals = Object.values(r);
            // Position: [0]=_row_id, [1]=?, [2]=Amount, [3]=KeapContactID,
            // [4]=?ID, [5]=?LongID, [6]=Source, [7]=AcctType, [8]=Date,
            // [9]=ProjectCode, [10]=SourceName, [11]=Description,
            // [12]=PaymentType, [13]=Currency, [14]=TranAmt, [15]=PayAmt
            if (vals.length < 15) return null;
            const a = parseFloat(String(vals[2] || '0').replace(/[$,]/g, ''));
            if (isNaN(a) || a <= 0) return null;
            return [
              lit('keap_import'), lit(`keap_imp_${vals[0]}`),
              lit(clean(vals[3])), // Keap contact ID
              excelDate(vals[8]),  // date
              amt(vals[2]),        // amount
              lit(clean(vals[13]) || 'USD'),
              lit(clean(vals[9])),  // project code
              lit(clean(vals[10])), // source/campaign name
              lit(clean(vals[12])), // payment type
              lit(clean(vals[10]))  // campaign name
            ].join(', ');
          }, t.full_name);
          total += n;
        }
      }
      return total;
    }
  },

  // ── STEP 9: Misc Donations → silver.donation ──
  {
    name: 'Misc Donations → silver.donation',
    run: async (pool) => {
      let total = 0;
      // Sept 2025 Kindful Transactions
      const cols = await pool.request().query(`
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA='misc' AND TABLE_NAME='Sept_2025_Kindful_Transactions'
        ORDER BY ORDINAL_POSITION
      `);
      const colNames = cols.recordset.map(c => c.COLUMN_NAME);
      console.log(`      misc.Sept_2025_Kindful_Transactions (${colNames.length} cols)...`);

      if (colNames.includes('SourceCode')) {
        const donCols = 'source_system, source_id, contact_source_id, donated_at, amount, currency, project_code, source_code, payment_type';
        total += await batchInsert(pool, 'silver.donation', donCols, (r) => {
          const a = parseFloat(String(r.TranAmount || r.GiftTotalAmount || '0').replace(/[$,]/g, ''));
          if (isNaN(a) || a <= 0) return null;
          return [
            lit('kindful'), lit(`misc_kindful_${r._row_id}`),
            lit(clean(r.KEAP)),
            excelDate(r.DATE), amt(r.TranAmount || r.GiftTotalAmount),
            lit('USD'), lit(clean(r.GiftProjectCode)),
            lit(clean(r.SourceCode)), lit(clean(r.PaymentType))
          ].join(', ');
        }, 'misc.Sept_2025_Kindful_Transactions');
      }
      return total;
    }
  },

  // ── STEP 10: Keap Notes → silver.note ──
  {
    name: 'Keap Notes → silver.note',
    run: async (pool) => {
      const cols = 'source_system, source_id, contact_source_id, note_type, content, created_at, created_by';
      return batchInsert(pool, 'silver.note', cols, (r) => {
        return [
          lit('keap'), lit(r.Id), lit(r.ContactId),
          lit(clean(r.ActionType)),
          lit(trunc(r.ActionDescription || r.CreationNotes, 4000)),
          dt(r.ActionDate),
          lit(trunc(r.First_Name && r.Last_Name ? `${r.First_Name} ${r.Last_Name}` : null, 200))
        ].join(', ');
      }, 'keap.hb840_Notes');
    }
  },

  // ── STEP 11: DD Notes → silver.note ──
  {
    name: 'DD Notes → silver.note',
    run: async (pool) => {
      const cols = 'source_system, source_id, contact_source_id, note_type, subject, content';
      return batchInsert(pool, 'silver.note', cols, (r) => {
        return [
          lit('donor_direct'), lit(`dd_note_${r._row_id}`),
          lit(r.AccountNumber),
          lit(clean(r.NoteType)),
          lit(trunc(r.ShortComment, 500)),
          lit(trunc(r.LongComment || r.Description, 4000))
        ].join(', ');
      }, 'original_files_from_donor_direct.Data_Entered_PFM_AccountNotes');
    }
  },

  // ── STEP 12: DD Communications → silver.communication ──
  {
    name: 'DD Communications → silver.communication',
    run: async (pool) => {
      const cols = 'source_system, contact_source_id, comm_date, comm_type, direction, subject, content, source_code, from_email, to_email';
      return batchInsert(pool, 'silver.communication', cols, (r) => {
        return [
          lit('donor_direct'), lit(r.AccountNumber),
          dtDate(r.Date),
          lit(clean(r.CommunicationType)),
          lit(clean(r.InboundOrOutbound)),
          lit(trunc(r.ShortComment, 500)),
          lit(trunc(r.LongComment, 4000)),
          lit(clean(r.SourceCode)),
          lit(clean(r.FROMEmailAddress)),
          lit(clean(r.ToEmailAddresses))
        ].join(', ');
      }, 'original_files_from_donor_direct.Data_Entered_PFM_AccountCommunications');
    }
  },

  // ── STEP 13: GB Activity → silver.communication ──
  {
    name: 'GB Activity → silver.communication',
    run: async (pool) => {
      const cols = 'source_system, contact_source_id, comm_date, comm_type, subject, content';
      return batchInsert(pool, 'silver.communication', cols, (r) => {
        const vals = Object.values(r);
        // [0]=_row_id, [1]=ContactID, [2]=ExternalContactID, [3]=ActivityType,
        // [4]=Subject, [5]=Note, [6]=DateOccurred, [7]=Timezone
        const extId = clean(vals[2]);
        const contId = clean(vals[1]);
        if (!extId && !contId) return null;
        return [
          lit('givebutter'),
          lit(extId || contId),
          excelDate(vals[6]),
          lit(clean(vals[3])),
          lit(trunc(vals[4], 500)),
          lit(trunc(vals[5], 4000))
        ].join(', ');
      }, 'data_files_to_send_to_givebutter.Activity_Data_Completed',
      { skipRows: 2 }); // skip 2 header rows
    }
  },

  // ── STEP 14: Keap Invoices → silver.invoice ──
  {
    name: 'Keap Invoices → silver.invoice',
    run: async (pool) => {
      const cols = 'keap_id, contact_keap_id, job_id, created_at, due_date, total, total_due, total_paid, pay_status, credit_status, refund_status, invoice_type, description, promo_code, updated_at';
      return batchInsert(pool, 'silver.invoice', cols, (r) => {
        return [
          int(r.Id), int(r.ContactId), int(r.JobId),
          dt(r.DateCreated), dtDate(r.DueDate),
          amt(r.InvoiceTotal), amt(r.TotalDue), amt(r.TotalPaid),
          lit(clean(r.PayStatus)), lit(clean(r.CreditStatus)),
          lit(clean(r.RefundStatus)), lit(clean(r.InvoiceType)),
          lit(trunc(r.Description, 1000)), lit(clean(r.PromoCode)),
          dt(r.LastUpdated)
        ].join(', ');
      }, 'keap.hb840_Invoice');
    }
  },

  // ── STEP 15: Keap Payments → silver.payment ──
  {
    name: 'Keap Payments → silver.payment',
    run: async (pool) => {
      const cols = 'keap_id, contact_keap_id, invoice_keap_id, pay_date, amount, pay_type, pay_note, collection_method, payment_subtype, created_at, updated_at';
      return batchInsert(pool, 'silver.payment', cols, (r) => {
        return [
          int(r.Id), int(r.ContactId), int(r.InvoiceId),
          dt(r.PayDate), amt(r.PayAmt),
          lit(clean(r.PayType)), lit(trunc(r.PayNote, 1000)),
          lit(clean(r.CollectionMethod)), lit(clean(r.PaymentSubType)),
          dt(r.DateCreated), dt(r.LastUpdated)
        ].join(', ');
      }, 'keap.hb840_Payment');
    }
  },

  // ── STEP 16: Keap Orders → silver.order ──
  {
    name: 'Keap Orders → silver.order',
    run: async (pool) => {
      const cols = 'keap_id, contact_keap_id, title, created_at, start_date, due_date, order_type, order_status, source, promo_code, coupon_code, updated_at';
      return batchInsert(pool, 'silver.invoice', cols, (r) => {
        // Wait — this should go to silver.[order], not silver.invoice!
        return null;
      }, 'keap.hb840_Orders_known_as_Jobs');
      // Corrected below
    }
  },

  // ── STEP 17: Keap Order Items → silver.order_item ──
  {
    name: 'Keap Order Items → silver.order_item',
    run: async (pool) => {
      const cols = 'keap_id, order_keap_id, product_keap_id, item_name, qty, cost_per_unit, price_per_unit, item_type, created_at';
      return batchInsert(pool, 'silver.order_item', cols, (r) => {
        return [
          int(r.Id), int(r.OrderId), int(r.ProductId),
          lit(trunc(r.ItemName, 500)), int(r.Qty),
          amt(r.CPU), amt(r.PPU),
          lit(clean(r.ItemType)),
          dt(r.DateCreated)
        ].join(', ');
      }, 'keap.hb840_OrderItem');
    }
  },

  // ── STEP 18: Keap Subscriptions → silver.subscription ──
  {
    name: 'Keap Subscriptions → silver.subscription',
    run: async (pool) => {
      const cols = 'keap_id, contact_keap_id, start_date, end_date, last_bill_date, next_bill_date, billing_amount, billing_cycle, frequency, status, reason_stopped, auto_charge, product_id, created_at, updated_at';
      return batchInsert(pool, 'silver.subscription', cols, (r) => {
        return [
          int(r.Id), int(r.ContactId),
          dtDate(r.StartDate), dtDate(r.EndDate),
          dtDate(r.LastBillDate), dtDate(r.NextBillDate),
          amt(r.BillingAmt), lit(clean(r.BillingCycle)), int(r.Frequency),
          lit(clean(r.Status)), lit(trunc(r.ReasonStopped, 500)),
          bit(r.AutoCharge), int(r.ProductId),
          dt(r.DateCreated), dt(r.LastUpdated)
        ].join(', ');
      }, 'keap.hb840_Subscriptions_known_as_JobRecurring');
    }
  },

  // ── STEP 19: Keap Products → silver.product ──
  {
    name: 'Keap Products → silver.product',
    run: async (pool) => {
      const cols = 'keap_id, name, short_desc, description, price, cost, sku, status, is_digital, shippable, taxable, created_at, updated_at';
      return batchInsert(pool, 'silver.product', cols, (r) => {
        return [
          int(r.Id), lit(trunc(r.ProductName, 500)),
          lit(trunc(r.ProductShortDesc, 1000)), lit(trunc(r.ProductDesc, 4000)),
          amt(r.ProductPrice), amt(r.ProductCost),
          lit(clean(r.Sku)), lit(clean(r.Status)),
          bit(r.IsDigital), bit(r.Shippable), bit(r.Taxable),
          dt(r.DateCreated), dt(r.LastUpdated)
        ].join(', ');
      }, 'keap.hb840_Products');
    }
  },

  // ── STEP 20: Keap Tags → silver.tag ──
  {
    name: 'Keap Tags → silver.tag',
    run: async (pool) => {
      const cols = 'keap_id, group_name, group_description, category_name, category_description, created_at, updated_at';
      return batchInsert(pool, 'silver.tag', cols, (r) => {
        return [
          int(r.Id), lit(trunc(r.GroupName, 500)),
          lit(trunc(r.GroupDescription, 2000)),
          lit(trunc(r.CategoryName, 500)),
          lit(trunc(r.CategoryDescription, 2000)),
          dt(r.DateCreated), dt(r.LastUpdated)
        ].join(', ');
      }, 'keap.hb840_Tags');
    }
  },

  // ── STEP 21: Keap Company → silver.company ──
  {
    name: 'Keap Company → silver.company',
    run: async (pool) => {
      const cols = 'keap_id, name, email, phone, fax, address_line1, address_line2, city, state, postal_code, country, notes, created_at, updated_at';
      return batchInsert(pool, 'silver.company', cols, (r) => {
        return [
          int(r.CompanyId), lit(trunc(r.Company, 500)),
          lit(clean(r.Email)), lit(clean(r.Phone1)),
          lit(clean(r.Fax1)),
          lit(trunc(r.StreetAddress1, 500)), lit(trunc(r.StreetAddress2, 500)),
          lit(clean(r.City)), lit(clean(r.State)),
          lit(clean(r.PostalCode)), lit(clean(r.Country)),
          lit(trunc(r.ContactNotes, 4000)),
          dt(r.DateCreated), dt(r.LastUpdated)
        ].join(', ');
      }, 'keap.hb840_Company');
    }
  },

  // ── STEP 22: DD Emails → silver.contact_email ──
  {
    name: 'DD Emails → silver.contact_email',
    run: async (pool) => {
      const cols = 'source_system, contact_source_id, email_address, email_type, is_primary, is_active';
      return batchInsert(pool, 'silver.contact_email', cols, (r) => {
        if (!clean(r.EmailAddress)) return null;
        return [
          lit('donor_direct'), lit(r.AccountNumber),
          lit(clean(r.EmailAddress)), lit(clean(r.EmailType)),
          bit(r.UseAsPrimary), bit(r.Active)
        ].join(', ');
      }, 'original_files_from_donor_direct.Data_Entered_PFM_AccountEmails');
    }
  },

  // ── STEP 23: DD Phones → silver.contact_phone ──
  {
    name: 'DD Phones → silver.contact_phone',
    run: async (pool) => {
      const cols = 'source_system, contact_source_id, phone_number, phone_type, area_code, is_primary, is_active';
      return batchInsert(pool, 'silver.contact_phone', cols, (r) => {
        const phone = clean(r.TelephoneNumber) || clean(r.Full_Number);
        if (!phone) return null;
        return [
          lit('donor_direct'), lit(r.AccountNumber),
          lit(phone), lit(clean(r.PhoneType)),
          lit(clean(r.AreaCode)),
          bit(r.UseAsPrimary), bit(r.Active)
        ].join(', ');
      }, 'original_files_from_donor_direct.Data_Entered_PFM_AccountPhones_csv');
    }
  },

  // ── STEP 24: DD Addresses → silver.contact_address ──
  {
    name: 'DD Addresses → silver.contact_address',
    run: async (pool) => {
      const cols = 'source_system, contact_source_id, address_line1, address_line2, city, state, postal_code, country';
      return batchInsert(pool, 'silver.contact_address', cols, (r) => {
        if (!clean(r.AddressLine1)) return null;
        return [
          lit('donor_direct'), lit(r.AccountNumber),
          lit(trunc(r.AddressLine1, 500)), lit(trunc(r.AddressLine2, 500)),
          lit(clean(r.City)), lit(clean(r.State)),
          lit(clean(r.ZipPostal)), lit(clean(r.Country))
        ].join(', ');
      }, 'original_files_from_donor_direct.Data_Entered_PFM_AccountAddresses_csv');
    }
  },
];

// Fix step 16 — Keap Orders (was broken)
STEPS[15] = {
  name: 'Keap Orders → silver.order',
  run: async (pool) => {
    const cols = 'keap_id, contact_keap_id, title, created_at, start_date, due_date, order_type, order_status, source, promo_code, coupon_code, updated_at';
    return batchInsert(pool, 'silver.[order]', cols, (r) => {
      return [
        int(r.Id), int(r.ContactId),
        lit(trunc(r.JobTitle, 500)),
        dt(r.DateCreated), dtDate(r.StartDate), dtDate(r.DueDate),
        lit(clean(r.OrderType)), lit(clean(r.OrderStatus)),
        lit(clean(r.Source)),
        lit(clean(r.PromoCode)), lit(clean(r.CouponCodeUsed)),
        dt(r.LastUpdated)
      ].join(', ');
    }, 'keap.hb840_Orders_known_as_Jobs');
  }
};

async function main() {
  loadEnv();
  const fromStep = parseInt((process.argv.find(a => a.startsWith('--from=')) || '--from=1').split('=')[1]) || 1;

  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST, database: 'sozov2',
    user: process.env.SOZO_SQL_USER, password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 120000,
    pool: { max: 3, min: 1 }
  });

  console.log(`Silver ETL — ${STEPS.length} steps (starting from step ${fromStep})\n`);
  const startTime = Date.now();

  for (let i = 0; i < STEPS.length; i++) {
    const step = STEPS[i];
    const stepNum = i + 1;
    if (stepNum < fromStep) {
      console.log(`  [${stepNum}/${STEPS.length}] ${step.name} — SKIPPED`);
      continue;
    }
    const t0 = Date.now();
    console.log(`  [${stepNum}/${STEPS.length}] ${step.name}...`);
    try {
      const count = await step.run(pool);
      const elapsed = ((Date.now() - t0) / 1000).toFixed(1);
      console.log(`    → ${(count || 0).toLocaleString()} rows (${elapsed}s)`);
    } catch (err) {
      console.error(`    FAIL: ${err.message}`);
    }
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nDone in ${totalElapsed} minutes.`);

  // Print summary
  console.log('\nSilver table row counts:');
  const summary = await pool.request().query(`
    SELECT s.name + '.' + t.name AS tbl, SUM(p.rows) AS cnt
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
    WHERE s.name = 'silver'
    GROUP BY s.name, t.name ORDER BY t.name
  `);
  for (const r of summary.recordset) {
    console.log(`  ${r.tbl}: ${r.cnt.toLocaleString()}`);
  }

  await pool.close();
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
