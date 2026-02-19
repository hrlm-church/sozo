/**
 * Load local CSV files into Azure SQL bronze tables (sozov2)
 *
 * Reads from external drive, creates bronze schema + tables, bulk inserts.
 * All columns stored as NVARCHAR(MAX) — zero transformation.
 *
 * Usage: node scripts/pipeline/load_local_to_bronze.js [--from=N] [--only=schema]
 */

const fs = require('fs');
const path = require('path');
const sql = require('mssql');

// ── env ─────────────────────────────────────────────────────────────────────
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(p)) { console.error('.env.local not found'); process.exit(1); }
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

const wait = ms => new Promise(r => setTimeout(r, ms));
const CHUNK = 50;
const WAIT_MS = 300;

const BASE_PATH = '/Volumes/PRO-G40/DIGITAL CULTURE/PURE FREEDOM/DATA PROJECT/RAW DATA SOZO';

const SOURCES = [
  { folder: 'MAILCHIMP', schema: 'mailchimp', files: [
    { file: 'sms_only_audience_export_0b7f5393a7.csv', table: 'sms_only_audience' },
    { file: 'cleaned_email_audience_export_0b7f5393a7.csv', table: 'cleaned_email_audience' },
    { file: 'unsubscribed_email_audience_export_0b7f5393a7.csv', table: 'unsubscribed_email_audience' },
    { file: 'subscribed_email_audience_export_0b7f5393a7.csv', table: 'subscribed_email_audience' },
    { file: 'nonsubscribed_email_audience_export_0b7f5393a7.csv', table: 'nonsubscribed_email_audience' },
  ]},
  { folder: 'STRIPE', schema: 'stripe_charges', files: [
    { file: '2020 Stripe.csv', table: '2020_Stripe' },
    { file: '2021 Stripe.csv', table: '2021_Stripe' },
    { file: '2022 Stripe.csv', table: '2022_Stripe' },
    { file: '2023 stripe.csv', table: '2023_Stripe' },
    { file: '2024 Stripe.csv', table: '2024_Stripe' },
    { file: '2025 stripe.csv', table: '2025_Stripe' },
    { file: '2026 stripe.csv', table: '2026_Stripe' },
  ]},
  { folder: 'WOOCOMMERCE', schema: 'woocommerce', files: [
    { file: 'Woo Commerce Data Base.csv', table: 'customers' },
    { file: 'Woo Commerce Data Base With Line Items.csv', table: 'order_lines' },
  ]},
  { folder: 'TICKERA', schema: 'tickera', files: [
    { file: 'Tickera Data Base.csv', table: 'tickets' },
  ]},
  { folder: 'SUBBLY', schema: 'subbly', files: [
    { file: 'Subbly Data Base.csv', table: 'customers' },
    { file: 'Subbly Data Base With Line Items.csv', table: 'subscriptions' },
  ]},
  { folder: 'SHOPIFY', schema: 'shopify', files: [
    { file: 'customers_export.csv', table: 'customers' },
    { file: 'Shopify Customer Data With Line Items.csv', table: 'order_lines' },
  ]},
];

// ── CSV parser (handles quoted fields, newlines inside quotes) ───────────────
function csvSplit(line) {
  const out = [];
  let cur = '', inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (ch === ',' && !inQ) {
      out.push(cur); cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out.map(v => v.trim());
}

// splitCsvRows removed — use streaming approach in loadFile to avoid OOM

function sanitizeColumnName(col) {
  let name = col
    .replace(/\uFEFF/g, '')  // strip BOM
    .replace(/[^a-zA-Z0-9_ ]/g, '')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .substring(0, 128);
  if (!name || /^\d/.test(name)) name = 'col_' + name;
  return name;
}

function lit(val) {
  if (val === null || val === undefined || val === '') return 'NULL';
  const s = String(val);
  if (s.length === 0) return 'NULL';
  return `N'${s.substring(0, 4000).replace(/'/g, "''")}'`;
}

// ── Insert batch with retry ─────────────────────────────────────────────────
async function insertBatch(pool, schema, tableName, colList, sanHeaders, rows) {
  const values = rows.map(row => {
    const vals = sanHeaders.map(h => lit(row[h.original]));
    return '(' + vals.join(',') + ')';
  }).join(',\n');

  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      await pool.request().batch(
        `INSERT INTO [${schema}].[${tableName}] (${colList}) VALUES ${values}`
      );
      return;
    } catch (err) {
      if (attempt === 5) throw err;
      const backoff = Math.min(10000 * attempt, 60000);
      console.log(`    Retry ${attempt}/5: ${err.message.substring(0, 80)}`);
      await wait(backoff);
    }
  }
}

// ── Load a single CSV file (streaming — no OOM on large files) ──────────────
async function loadFile(pool, filePath, schema, tableName) {
  console.log(`\n  Loading: ${filePath}`);
  console.log(`  Target:  [${schema}].[${tableName}]`);

  if (!fs.existsSync(filePath)) {
    console.log(`    SKIP: file not found`);
    return 0;
  }

  const stream = fs.createReadStream(filePath, { encoding: 'utf8', highWaterMark: 64 * 1024 });
  let buf = '';
  let inQuote = false;
  let skipLF = false;
  let rawHeaders = null;
  let sanHeaders = null;
  let colList = null;
  const colMap = {};
  let inserted = 0;
  let batch = [];

  async function processRow(line) {
    if (!rawHeaders) {
      // First row = headers
      rawHeaders = csvSplit(line).filter(h => h.length > 0);
      sanHeaders = [];
      for (const h of rawHeaders) {
        let san = sanitizeColumnName(h);
        if (colMap[san.toLowerCase()]) {
          let i = 2;
          while (colMap[(san + '_' + i).toLowerCase()]) i++;
          san = san + '_' + i;
        }
        colMap[san.toLowerCase()] = true;
        sanHeaders.push({ original: h, sanitized: san });
      }
      // Create table (drop if exists)
      const colDefs = sanHeaders.map(h => `[${h.sanitized}] NVARCHAR(MAX) NULL`).join(',\n    ');
      await pool.request().batch(`
        IF OBJECT_ID('[${schema}].[${tableName}]', 'U') IS NOT NULL
          DROP TABLE [${schema}].[${tableName}];
        CREATE TABLE [${schema}].[${tableName}] (
          _row_id INT IDENTITY(1,1) PRIMARY KEY,
          ${colDefs}
        );
      `);
      colList = sanHeaders.map(h => `[${h.sanitized}]`).join(',');
      console.log(`    Created table: ${sanHeaders.length} columns`);
      return;
    }

    // Data row
    const values = csvSplit(line);
    const row = {};
    for (let c = 0; c < rawHeaders.length; c++) {
      row[rawHeaders[c]] = values[c] ?? '';
    }
    batch.push(row);

    if (batch.length >= CHUNK) {
      await insertBatch(pool, schema, tableName, colList, sanHeaders, batch);
      inserted += batch.length;
      batch = [];
      await wait(WAIT_MS);
      if (inserted % 5000 < CHUNK) {
        process.stdout.write(`    ${inserted.toLocaleString()} rows...\r`);
      }
    }
  }

  // Stream file character by character — handles quoted newlines correctly
  for await (const chunk of stream) {
    for (let i = 0; i < chunk.length; i++) {
      const ch = chunk[i];
      if (skipLF && ch === '\n') { skipLF = false; continue; }
      skipLF = false;

      if (ch === '"') { inQuote = !inQuote; buf += ch; }
      else if ((ch === '\n' || ch === '\r') && !inQuote) {
        if (ch === '\r') skipLF = true;
        if (buf.length > 0) await processRow(buf);
        buf = '';
      } else {
        buf += ch;
      }
    }
  }

  // Handle final row (no trailing newline)
  if (buf.length > 0) await processRow(buf);

  // Flush remaining batch
  if (batch.length > 0) {
    await insertBatch(pool, schema, tableName, colList, sanHeaders, batch);
    inserted += batch.length;
  }

  if (!rawHeaders) {
    console.log(`    SKIP: no data rows`);
    return 0;
  }

  console.log(`    Done: ${inserted.toLocaleString()} rows inserted`);
  return inserted;
}

// ── Main ────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();

  const args = process.argv.slice(2);
  const fromArg = args.find(a => a.startsWith('--from='));
  const onlyArg = args.find(a => a.startsWith('--only='));
  const fromIdx = fromArg ? parseInt(fromArg.split('=')[1]) : 0;
  const onlySchema = onlyArg ? onlyArg.split('=')[1] : null;

  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 600000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 5, min: 1, idleTimeoutMillis: 30000 },
  });

  console.log('Connected to sozov2.\n');

  let totalRows = 0;
  let sourceIdx = 0;

  for (const source of SOURCES) {
    sourceIdx++;
    if (sourceIdx <= fromIdx) {
      console.log(`Skipping ${source.schema} (--from=${fromIdx})`);
      continue;
    }
    if (onlySchema && source.schema !== onlySchema) continue;

    console.log(`\n${'='.repeat(70)}`);
    console.log(`Source ${sourceIdx}/${SOURCES.length}: ${source.folder} → [${source.schema}]`);
    console.log('='.repeat(70));

    // Create schema if needed
    await pool.request().batch(`
      IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '${source.schema}')
        EXEC('CREATE SCHEMA [${source.schema}]')
    `);

    for (const f of source.files) {
      const filePath = path.join(BASE_PATH, source.folder, f.file);
      try {
        const count = await loadFile(pool, filePath, source.schema, f.table);
        totalRows += count;
      } catch (err) {
        console.error(`    ERROR: ${err.message}`);
      }
    }
  }

  // Summary
  console.log(`\n${'='.repeat(70)}`);
  console.log(`COMPLETE: ${totalRows.toLocaleString()} total rows loaded across all sources`);
  console.log('='.repeat(70));

  // Verify counts
  console.log('\nVerification:');
  for (const source of SOURCES) {
    if (onlySchema && source.schema !== onlySchema) continue;
    for (const f of source.files) {
      try {
        const res = await pool.request().query(
          `SELECT COUNT(*) AS n FROM [${source.schema}].[${f.table}]`
        );
        console.log(`  [${source.schema}].[${f.table}]: ${res.recordset[0].n.toLocaleString()} rows`);
      } catch (err) {
        console.log(`  [${source.schema}].[${f.table}]: ERROR — ${err.message.substring(0, 60)}`);
      }
    }
  }

  await pool.close();
  console.log('\nDone.');
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
