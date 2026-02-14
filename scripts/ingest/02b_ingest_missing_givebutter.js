/**
 * Ingest 2 missing Givebutter files into raw.record
 *
 * 1. Activity Data (Completed)(Activities Data).csv — has instruction rows before the real header
 * 2. Activity Data (Completed)(Pink Needs Put in Later).csv — has trailing empty columns
 *
 * Run: node scripts/ingest/02b_ingest_missing_givebutter.js
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const sql = require('mssql');
const { BlobServiceClient, StorageSharedKeyCredential } = require('@azure/storage-blob');

// ── env ─────────────────────────────────────────────────────────────────────
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
    requestTimeout: 600000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 5, min: 0, idleTimeoutMillis: 10000 },
  };
}

function getContainerClient() {
  const account = process.env.SOZO_STORAGE_ACCOUNT;
  const key = process.env.SOZO_STORAGE_ACCOUNT_KEY;
  const cred = new StorageSharedKeyCredential(account, key);
  const svc = new BlobServiceClient(`https://${account}.blob.core.windows.net`, cred);
  return svc.getContainerClient(process.env.SOZO_STORAGE_RAW_CONTAINER || 'raw');
}

function csvSplit(line) {
  const out = [];
  let cur = '';
  let inQ = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQ && line[i + 1] === '"') { cur += '"'; i++; }
      else inQ = !inQ;
    } else if (ch === ',' && !inQ) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out.map(v => v.trim());
}

/**
 * Parse multi-line CSV content where fields can contain newlines inside quotes.
 * Returns array of logical rows (each a single string with no unbalanced quotes).
 */
function splitCsvRows(content) {
  const rows = [];
  let current = '';
  let inQuote = false;

  for (let i = 0; i < content.length; i++) {
    const ch = content[i];
    if (ch === '"') {
      inQuote = !inQuote;
      current += ch;
    } else if ((ch === '\n' || ch === '\r') && !inQuote) {
      if (ch === '\r' && content[i + 1] === '\n') i++; // skip \r\n
      if (current.length > 0) rows.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  if (current.length > 0) rows.push(current);
  return rows;
}

async function downloadBlob(containerClient, blobName) {
  const blobClient = containerClient.getBlobClient(blobName);
  const downloadRes = await blobClient.download(0);
  const chunks = [];
  for await (const chunk of downloadRes.readableStreamBody) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks).toString('utf8');
}

async function ingestFile(pool, containerClient, blobName, sourceId, batchId, opts = {}) {
  const { headerRow = 0 } = opts;

  console.log(`\nIngesting: ${blobName}`);
  console.log('  Downloading from Azure Blob...');
  const content = await downloadBlob(containerClient, blobName);
  const fileHash = crypto.createHash('sha256').update(content).digest('hex');

  // Check if already ingested
  const existing = await pool.request()
    .input('bp', sql.NVarChar, blobName)
    .query("SELECT file_hash FROM meta.file_lineage WHERE blob_path = @bp AND status = 'loaded'");
  if (existing.recordset.length > 0 && existing.recordset[0].file_hash === fileHash) {
    console.log('  SKIP — already ingested with same hash');
    return 0;
  }

  // Parse rows (handle multi-line quoted fields)
  const allRows = splitCsvRows(content);
  console.log(`  Total logical rows: ${allRows.length}`);

  // Find the header row
  const headers = csvSplit(allRows[headerRow]).filter(h => h.length > 0);
  const dataRows = allRows.slice(headerRow + 1);
  console.log(`  Header row index: ${headerRow} — ${headers.length} columns: ${headers.join(', ')}`);
  console.log(`  Data rows: ${dataRows.length}`);

  // Record lineage
  const lineageId = crypto.randomUUID();
  await pool.request()
    .input('lid', sql.UniqueIdentifier, lineageId)
    .input('bid', sql.UniqueIdentifier, batchId)
    .input('sid', sql.Int, sourceId)
    .input('bp', sql.NVarChar, blobName)
    .input('fh', sql.VarChar, fileHash)
    .input('rc', sql.Int, dataRows.length)
    .query(`
      INSERT INTO meta.file_lineage (lineage_id, batch_id, source_id, blob_path, file_hash, row_count, status)
      VALUES (@lid, @bid, @sid, @bp, @fh, @rc, 'loading')
    `);

  // Bulk insert in batches
  const BATCH_SIZE = 500;
  let rowsInserted = 0;

  for (let start = 0; start < dataRows.length; start += BATCH_SIZE) {
    const end = Math.min(start + BATCH_SIZE, dataRows.length);
    const table = new sql.Table('raw.record');
    table.create = false;
    table.columns.add('lineage_id', sql.UniqueIdentifier, { nullable: false });
    table.columns.add('source_id', sql.Int, { nullable: false });
    table.columns.add('row_num', sql.Int, { nullable: false });
    table.columns.add('record_hash', sql.VarChar(64), { nullable: false });
    table.columns.add('data', sql.NVarChar(sql.MAX), { nullable: false });

    for (let i = start; i < end; i++) {
      const values = csvSplit(dataRows[i]);
      const payload = {};
      for (let c = 0; c < headers.length; c++) {
        payload[headers[c]] = values[c] ?? '';
      }
      const rowHash = crypto.createHash('sha256').update(dataRows[i]).digest('hex');
      table.rows.add(lineageId, sourceId, i + headerRow + 2, rowHash, JSON.stringify(payload));
    }

    await pool.request().bulk(table);
    rowsInserted += (end - start);
    process.stdout.write(`  Inserted ${rowsInserted} / ${dataRows.length}\r`);
  }

  // Update lineage
  await pool.request()
    .input('lid', sql.UniqueIdentifier, lineageId)
    .query("UPDATE meta.file_lineage SET status = 'loaded' WHERE lineage_id = @lid");

  console.log(`  Done: ${rowsInserted} rows inserted`);
  return rowsInserted;
}

async function main() {
  loadEnv();
  console.log('Ingest Missing Givebutter Files');
  console.log('='.repeat(60));

  const pool = await sql.connect(getDbConfig());
  const containerClient = getContainerClient();

  try {
    // Get givebutter source_id
    const srcRes = await pool.request().query(
      "SELECT source_id FROM meta.source_system WHERE name = 'givebutter'"
    );
    const sourceId = srcRes.recordset[0].source_id;
    console.log(`Givebutter source_id: ${sourceId}`);

    const batchId = crypto.randomUUID();
    let total = 0;

    // File 1: Activities Data — header is on row index 2 (3rd logical row)
    // Row 0: instruction text (multi-line quoted field)
    // Row 1: column descriptions (multi-line quoted fields)
    // Row 2: actual header: Contact ID,External Contact ID,Activity Type,Subject,Note,Date Occurred,Timezone
    // Row 3+: data
    total += await ingestFile(
      pool, containerClient,
      'givebutter/Activity Data (Completed)(Activities Data).csv',
      sourceId, batchId,
      { headerRow: 2 }
    );

    // File 2: Pink — header is row 0, but has hundreds of trailing empty columns
    // The filter(h => h.length > 0) in ingestFile handles this
    total += await ingestFile(
      pool, containerClient,
      'givebutter/Activity Data (Completed)(Pink Needs Put in Later).csv',
      sourceId, batchId,
      { headerRow: 0 }
    );

    console.log('\n' + '='.repeat(60));
    console.log(`Total rows ingested: ${total}`);

    // Verify
    const cnt = await pool.request().query('SELECT COUNT(*) AS cnt FROM raw.record');
    console.log(`raw.record total: ${cnt.recordset[0].cnt.toLocaleString()}`);

    const lineage = await pool.request().query(
      "SELECT blob_path, row_count, status FROM meta.file_lineage WHERE blob_path LIKE 'givebutter/Activity%' ORDER BY blob_path"
    );
    console.log('\nFile lineage:');
    for (const r of lineage.recordset) {
      console.log(`  ${r.status.padEnd(10)} ${r.row_count.toString().padStart(6)} ${r.blob_path}`);
    }
  } finally {
    await pool.close();
  }
}

main().catch(err => {
  console.error('FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
