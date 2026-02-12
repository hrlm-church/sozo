/**
 * Step 1.2 — Bulk Ingest Raw Data from Azure Blob Storage
 *
 * Streams every CSV from the `raw` container into raw.record using bulk inserts.
 * Keap dedup: only ingests pass_1_foundation/ files (skips pass_2/pass_3).
 * Tracks file lineage with hash-based skip for re-runs.
 *
 * Run: node scripts/ingest/02_ingest_raw.js
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const readline = require('readline');
const { Readable } = require('stream');
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
    pool: { max: 10, min: 0, idleTimeoutMillis: 10000 },
  };
}

function getContainerClient() {
  const account = process.env.SOZO_STORAGE_ACCOUNT;
  const key = process.env.SOZO_STORAGE_ACCOUNT_KEY;
  const cred = new StorageSharedKeyCredential(account, key);
  const svc = new BlobServiceClient(`https://${account}.blob.core.windows.net`, cred);
  return svc.getContainerClient(process.env.SOZO_STORAGE_RAW_CONTAINER || 'raw');
}

// ── CSV parser ──────────────────────────────────────────────────────────────
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

// ── helpers ─────────────────────────────────────────────────────────────────
function humanSize(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function shouldSkipBlob(blobName) {
  // Skip non-CSV
  if (!blobName.toLowerCase().endsWith('.csv')) return true;
  // Skip .csv.xlsx files
  if (blobName.toLowerCase().endsWith('.csv.xlsx')) return true;
  // Skip Keap pass_2 and pass_3 (duplicates of pass_1_foundation)
  if (blobName.startsWith('keap/pass_2_')) return true;
  if (blobName.startsWith('keap/pass_3_')) return true;
  // Skip __unitystorage
  if (blobName.startsWith('__')) return true;
  return false;
}

function getSourceName(blobName) {
  return blobName.split('/')[0];
}

// ── main ────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();
  console.log('Step 1.2 — Bulk Ingest Raw Data');
  console.log('='.repeat(60));

  const pool = await sql.connect(getDbConfig());
  const containerClient = getContainerClient();

  try {
    // Load source system lookup
    const srcRes = await pool.request().query('SELECT source_id, name FROM meta.source_system');
    const sourceMap = {};
    for (const r of srcRes.recordset) sourceMap[r.name] = r.source_id;

    // Check already-ingested files
    const existingRes = await pool.request().query(
      'SELECT blob_path, file_hash FROM meta.file_lineage WHERE status = \'loaded\''
    );
    const existingFiles = new Map();
    for (const r of existingRes.recordset) existingFiles.set(r.blob_path, r.file_hash);

    // List all blobs
    console.log('\nListing blobs in raw container...');
    const blobs = [];
    for await (const item of containerClient.listBlobsFlat()) {
      if (!shouldSkipBlob(item.name)) {
        blobs.push({ name: item.name, size: item.properties.contentLength });
      }
    }
    console.log(`Found ${blobs.length} CSV files to ingest (after Keap dedup)`);

    const batchId = crypto.randomUUID();
    let totalRows = 0;
    let filesIngested = 0;
    let filesSkipped = 0;

    for (const blob of blobs) {
      const sourceName = getSourceName(blob.name);
      const sourceId = sourceMap[sourceName];
      if (!sourceId) {
        console.log(`  SKIP: ${blob.name} — unknown source "${sourceName}"`);
        filesSkipped++;
        continue;
      }

      // Download blob to buffer
      process.stdout.write(`  ${blob.name} (${humanSize(blob.size)})...`);
      const blobClient = containerClient.getBlobClient(blob.name);
      const downloadRes = await blobClient.download(0);
      const chunks = [];
      for await (const chunk of downloadRes.readableStreamBody) {
        chunks.push(chunk);
      }
      const content = Buffer.concat(chunks).toString('utf8');
      const fileHash = crypto.createHash('sha256').update(content).digest('hex');

      // Check if already ingested with same hash
      if (existingFiles.get(blob.name) === fileHash) {
        console.log(' SKIP (already ingested)');
        filesSkipped++;
        continue;
      }

      // Parse CSV
      const lines = content.split(/\r?\n/).filter(l => l.length > 0);
      if (lines.length < 2) {
        console.log(' SKIP (empty)');
        filesSkipped++;
        continue;
      }

      const headers = csvSplit(lines[0]);
      const dataLines = lines.slice(1);

      // Record lineage
      const lineageId = crypto.randomUUID();
      await pool.request()
        .input('lid', sql.UniqueIdentifier, lineageId)
        .input('bid', sql.UniqueIdentifier, batchId)
        .input('sid', sql.Int, sourceId)
        .input('bp', sql.NVarChar, blob.name)
        .input('fh', sql.VarChar, fileHash)
        .input('rc', sql.Int, dataLines.length)
        .query(`
          INSERT INTO meta.file_lineage (lineage_id, batch_id, source_id, blob_path, file_hash, row_count, status)
          VALUES (@lid, @bid, @sid, @bp, @fh, @rc, 'loading')
        `);

      // Bulk insert rows
      const BATCH_SIZE = 2000;
      let rowsInserted = 0;

      for (let start = 0; start < dataLines.length; start += BATCH_SIZE) {
        const end = Math.min(start + BATCH_SIZE, dataLines.length);
        const table = new sql.Table('raw.record');
        table.create = false;
        table.columns.add('lineage_id', sql.UniqueIdentifier, { nullable: false });
        table.columns.add('source_id', sql.Int, { nullable: false });
        table.columns.add('row_num', sql.Int, { nullable: false });
        table.columns.add('record_hash', sql.VarChar(64), { nullable: false });
        table.columns.add('data', sql.NVarChar(sql.MAX), { nullable: false });

        for (let i = start; i < end; i++) {
          const values = csvSplit(dataLines[i]);
          const payload = {};
          for (let c = 0; c < headers.length; c++) {
            payload[headers[c] || `col_${c + 1}`] = values[c] ?? '';
          }
          const rowHash = crypto.createHash('sha256').update(dataLines[i]).digest('hex');
          table.rows.add(lineageId, sourceId, i + 2, rowHash, JSON.stringify(payload));
        }

        await pool.request().bulk(table);
        rowsInserted += (end - start);
      }

      // Update lineage status
      await pool.request()
        .input('lid', sql.UniqueIdentifier, lineageId)
        .query(`UPDATE meta.file_lineage SET status = 'loaded' WHERE lineage_id = @lid`);

      totalRows += rowsInserted;
      filesIngested++;
      console.log(` ${rowsInserted.toLocaleString()} rows`);
    }

    // Summary
    console.log('\n' + '='.repeat(60));
    console.log('Ingestion complete:');
    console.log(`  Files ingested: ${filesIngested}`);
    console.log(`  Files skipped:  ${filesSkipped}`);
    console.log(`  Total rows:     ${totalRows.toLocaleString()}`);
    console.log(`  Batch ID:       ${batchId}`);

    // Verify
    const countRes = await pool.request().query('SELECT COUNT(*) AS cnt FROM raw.record');
    console.log(`  raw.record total: ${countRes.recordset[0].cnt.toLocaleString()}`);

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
