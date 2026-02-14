/**
 * Load ALL bronze files into Azure SQL (sozov2)
 *
 * Strategy:
 *   - Each source folder → SQL schema (e.g. keap, donor_direct, stripe_import)
 *   - Each file → SQL table with ALL original columns as NVARCHAR(MAX)
 *   - Zero transformation — data goes in exactly as it exists in the file
 *   - CSV and XLSX/XLSB supported
 *
 * Run: node scripts/pipeline/load_bronze_to_sql.js
 */

const fs = require('fs');
const path = require('path');
const sql = require('mssql');
const XLSX = require('xlsx');
const { BlobServiceClient, StorageSharedKeyCredential } = require('@azure/storage-blob');

// ── env ─────────────────────────────────────────────────────────────────────
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(p)) { console.error('.env.local not found'); process.exit(1); }
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

function getPool() {
  return sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 600000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 5, min: 1, idleTimeoutMillis: 30000 },
  });
}

function getContainerClient() {
  const account = 'pfsozo';
  const key = process.env.PFSOZO_STORAGE_KEY;
  const cred = new StorageSharedKeyCredential(account, key);
  const svc = new BlobServiceClient(`https://${account}.blob.core.windows.net`, cred);
  return svc.getContainerClient('bronze');
}

// ── CSV parser (handles quoted fields with newlines) ────────────────────────
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

function splitCsvRows(content) {
  const rows = [];
  let current = '', inQuote = false;
  for (let i = 0; i < content.length; i++) {
    const ch = content[i];
    if (ch === '"') { inQuote = !inQuote; current += ch; }
    else if ((ch === '\n' || ch === '\r') && !inQuote) {
      if (ch === '\r' && content[i + 1] === '\n') i++;
      if (current.length > 0) rows.push(current);
      current = '';
    } else { current += ch; }
  }
  if (current.length > 0) rows.push(current);
  return rows;
}

function parseCsv(content) {
  const rows = splitCsvRows(content);
  if (rows.length < 2) return { headers: [], data: [] };
  const headers = csvSplit(rows[0]).filter(h => h.length > 0);
  const data = [];
  for (let i = 1; i < rows.length; i++) {
    const values = csvSplit(rows[i]);
    const row = {};
    for (let c = 0; c < headers.length; c++) {
      row[headers[c]] = values[c] ?? '';
    }
    data.push(row);
  }
  return { headers, data };
}

// ── XLSX parser ─────────────────────────────────────────────────────────────
function parseXlsx(buffer) {
  const wb = XLSX.read(buffer, { type: 'buffer' });
  // Use first sheet
  const sheetName = wb.SheetNames[0];
  const sheet = wb.Sheets[sheetName];
  const jsonData = XLSX.utils.sheet_to_json(sheet, { defval: '' });
  if (jsonData.length === 0) return { headers: [], data: [], sheetName };
  const headers = Object.keys(jsonData[0]);
  return { headers, data: jsonData, sheetName };
}

// ── Name sanitization ───────────────────────────────────────────────────────
function sanitizeSchemaName(folderName) {
  return folderName
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .substring(0, 64);
}

function sanitizeTableName(fileName) {
  return fileName
    .replace(/\.(csv|xlsx|xlsb|xls)$/i, '')
    .replace(/[^a-zA-Z0-9_]/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .substring(0, 128);
}

function sanitizeColumnName(col) {
  let name = col
    .replace(/[^a-zA-Z0-9_ ]/g, '')
    .replace(/\s+/g, '_')
    .replace(/_+/g, '_')
    .replace(/^_|_$/g, '')
    .substring(0, 128);
  if (!name || /^\d/.test(name)) name = 'col_' + name;
  return name;
}

// ── SQL literal encoding ────────────────────────────────────────────────────
function lit(val) {
  if (val === null || val === undefined || val === '') return 'NULL';
  const s = String(val);
  if (s.length === 0) return 'NULL';
  // Truncate to 4000 chars for NVARCHAR(MAX) safety
  return `N'${s.substring(0, 4000).replace(/'/g, "''")}'`;
}

// ── Download blob ───────────────────────────────────────────────────────────
async function downloadBlob(containerClient, blobName) {
  const blobClient = containerClient.getBlobClient(blobName);
  const downloadRes = await blobClient.download(0);
  const chunks = [];
  for await (const chunk of downloadRes.readableStreamBody) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

// ── Main loader ─────────────────────────────────────────────────────────────
const wait = ms => new Promise(r => setTimeout(r, ms));
const CHUNK = 50; // rows per INSERT statement
const LARGE_FILE_THRESHOLD = 50 * 1024 * 1024; // 50MB — stream instead of buffer

// Insert a batch of rows into SQL
async function insertBatch(pool, schema, tableName, colList, sanHeaders, rows) {
  const values = rows.map(row => {
    const vals = sanHeaders.map(h => lit(row[h.original]));
    return '(' + vals.join(',') + ')';
  }).join(',\n');

  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      await pool.request().batch(`INSERT INTO [${schema}].[${tableName}] (${colList}) VALUES ${values}`);
      return;
    } catch (err) {
      if (attempt === 5) throw err;
      const backoff = Math.min(10000 * attempt, 60000);
      console.log(`    Retry ${attempt}/5: ${err.message.substring(0, 80)}`);
      await wait(backoff);
    }
  }
}

// Stream-parse large CSV from buffer in chunks to avoid OOM
async function loadLargeCsv(pool, buf, schema, tableName) {
  const content = buf.toString('utf8');
  buf = null; // free buffer

  // Extract header line
  let headerEnd = 0;
  for (let i = 0; i < content.length; i++) {
    if (content[i] === '\n' || content[i] === '\r') {
      headerEnd = i;
      break;
    }
  }
  const headerLine = content.substring(0, headerEnd);
  const headers = csvSplit(headerLine).filter(h => h.length > 0);

  // Sanitize column names
  const colMap = {};
  const sanHeaders = [];
  for (const h of headers) {
    let san = sanitizeColumnName(h);
    if (colMap[san.toLowerCase()]) {
      let i = 2;
      while (colMap[(san + '_' + i).toLowerCase()]) i++;
      san = san + '_' + i;
    }
    colMap[san.toLowerCase()] = true;
    sanHeaders.push({ original: h, sanitized: san });
  }

  // Create table
  const colDefs = sanHeaders.map(h => `[${h.sanitized}] NVARCHAR(MAX) NULL`).join(',\n    ');
  await pool.request().batch(`
    IF OBJECT_ID('${schema}.${tableName}', 'U') IS NOT NULL DROP TABLE [${schema}].[${tableName}];
    CREATE TABLE [${schema}].[${tableName}] (
    _row_id INT IDENTITY(1,1) PRIMARY KEY,
    ${colDefs}
    );
  `);
  console.log(`    Created [${schema}].[${tableName}] — ${sanHeaders.length} columns (streaming)`);

  const colList = sanHeaders.map(h => `[${h.sanitized}]`).join(',');
  let inserted = 0;
  let batch = [];

  // Stream through content row by row
  let pos = headerEnd + 1;
  if (content[headerEnd] === '\r' && content[headerEnd + 1] === '\n') pos = headerEnd + 2;
  let current = '', inQuote = false;

  for (let i = pos; i < content.length; i++) {
    const ch = content[i];
    if (ch === '"') { inQuote = !inQuote; current += ch; }
    else if ((ch === '\n' || ch === '\r') && !inQuote) {
      if (ch === '\r' && content[i + 1] === '\n') i++;
      if (current.length > 0) {
        const values = csvSplit(current);
        const row = {};
        for (let c = 0; c < headers.length; c++) {
          row[headers[c]] = values[c] ?? '';
        }
        batch.push(row);

        if (batch.length >= CHUNK) {
          await insertBatch(pool, schema, tableName, colList, sanHeaders, batch);
          inserted += batch.length;
          batch = [];
          if (inserted % 5000 === 0) {
            process.stdout.write(`\r    Rows: ${inserted.toLocaleString()}`);
          }
        }
      }
      current = '';
    } else { current += ch; }
  }
  // Last row
  if (current.length > 0) {
    const values = csvSplit(current);
    const row = {};
    for (let c = 0; c < headers.length; c++) {
      row[headers[c]] = values[c] ?? '';
    }
    batch.push(row);
  }
  if (batch.length > 0) {
    await insertBatch(pool, schema, tableName, colList, sanHeaders, batch);
    inserted += batch.length;
  }

  console.log(`\r    ✓ ${inserted.toLocaleString()} rows loaded into [${schema}].[${tableName}]                `);
  return { rows: inserted, table: `${schema}.${tableName}`, sanHeaders };
}

async function loadFile(pool, containerClient, blobName, schema) {
  const ext = path.extname(blobName).toLowerCase();
  const fileName = path.basename(blobName);

  // Skip non-data files
  if (['.pdf', '.url', '.png', '.jpg', '.svg'].includes(ext)) {
    console.log(`  SKIP (not data): ${blobName}`);
    return { rows: 0, skipped: true };
  }
  if (fileName === '.DS_Store') return { rows: 0, skipped: true };

  console.log(`\n  Loading: ${blobName}`);
  const buf = await downloadBlob(containerClient, blobName);
  console.log(`    Downloaded: ${(buf.length / 1024).toFixed(0)} KB`);

  // Build table name from subfolder + filename
  const parts = blobName.split('/');
  let tableName;
  if (parts.length > 2) {
    const subPath = parts.slice(1, -1).join('_');
    tableName = sanitizeTableName(subPath + '_' + fileName);
  } else {
    tableName = sanitizeTableName(fileName);
  }

  // For large CSVs, use streaming approach to avoid OOM
  if (ext === '.csv' && buf.length > LARGE_FILE_THRESHOLD) {
    console.log(`    Large file (${(buf.length / 1024 / 1024).toFixed(0)} MB) — using streaming parser`);
    return await loadLargeCsv(pool, buf, schema, tableName);
  }

  let headers, data, sheetName;

  if (ext === '.csv') {
    const content = buf.toString('utf8');
    ({ headers, data } = parseCsv(content));
  } else if (ext === '.xlsx' || ext === '.xlsb' || ext === '.xls') {
    ({ headers, data, sheetName } = parseXlsx(buf));
    if (sheetName) console.log(`    Sheet: ${sheetName}`);
  } else {
    console.log(`    SKIP (unsupported format: ${ext})`);
    return { rows: 0, skipped: true };
  }

  if (headers.length === 0 || data.length === 0) {
    console.log(`    SKIP (empty file)`);
    return { rows: 0, skipped: true };
  }

  // Sanitize column names and deduplicate
  const colMap = {};
  const sanHeaders = [];
  for (const h of headers) {
    let san = sanitizeColumnName(h);
    // Deduplicate
    if (colMap[san.toLowerCase()]) {
      let i = 2;
      while (colMap[(san + '_' + i).toLowerCase()]) i++;
      san = san + '_' + i;
    }
    colMap[san.toLowerCase()] = true;
    sanHeaders.push({ original: h, sanitized: san });
  }

  // Create table
  const colDefs = sanHeaders.map(h => `[${h.sanitized}] NVARCHAR(MAX) NULL`).join(',\n    ');
  const createSql = `
    IF OBJECT_ID('${schema}.${tableName}', 'U') IS NOT NULL DROP TABLE [${schema}].[${tableName}];
    CREATE TABLE [${schema}].[${tableName}] (
    _row_id INT IDENTITY(1,1) PRIMARY KEY,
    ${colDefs}
    );
  `;
  await pool.request().batch(createSql);
  console.log(`    Created [${schema}].[${tableName}] — ${sanHeaders.length} columns`);

  // Insert data in chunks
  const colList = sanHeaders.map(h => `[${h.sanitized}]`).join(',');
  let inserted = 0;

  for (let i = 0; i < data.length; i += CHUNK) {
    const chunk = data.slice(i, i + CHUNK);
    await insertBatch(pool, schema, tableName, colList, sanHeaders, chunk);

    inserted += chunk.length;
    if (inserted % 5000 === 0 || i + CHUNK >= data.length) {
      process.stdout.write(`\r    Rows: ${inserted.toLocaleString()} / ${data.length.toLocaleString()}`);
    }
  }
  console.log(`\r    ✓ ${inserted.toLocaleString()} rows loaded into [${schema}].[${tableName}]                `);
  return { rows: inserted, table: `${schema}.${tableName}` };
}

// ── Compute expected table name for a blob (mirrors loadFile logic) ────────
function blobToTableInfo(blobName) {
  const ext = path.extname(blobName).toLowerCase();
  const fileName = path.basename(blobName);
  if (['.pdf', '.url', '.png', '.jpg', '.svg'].includes(ext)) return null;
  if (fileName === '.DS_Store') return null;
  if (!['.csv', '.xlsx', '.xlsb', '.xls'].includes(ext)) return null;

  const parts = blobName.split('/');
  const schema = sanitizeSchemaName(parts[0]);
  let tableName;
  if (parts.length > 2) {
    const subPath = parts.slice(1, -1).join('_');
    tableName = sanitizeTableName(subPath + '_' + fileName);
  } else {
    tableName = sanitizeTableName(fileName);
  }
  return { schema, tableName };
}

// ── main ────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();
  const resume = process.argv.includes('--resume');
  console.log(`Bronze → SQL Loader (sozov2)${resume ? ' [RESUME MODE]' : ''}`);
  console.log('='.repeat(60));

  const pool = await getPool();
  const containerClient = getContainerClient();

  // If resume, get existing tables + row counts
  const existing = {};
  if (resume) {
    const res = await pool.request().query(`
      SELECT s.name AS sch, t.name AS tbl, SUM(p.rows) AS cnt
      FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
      GROUP BY s.name, t.name
    `);
    for (const r of res.recordset) {
      existing[`${r.sch}.${r.tbl}`] = r.cnt;
    }
    console.log(`Found ${Object.keys(existing).length} existing tables in database`);
  }

  // List all blobs in bronze
  const blobs = [];
  for await (const blob of containerClient.listBlobsFlat()) {
    if (blob.name === '.DS_Store' || blob.name.endsWith('/.DS_Store')) continue;
    blobs.push({ name: blob.name, size: blob.properties.contentLength });
  }
  console.log(`Found ${blobs.length} blobs in bronze`);

  // Group by top-level folder → schema
  const folderGroups = {};
  for (const b of blobs) {
    const parts = b.name.split('/');
    const folder = parts[0];
    if (!folderGroups[folder]) folderGroups[folder] = [];
    folderGroups[folder].push(b);
  }

  console.log(`\nFolders → Schemas:`);
  for (const [folder, files] of Object.entries(folderGroups)) {
    const schema = sanitizeSchemaName(folder);
    console.log(`  ${folder} → [${schema}] (${files.length} files)`);
  }

  // Create schemas and load files
  const results = [];
  let skippedExisting = 0;
  for (const [folder, files] of Object.entries(folderGroups)) {
    const schema = sanitizeSchemaName(folder);
    console.log(`\n${'═'.repeat(60)}`);
    console.log(`Schema: [${schema}] (from "${folder}")`);

    // Create schema if not exists
    await pool.request().batch(`IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '${schema}') EXEC('CREATE SCHEMA [${schema}]')`);

    for (const blob of files.sort((a, b) => a.name.localeCompare(b.name))) {
      const blobName = blob.name;

      // Resume check: skip tables that already exist with rows
      if (resume) {
        const info = blobToTableInfo(blobName);
        if (info) {
          const key = `${info.schema}.${info.tableName}`;
          const existingRows = existing[key];
          if (existingRows > 0) {
            // Check if the blob size suggests the table is complete
            // For large files (>1MB), check if row count seems reasonable
            // A partial table (like hb840_Contact with 13900 vs ~83K) should be re-loaded
            const blobSizeKB = (blob.size || 0) / 1024;
            const rowsPerKB = existingRows / Math.max(blobSizeKB, 1);
            // If file is big (>500KB) and row density is suspiciously low (<0.5 rows/KB), re-load
            const suspectPartial = blobSizeKB > 500 && rowsPerKB < 0.5;
            if (!suspectPartial) {
              console.log(`  SKIP (exists): [${key}] — ${existingRows.toLocaleString()} rows`);
              skippedExisting++;
              results.push({ blob: blobName, rows: existingRows, table: key, resumed: true });
              continue;
            }
            console.log(`  RE-LOAD (partial): [${key}] — only ${existingRows.toLocaleString()} rows for ${(blobSizeKB/1024).toFixed(1)} MB file`);
          }
        }
      }

      try {
        const result = await loadFile(pool, containerClient, blobName, schema);
        if (!result.skipped) {
          results.push({ blob: blobName, ...result });
        }
      } catch (err) {
        console.error(`\n    ERROR loading ${blobName}: ${err.message}`);
        results.push({ blob: blobName, rows: 0, error: err.message });
      }
      await wait(300); // DTU breathing room
    }
  }

  // Summary
  console.log('\n' + '='.repeat(60));
  console.log('LOAD SUMMARY');
  console.log('='.repeat(60));
  let totalRows = 0, totalTables = 0, errors = 0, skipped = 0;
  for (const r of results) {
    if (r.error) {
      console.log(`  ✗ ${r.blob}: ${r.error.substring(0, 80)}`);
      errors++;
    } else if (r.resumed) {
      totalRows += r.rows;
      totalTables++;
      skipped++;
    } else {
      console.log(`  ✓ ${r.table}: ${r.rows.toLocaleString()} rows`);
      totalRows += r.rows;
      totalTables++;
    }
  }
  if (skippedExisting > 0) console.log(`  (${skippedExisting} tables skipped — already loaded)`);
  console.log(`\nTotal: ${totalTables} tables, ${totalRows.toLocaleString()} rows, ${errors} errors`);

  // Verify
  const tableCheck = await pool.request().query(`
    SELECT s.name AS sch, t.name AS tbl, SUM(p.rows) AS cnt
    FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0,1)
    GROUP BY s.name, t.name
    ORDER BY s.name, t.name
  `);
  console.log(`\nDatabase tables:`);
  for (const r of tableCheck.recordset) {
    console.log(`  [${r.sch}].[${r.tbl}]: ${r.cnt.toLocaleString()} rows`);
  }

  await pool.close();
  console.log('\nDone.');
}

main().catch(err => {
  console.error('FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
