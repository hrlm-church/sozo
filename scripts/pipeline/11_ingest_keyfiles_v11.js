const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { execSync } = require('child_process');
const { withDb, loadEnvFile } = require('./_db');

loadEnvFile();

const SOURCE_PREFIXES = [
  'bloomerang/',
  'donor_direct/',
  'givebutter/',
  'keap/',
  'kindful/',
  'stripe/',
  'transactions_imports/',
];

const MAX_ROWS_PER_FILE = Number(process.env.SOZO_MAX_ROWS_PER_FILE || 0);

const csvSplit = (line) => {
  const out = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      out.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  out.push(current);
  return out.map((v) => v.trim());
};

const hash = (text) => crypto.createHash('sha256').update(text, 'utf8').digest('hex');

const envRequired = ['SOZO_STORAGE_ACCOUNT', 'SOZO_STORAGE_ACCOUNT_KEY', 'SOZO_STORAGE_RAW_CONTAINER'];
for (const key of envRequired) {
  if (!process.env[key]) {
    console.error(`Missing env var ${key}`);
    process.exit(1);
  }
}

function downloadBlob(blobName, localFile) {
  const cmd = [
    'az storage blob download',
    `--account-name "${process.env.SOZO_STORAGE_ACCOUNT}"`,
    `--account-key "${process.env.SOZO_STORAGE_ACCOUNT_KEY}"`,
    `--container-name "${process.env.SOZO_STORAGE_RAW_CONTAINER}"`,
    `--name "${blobName.replace(/"/g, '\\"')}"`,
    `--file "${localFile}"`,
    '--overwrite',
  ].join(' ');
  execSync(cmd, { stdio: 'ignore' });
}

function listCsvBlobsForPrefix(prefix) {
  const cmd = [
    'az storage blob list',
    `--account-name "${process.env.SOZO_STORAGE_ACCOUNT}"`,
    `--account-key "${process.env.SOZO_STORAGE_ACCOUNT_KEY}"`,
    `--container-name "${process.env.SOZO_STORAGE_RAW_CONTAINER}"`,
    `--prefix "${prefix.replace(/"/g, '\\"')}"`,
    '--query "[?ends_with(name, \'.csv\')].name"',
    '-o json',
  ].join(' ');
  const out = execSync(cmd, { encoding: 'utf8' });
  const arr = JSON.parse(out || '[]');
  return Array.isArray(arr) ? arr : [];
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withRetry(fn, attempts = 3, baseDelayMs = 1200) {
  let lastErr;
  for (let i = 1; i <= attempts; i += 1) {
    try {
      // eslint-disable-next-line no-await-in-loop
      return await fn();
    } catch (err) {
      lastErr = err;
      if (i < attempts) {
        // eslint-disable-next-line no-await-in-loop
        await sleep(baseDelayMs * i);
      }
    }
  }
  throw lastErr;
}

function resolveTargetFiles() {
  const explicit = String(process.env.SOZO_TARGET_FILES || '').trim();
  if (explicit) {
    return explicit.split(',').map((x) => x.trim()).filter(Boolean);
  }

  const all = [];
  for (const prefix of SOURCE_PREFIXES) {
    const files = listCsvBlobsForPrefix(prefix);
    for (const f of files) all.push(f);
  }
  return all;
}

async function ingestFile(pool, blobName) {
  const sourceSystem = blobName.split('/')[0];
  const localFile = path.join(os.tmpdir(), `sozo_v11_${blobName.replace(/[\/\s]/g, '_')}`);
  await withRetry(async () => {
    downloadBlob(blobName, localFile);
  }, 4, 1500);

  const content = fs.readFileSync(localFile, 'utf8');
  const lines = content.split(/\r?\n/).filter((l) => l.length > 0);
  if (lines.length < 2) return { blobName, loaded: 0 };

  const header = csvSplit(lines[0]);
  const dataRows = MAX_ROWS_PER_FILE > 0 ? lines.slice(1, MAX_ROWS_PER_FILE + 1) : lines.slice(1);
  const fileHash = hash(content);

  const exists = await pool.request()
    .input('source_system', sourceSystem)
    .input('file_path', `raw/${blobName}`)
    .input('file_hash', fileHash)
    .query(`
SELECT TOP (1) lineage_id
FROM meta.source_file_lineage
WHERE source_system=@source_system
  AND file_path=@file_path
  AND file_hash=@file_hash
  AND status='loaded'
ORDER BY ingested_at DESC;
`);
  if (exists.recordset.length) {
    return { blobName, loaded: 0, skipped: true };
  }

  const lineageRes = await pool
    .request()
    .input('source_system', sourceSystem)
    .input('file_path', `raw/${blobName}`)
    .input('file_hash', fileHash)
    .input('row_count', dataRows.length)
    .batch(`
DECLARE @batch_id UNIQUEIDENTIFIER = NEWID();
DECLARE @lineage_id UNIQUEIDENTIFIER = NEWID();
INSERT INTO meta.source_file_lineage(lineage_id,batch_id,source_system,file_path,file_hash,row_count,status)
VALUES(@lineage_id,@batch_id,@source_system,@file_path,@file_hash,@row_count,'loaded');
SELECT @batch_id AS batch_id, @lineage_id AS lineage_id;
`);

  const batchId = lineageRes.recordset[0].batch_id;
  const lineageId = lineageRes.recordset[0].lineage_id;

  let loaded = 0;
  for (let i = 0; i < dataRows.length; i += 1) {
    const rawLine = dataRows[i];
    const values = csvSplit(rawLine);
    const payload = {};
    for (let c = 0; c < header.length; c += 1) payload[header[c] || `col_${c + 1}`] = values[c] ?? null;

    const sourceRecordId = `${sourceSystem}:${blobName}:${i + 2}`;
    const recordHash = hash(rawLine);

    // eslint-disable-next-line no-await-in-loop
    await pool
      .request()
      .input('batch_id', batchId)
      .input('lineage_id', lineageId)
      .input('source_system', sourceSystem)
      .input('file_path', `raw/${blobName}`)
      .input('row_number', i + 2)
      .input('source_record_id', sourceRecordId)
      .input('record_hash', recordHash)
      .input('record_json', JSON.stringify(payload))
      .batch(`
INSERT INTO bronze.raw_record(batch_id,lineage_id,source_system,file_path,row_number,source_record_id,record_hash,record_json)
VALUES(@batch_id,@lineage_id,@source_system,@file_path,@row_number,@source_record_id,@record_hash,@record_json);
`);

    loaded += 1;
  }

  return { blobName, loaded, skipped: false };
}

async function main() {
  await withDb(async (pool) => {
    const targetFiles = resolveTargetFiles();
    if (!targetFiles.length) {
      console.log('WARN: no target CSV files found for ingestion.');
      return;
    }

    const results = [];
    for (const blobName of targetFiles) {
      try {
        // eslint-disable-next-line no-await-in-loop
        const r = await ingestFile(pool, blobName);
        results.push(r);
        console.log(`OK: ${blobName} -> ${r.skipped ? 'skipped(already_loaded)' : `${r.loaded} rows`}`);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.log(`ERROR: ${blobName} -> ${message}`);
        results.push({ blobName, loaded: 0, skipped: false, failed: true, error: message });
      }
    }
    const total = results.reduce((a, r) => a + (r.loaded || 0), 0);
    const failed = results.filter((r) => r.failed).length;
    const skipped = results.filter((r) => r.skipped).length;
    console.log(`OK: key-file ingestion complete, total rows=${total}, skipped=${skipped}, failed=${failed}`);
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
