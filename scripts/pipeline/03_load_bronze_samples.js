const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { withDb } = require('./_db');

const SOURCES = [
  'bloomerang',
  'donor_direct',
  'givebutter',
  'keap',
  'kindful',
  'stripe',
  'transactions_imports',
];

const SAMPLE_DIR = path.join(process.cwd(), 'data', 'samples');

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

async function ingestSource(pool, source) {
  const file = path.join(SAMPLE_DIR, `${source}.csv`);
  if (!fs.existsSync(file)) {
    console.log(`WARN: sample missing for ${source} (${file})`);
    return { source, loaded: 0, skipped: true };
  }

  const content = fs.readFileSync(file, 'utf8');
  const lines = content.split(/\r?\n/).filter((l) => l.length > 0);
  if (lines.length < 2) {
    console.log(`WARN: no data rows in ${file}`);
    return { source, loaded: 0, skipped: true };
  }

  const header = csvSplit(lines[0]);
  const dataRows = lines.slice(1);
  const fileHash = hash(content);

  const lineageRes = await pool
    .request()
    .input('source_system', source)
    .input('file_path', `raw/${source}/sample/${source}.csv`)
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
    for (let c = 0; c < header.length; c += 1) {
      payload[header[c] || `col_${c + 1}`] = values[c] ?? null;
    }

    const sourceRecordId = `${source}:${path.basename(file)}:${i + 2}`;
    const recordHash = hash(rawLine);

    await pool
      .request()
      .input('batch_id', batchId)
      .input('lineage_id', lineageId)
      .input('source_system', source)
      .input('file_path', `raw/${source}/sample/${source}.csv`)
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

  return { source, loaded, skipped: false };
}

async function main() {
  await withDb(async (pool) => {
    const results = [];
    for (const source of SOURCES) {
      // eslint-disable-next-line no-await-in-loop
      results.push(await ingestSource(pool, source));
    }

    console.log('OK: bronze sample ingestion summary');
    for (const r of results) {
      console.log(`- ${r.source}: ${r.skipped ? 'skipped' : `${r.loaded} rows`}`);
    }
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
