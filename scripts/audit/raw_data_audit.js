/**
 * Raw Data Deep Audit
 *
 * Connects to Azure Blob Storage, downloads every CSV in the `raw` container,
 * profiles each file, deduplicates Keap passes, performs cross-source analysis,
 * and generates comprehensive reports.
 */

const fs = require('fs');
const path = require('path');
const os = require('os');
const readline = require('readline');
const { BlobServiceClient, StorageSharedKeyCredential } = require('@azure/storage-blob');

// ── env ─────────────────────────────────────────────────────────────────────
function loadEnv() {
  const envPath = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(envPath)) {
    console.error('ERROR: .env.local not found');
    process.exit(1);
  }
  const lines = fs.readFileSync(envPath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) {
      const val = m[2].replace(/^["']|["']$/g, '');
      process.env[m[1]] = val;
    }
  }
}

// ── CSV parser (handles quoted fields) ──────────────────────────────────────
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

// ── type inference ──────────────────────────────────────────────────────────
function inferType(value) {
  if (value === null || value === undefined || value === '') return 'empty';
  if (/^-?\d+$/.test(value)) return 'integer';
  if (/^-?\d+\.\d+$/.test(value)) return 'decimal';
  if (/^\d{4}-\d{2}-\d{2}/.test(value)) return 'date';
  if (/^\d{1,2}\/\d{1,2}\/\d{2,4}/.test(value)) return 'date';
  if (/^(true|false)$/i.test(value)) return 'boolean';
  if (/@/.test(value) && /\./.test(value)) return 'email';
  if (/^\+?\d[\d\s\-().]{7,}$/.test(value)) return 'phone';
  if (/^\$?[\d,]+\.\d{2}$/.test(value)) return 'currency';
  return 'string';
}

function detectFieldRole(header) {
  const h = header.toLowerCase();
  if (/\b(id|_id|key)\b/.test(h)) return 'id';
  if (/email/.test(h)) return 'email';
  if (/phone|mobile|fax/.test(h)) return 'phone';
  if (/\b(first.?name|last.?name|full.?name|display.?name)\b/.test(h)) return 'name';
  if (/amount|total|price|balance|payment|revenue|cost/.test(h)) return 'amount';
  if (/date|created|updated|modified|time|_at\b|_on\b/.test(h)) return 'date';
  if (/address|street|city|state|zip|postal|country/.test(h)) return 'address';
  if (/tag|label|category|type|status/.test(h)) return 'category';
  return null;
}

// ── blob helpers ────────────────────────────────────────────────────────────
function getContainerClient() {
  const account = process.env.SOZO_STORAGE_ACCOUNT;
  const key = process.env.SOZO_STORAGE_ACCOUNT_KEY;
  const container = process.env.SOZO_STORAGE_RAW_CONTAINER || 'raw';
  const cred = new StorageSharedKeyCredential(account, key);
  const blobService = new BlobServiceClient(`https://${account}.blob.core.windows.net`, cred);
  return blobService.getContainerClient(container);
}

async function listCsvBlobs(containerClient) {
  const blobs = [];
  for await (const item of containerClient.listBlobsFlat()) {
    if (item.name.toLowerCase().endsWith('.csv')) {
      blobs.push({ name: item.name, size: item.properties.contentLength });
    }
  }
  return blobs;
}

async function downloadBlobToFile(containerClient, blobName, destPath) {
  const blobClient = containerClient.getBlobClient(blobName);
  await blobClient.downloadToFile(destPath);
}

// ── file profiler ───────────────────────────────────────────────────────────
async function profileCsvFile(filePath, blobName, blobSize) {
  const profile = {
    blob: blobName,
    source: blobName.split('/')[0],
    subPath: blobName.split('/').slice(1).join('/'),
    sizeBytes: blobSize,
    sizeHuman: humanSize(blobSize),
    headers: [],
    rowCount: 0,
    sampleRows: [],
    columns: [],
    keyFields: [],
  };

  return new Promise((resolve, reject) => {
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    let lineNum = 0;
    let headers = [];
    const colStats = []; // { filled: 0, types: {}, uniques: Set (capped) }
    const MAX_UNIQUES = 500;

    rl.on('line', (line) => {
      lineNum++;
      if (lineNum === 1) {
        headers = csvSplit(line);
        profile.headers = headers;
        for (let i = 0; i < headers.length; i++) {
          colStats.push({ filled: 0, types: {}, uniques: new Set() });
        }
        return;
      }

      profile.rowCount++;
      const values = csvSplit(line);

      if (profile.rowCount <= 5) {
        const row = {};
        for (let i = 0; i < headers.length; i++) {
          row[headers[i]] = values[i] ?? '';
        }
        profile.sampleRows.push(row);
      }

      for (let i = 0; i < headers.length; i++) {
        const v = values[i] ?? '';
        if (v !== '') {
          colStats[i].filled++;
          const t = inferType(v);
          colStats[i].types[t] = (colStats[i].types[t] || 0) + 1;
          if (colStats[i].uniques.size < MAX_UNIQUES) {
            colStats[i].uniques.add(v);
          }
        }
      }
    });

    rl.on('close', () => {
      const total = profile.rowCount;
      for (let i = 0; i < headers.length; i++) {
        const s = colStats[i];
        const fillRate = total > 0 ? ((s.filled / total) * 100).toFixed(1) : '0.0';
        const topType = Object.entries(s.types).sort((a, b) => b[1] - a[1])[0];
        const role = detectFieldRole(headers[i]);

        const col = {
          name: headers[i],
          fillRate: `${fillRate}%`,
          filledCount: s.filled,
          uniqueCount: s.uniques.size,
          uniqueCapped: s.uniques.size >= MAX_UNIQUES,
          dominantType: topType ? topType[0] : 'empty',
          typeBreakdown: s.types,
        };
        if (role) col.detectedRole = role;
        profile.columns.push(col);

        if (role) {
          profile.keyFields.push({ column: headers[i], role });
        }
      }
      resolve(profile);
    });

    rl.on('error', reject);
  });
}

function humanSize(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

// ── keap dedup ──────────────────────────────────────────────────────────────
function deduplicateKeapPasses(profiles) {
  const keapProfiles = profiles.filter(p => p.source === 'keap');
  const nonKeap = profiles.filter(p => p.source !== 'keap');

  // Group keap files by base filename
  const groups = {};
  for (const p of keapProfiles) {
    const filename = path.basename(p.blob);
    if (!groups[filename]) groups[filename] = [];
    groups[filename].push(p);
  }

  const canonical = [];
  const duplicateMap = {};

  for (const [filename, copies] of Object.entries(groups)) {
    // Use pass_1_foundation as canonical, or first found
    const primary = copies.find(c => c.blob.includes('pass_1_foundation')) || copies[0];
    canonical.push(primary);
    duplicateMap[filename] = {
      canonical: primary.blob,
      duplicates: copies.filter(c => c !== primary).map(c => c.blob),
      copyCount: copies.length,
    };
  }

  return {
    dedupedProfiles: [...nonKeap, ...canonical],
    allProfiles: profiles,
    keapDuplicateMap: duplicateMap,
  };
}

// ── cross-source analysis ───────────────────────────────────────────────────
function crossSourceAnalysis(profiles) {
  const sourceMap = {};
  for (const p of profiles) {
    if (!sourceMap[p.source]) sourceMap[p.source] = [];
    sourceMap[p.source].push(p);
  }

  // Entity types per source
  const entitySummary = {};
  for (const [source, files] of Object.entries(sourceMap)) {
    entitySummary[source] = {
      fileCount: files.length,
      totalRows: files.reduce((s, f) => s + f.rowCount, 0),
      totalSize: humanSize(files.reduce((s, f) => s + f.sizeBytes, 0)),
      files: files.map(f => ({
        name: f.subPath,
        rows: f.rowCount,
        columns: f.headers.length,
      })),
    };
  }

  // Identity key coverage
  const identityKeys = {};
  for (const p of profiles) {
    for (const kf of p.keyFields) {
      if (['email', 'phone', 'id', 'name'].includes(kf.role)) {
        if (!identityKeys[kf.role]) identityKeys[kf.role] = [];
        identityKeys[kf.role].push({
          source: p.source,
          file: p.subPath,
          column: kf.column,
        });
      }
    }
  }

  // Date range coverage
  const dateColumns = [];
  for (const p of profiles) {
    for (const col of p.columns) {
      if (col.detectedRole === 'date' || col.dominantType === 'date') {
        dateColumns.push({
          source: p.source,
          file: p.subPath,
          column: col.name,
          fillRate: col.fillRate,
        });
      }
    }
  }

  // Amount/financial fields
  const financialFields = [];
  for (const p of profiles) {
    for (const col of p.columns) {
      if (col.detectedRole === 'amount' || col.dominantType === 'currency' || col.dominantType === 'decimal') {
        financialFields.push({
          source: p.source,
          file: p.subPath,
          column: col.name,
          fillRate: col.fillRate,
        });
      }
    }
  }

  return {
    sourceBreakdown: entitySummary,
    identityKeyCoverage: identityKeys,
    dateColumns,
    financialFields,
  };
}

// ── markdown report ─────────────────────────────────────────────────────────
function generateMarkdown(dedupResult, crossAnalysis) {
  const { dedupedProfiles, allProfiles, keapDuplicateMap } = dedupResult;
  const lines = [];
  const ln = (s = '') => lines.push(s);

  ln('# Raw Data Deep Audit Report');
  ln();
  ln(`> Generated: ${new Date().toISOString()}`);
  ln(`> Container: \`raw\` on \`${process.env.SOZO_STORAGE_ACCOUNT}\``);
  ln(`> Total blobs scanned: ${allProfiles.length} CSV files`);
  ln(`> Unique files (after Keap dedup): ${dedupedProfiles.length}`);
  ln();

  // ── Overview table
  ln('## Source Overview');
  ln();
  ln('| Source | Files | Total Rows | Total Size |');
  ln('|--------|------:|----------:|-----------:|');
  for (const [source, info] of Object.entries(crossAnalysis.sourceBreakdown)) {
    ln(`| ${source} | ${info.fileCount} | ${info.totalRows.toLocaleString()} | ${info.totalSize} |`);
  }
  ln();

  // ── Keap dedup
  ln('## Keap Pass Deduplication');
  ln();
  ln('Keap data appears under 3 passes (`pass_1_foundation/`, `pass_2_structure/`, `pass_3_journey/`). These are the **same files** reorganized. We use `pass_1_foundation` as canonical.');
  ln();
  ln('| File | Copies | Canonical Path |');
  ln('|------|-------:|---------------|');
  for (const [filename, info] of Object.entries(keapDuplicateMap)) {
    ln(`| ${filename} | ${info.copyCount} | \`${info.canonical}\` |`);
  }
  ln();

  // ── Per-file detail
  ln('## File Profiles');
  ln();

  for (const p of dedupedProfiles) {
    ln(`### ${p.source} / ${p.subPath}`);
    ln();
    ln(`- **Rows:** ${p.rowCount.toLocaleString()}`);
    ln(`- **Size:** ${p.sizeHuman}`);
    ln(`- **Columns:** ${p.headers.length}`);
    ln();

    // Column table
    ln('| Column | Fill Rate | Uniques | Type | Role |');
    ln('|--------|----------:|--------:|------|------|');
    for (const col of p.columns) {
      const uniq = col.uniqueCapped ? `${col.uniqueCount}+` : `${col.uniqueCount}`;
      ln(`| ${col.name} | ${col.fillRate} | ${uniq} | ${col.dominantType} | ${col.detectedRole || ''} |`);
    }
    ln();

    // Sample rows
    if (p.sampleRows.length > 0) {
      ln('<details><summary>Sample rows (first 5)</summary>');
      ln();
      ln('```json');
      for (const row of p.sampleRows) {
        ln(JSON.stringify(row, null, 2));
      }
      ln('```');
      ln('</details>');
      ln();
    }
  }

  // ── Cross-source analysis
  ln('## Cross-Source Analysis');
  ln();

  ln('### Identity Key Coverage');
  ln();
  for (const [role, fields] of Object.entries(crossAnalysis.identityKeyCoverage)) {
    ln(`**${role}** fields found in:`);
    for (const f of fields) {
      ln(`- \`${f.source}\` → \`${f.file}\` → column \`${f.column}\``);
    }
    ln();
  }

  ln('### Date Columns');
  ln();
  ln('| Source | File | Column | Fill Rate |');
  ln('|--------|------|--------|----------:|');
  for (const d of crossAnalysis.dateColumns) {
    ln(`| ${d.source} | ${d.file} | ${d.column} | ${d.fillRate} |`);
  }
  ln();

  ln('### Financial / Amount Fields');
  ln();
  ln('| Source | File | Column | Fill Rate |');
  ln('|--------|------|--------|----------:|');
  for (const f of crossAnalysis.financialFields) {
    ln(`| ${f.source} | ${f.file} | ${f.column} | ${f.fillRate} |`);
  }
  ln();

  return lines.join('\n');
}

// ── main ────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();
  console.log('Raw Data Deep Audit');
  console.log('='.repeat(60));

  const containerClient = getContainerClient();

  // Step 1: List CSV blobs
  console.log('\n[1/6] Listing CSV blobs in raw container...');
  const blobs = await listCsvBlobs(containerClient);
  console.log(`  Found ${blobs.length} CSV files`);

  // Step 2: Download to temp dir
  const tmpDir = path.join(os.tmpdir(), `sozo-audit-${Date.now()}`);
  fs.mkdirSync(tmpDir, { recursive: true });
  console.log(`\n[2/6] Downloading to ${tmpDir}...`);

  for (const blob of blobs) {
    const dest = path.join(tmpDir, blob.name.replace(/\//g, '__'));
    process.stdout.write(`  ${blob.name} (${humanSize(blob.size)})...`);
    await downloadBlobToFile(containerClient, blob.name, dest);
    blob._localPath = dest;
    console.log(' OK');
  }

  // Step 3: Profile each file
  console.log('\n[3/6] Profiling files...');
  const profiles = [];
  for (const blob of blobs) {
    process.stdout.write(`  ${blob.name}...`);
    const profile = await profileCsvFile(blob._localPath, blob.name, blob.size);
    profiles.push(profile);
    console.log(` ${profile.rowCount.toLocaleString()} rows, ${profile.headers.length} cols`);
  }

  // Step 4: Keap dedup
  console.log('\n[4/6] Deduplicating Keap passes...');
  const dedupResult = deduplicateKeapPasses(profiles);
  const dupCount = Object.values(dedupResult.keapDuplicateMap)
    .reduce((s, d) => s + d.duplicates.length, 0);
  console.log(`  ${dupCount} duplicates identified, ${dedupResult.dedupedProfiles.length} unique files`);

  // Step 5: Cross-source analysis
  console.log('\n[5/6] Cross-source analysis...');
  const crossAnalysis = crossSourceAnalysis(dedupResult.dedupedProfiles);
  console.log(`  Sources: ${Object.keys(crossAnalysis.sourceBreakdown).join(', ')}`);

  // Step 6: Generate reports
  console.log('\n[6/6] Generating reports...');
  const reportsDir = path.join(process.cwd(), 'reports', 'audit');
  fs.mkdirSync(reportsDir, { recursive: true });

  const mdPath = path.join(reportsDir, 'RAW_DATA_DEEP_AUDIT.md');
  const jsonPath = path.join(reportsDir, 'raw_data_deep_audit.json');

  const markdown = generateMarkdown(dedupResult, crossAnalysis);
  fs.writeFileSync(mdPath, markdown, 'utf8');
  console.log(`  Markdown: ${mdPath}`);

  const jsonReport = {
    generated: new Date().toISOString(),
    container: 'raw',
    storageAccount: process.env.SOZO_STORAGE_ACCOUNT,
    totalBlobsScanned: profiles.length,
    uniqueFilesAfterDedup: dedupResult.dedupedProfiles.length,
    keapDuplicateMap: dedupResult.keapDuplicateMap,
    sourceBreakdown: crossAnalysis.sourceBreakdown,
    identityKeyCoverage: crossAnalysis.identityKeyCoverage,
    dateColumns: crossAnalysis.dateColumns,
    financialFields: crossAnalysis.financialFields,
    fileProfiles: dedupResult.dedupedProfiles.map(p => ({
      blob: p.blob,
      source: p.source,
      subPath: p.subPath,
      sizeBytes: p.sizeBytes,
      sizeHuman: p.sizeHuman,
      rowCount: p.rowCount,
      headers: p.headers,
      columns: p.columns,
      keyFields: p.keyFields,
      sampleRows: p.sampleRows,
    })),
  };
  fs.writeFileSync(jsonPath, JSON.stringify(jsonReport, null, 2), 'utf8');
  console.log(`  JSON:     ${jsonPath}`);

  // Cleanup temp files
  for (const blob of blobs) {
    try { fs.unlinkSync(blob._localPath); } catch {}
  }
  try { fs.rmdirSync(tmpDir); } catch {}

  console.log('\nDone.');
}

main().catch(err => {
  console.error('FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
