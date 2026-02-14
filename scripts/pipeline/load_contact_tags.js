/**
 * Load Keap Contact-Tag Assignments into silver.contact_tag
 *
 * Phase 1: Stream 838 MB XML from blob → parse with SAX → deduplicate in memory
 * Phase 2: Batch INSERT into SQL (100-row chunks, 200ms waits)
 *
 * The XML is a Sequel Ace dump with <row> elements containing:
 *   TagId, Tag, ContactId, FirstName, LastName, DateApplied
 *
 * Usage:
 *   node scripts/pipeline/load_contact_tags.js
 *   node scripts/pipeline/load_contact_tags.js --skip=50000   # resume insert phase
 */
const fs = require('fs');
const path = require('path');
const sql = require('mssql');
const sax = require('sax');
const { BlobServiceClient, StorageSharedKeyCredential } = require('@azure/storage-blob');

// ── env ─────────────────────────────────────────────────────────────────────
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

const BATCH = 100;
const WAIT_MS = 200;
const wait = ms => new Promise(r => setTimeout(r, ms));

function dt(v) {
  if (v == null || v === '') return 'NULL';
  const d = new Date(String(v).trim());
  if (isNaN(d.getTime())) return 'NULL';
  const y = d.getFullYear();
  if (y < 1753 || y > 9999) return 'NULL';
  const pad = n => String(n).padStart(2, '0');
  return `N'${y}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}'`;
}

async function main() {
  loadEnv();
  const skipRows = parseInt((process.argv.find(a => a.startsWith('--skip=')) || '').split('=')[1] || '0', 10);

  // ═══════════════════════════════════════════════════════════════════════════
  // PHASE 1: Stream XML → in-memory array (deduped)
  // ═══════════════════════════════════════════════════════════════════════════
  console.log('Phase 1: Streaming XML from blob storage...');
  const t0 = Date.now();

  const account = 'pfpuredatalake';
  const key = process.env.SOZO_STORAGE_ACCOUNT_KEY;
  const cred = new StorageSharedKeyCredential(account, key);
  const svc = new BlobServiceClient(`https://${account}.blob.core.windows.net`, cred);
  const container = svc.getContainerClient('raw');
  const blobClient = container.getBlobClient('tmp/hb840 Tag Applications.xml');
  const downloadRes = await blobClient.download(0);

  // Accumulate unique rows in memory
  // Key = "tagId:contactId", Value = dateApplied string
  const seen = new Map();
  let totalParsed = 0;
  let dupes = 0;

  await new Promise((resolve, reject) => {
    const parser = sax.createStream(false, { lowercase: true, trim: true });
    let inRow = false, currentField = null, currentRow = {};

    parser.on('opentag', (node) => {
      if (node.name === 'row') { inRow = true; currentRow = {}; }
      else if (inRow && node.name === 'field') {
        currentField = (node.attributes.name || '').toLowerCase();
        currentRow[currentField] = '';
      }
    });

    parser.on('text', (text) => {
      if (inRow && currentField) currentRow[currentField] += text;
    });

    parser.on('cdata', (cdata) => {
      if (inRow && currentField) currentRow[currentField] += cdata;
    });

    parser.on('closetag', (name) => {
      if (name === 'field') { currentField = null; }
      else if (name === 'row') {
        inRow = false;
        totalParsed++;

        const tagId = parseInt(currentRow.tagid, 10);
        const contactId = parseInt(currentRow.contactid, 10);
        if (isNaN(tagId) || isNaN(contactId)) return;

        const k = `${tagId}:${contactId}`;
        if (seen.has(k)) { dupes++; return; }
        seen.set(k, currentRow.dateapplied || '');

        if (totalParsed % 50000 === 0) {
          const mb = (process.memoryUsage().heapUsed / 1048576).toFixed(0);
          process.stdout.write(`\r  ${totalParsed.toLocaleString()} rows parsed, ${seen.size.toLocaleString()} unique (${mb} MB heap)`);
        }
      }
    });

    parser.on('end', resolve);
    parser.on('error', reject);
    downloadRes.readableStreamBody.pipe(parser);
  });

  const parseTime = ((Date.now() - t0) / 1000).toFixed(1);
  console.log(`\n  Done parsing: ${totalParsed.toLocaleString()} rows, ${seen.size.toLocaleString()} unique, ${dupes.toLocaleString()} duplicates (${parseTime}s)`);

  // Convert Map to array for indexed access
  const rows = [];
  for (const [k, dateApplied] of seen) {
    const [tagId, contactId] = k.split(':');
    rows.push({ tagId, contactId, dateApplied });
  }
  seen.clear(); // free memory

  console.log(`  ${rows.length.toLocaleString()} rows ready for insert.`);

  // ═══════════════════════════════════════════════════════════════════════════
  // PHASE 2: Batch INSERT into SQL
  // ═══════════════════════════════════════════════════════════════════════════
  console.log('\nPhase 2: Inserting into silver.contact_tag...');
  const t1 = Date.now();

  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 120000,
    pool: { max: 3, min: 1 }
  });

  // Create table if not exists
  await pool.request().query(`
    IF OBJECT_ID('silver.contact_tag', 'U') IS NULL
    BEGIN
      CREATE TABLE silver.contact_tag (
        contact_tag_id    INT IDENTITY(1,1) PRIMARY KEY,
        tag_keap_id       INT NOT NULL,
        contact_keap_id   INT NOT NULL,
        contact_source_id VARCHAR(20),
        date_applied      DATETIME2
      );
      CREATE INDEX ix_ct_contact ON silver.contact_tag (contact_keap_id);
      CREATE INDEX ix_ct_tag ON silver.contact_tag (tag_keap_id);
      CREATE INDEX ix_ct_source ON silver.contact_tag (contact_source_id);
    END
  `);

  // Truncate for clean load (unless resuming)
  if (skipRows === 0) {
    console.log('  Truncating table...');
    try {
      await pool.request().query('TRUNCATE TABLE silver.contact_tag');
    } catch (e) {
      // Drop and recreate if truncate fails
      await pool.request().query('DROP TABLE silver.contact_tag');
      await pool.request().query(`
        CREATE TABLE silver.contact_tag (
          contact_tag_id    INT IDENTITY(1,1) PRIMARY KEY,
          tag_keap_id       INT NOT NULL,
          contact_keap_id   INT NOT NULL,
          contact_source_id VARCHAR(20),
          date_applied      DATETIME2
        )
      `);
    }
  } else {
    console.log(`  Resuming from row ${skipRows.toLocaleString()}...`);
  }

  // Drop unique constraint during load (add back after for speed)
  // No constraint to drop since we deduplicated in memory

  let inserted = 0;
  let errors = 0;
  const startIdx = skipRows;

  for (let i = startIdx; i < rows.length; i += BATCH) {
    const chunk = rows.slice(i, i + BATCH);
    const cols = 'tag_keap_id, contact_keap_id, contact_source_id, date_applied';
    const vals = chunk.map(r =>
      `(${r.tagId}, ${r.contactId}, '${r.contactId}', ${dt(r.dateApplied)})`
    ).join(',\n');

    try {
      await pool.request().query(`INSERT INTO silver.contact_tag (${cols}) VALUES ${vals}`);
      inserted += chunk.length;
    } catch (err) {
      // Row-by-row fallback
      for (const r of chunk) {
        try {
          await pool.request().query(
            `INSERT INTO silver.contact_tag (${cols}) VALUES (${r.tagId}, ${r.contactId}, '${r.contactId}', ${dt(r.dateApplied)})`
          );
          inserted++;
        } catch (e2) { errors++; }
      }
    }

    await wait(WAIT_MS);

    if ((i - startIdx) % 5000 < BATCH) {
      const elapsed = ((Date.now() - t1) / 1000).toFixed(0);
      const rate = (inserted / (elapsed || 1)).toFixed(0);
      const pct = ((i / rows.length) * 100).toFixed(1);
      process.stdout.write(`\r  ${inserted.toLocaleString()} / ${rows.length.toLocaleString()} (${pct}%, ${rate}/s, ${elapsed}s)  `);
    }
  }

  const insertTime = ((Date.now() - t1) / 1000).toFixed(1);
  console.log(`\n  Insert complete: ${inserted.toLocaleString()} rows in ${insertTime}s (${errors} errors)`);

  // Add unique constraint after load
  console.log('  Adding unique constraint...');
  try {
    await pool.request().query(
      'ALTER TABLE silver.contact_tag ADD CONSTRAINT uq_contact_tag UNIQUE (tag_keap_id, contact_keap_id)'
    );
    console.log('  OK');
  } catch (e) {
    console.log(`  Constraint skipped: ${e.message.substring(0, 80)}`);
  }

  // ═══════════════════════════════════════════════════════════════════════════
  // VERIFICATION
  // ═══════════════════════════════════════════════════════════════════════════
  console.log('\nVerification:');
  const cnt = await pool.request().query('SELECT COUNT(*) AS n FROM silver.contact_tag');
  console.log(`  Total rows: ${cnt.recordset[0].n.toLocaleString()}`);

  const topTags = await pool.request().query(`
    SELECT TOP 15 t.group_name, t.category_name, COUNT(*) AS n
    FROM silver.contact_tag ct
    JOIN silver.tag t ON t.keap_id = ct.tag_keap_id
    GROUP BY t.group_name, t.category_name
    ORDER BY n DESC
  `);
  console.log('\n  Top 15 tags by assignment count:');
  for (const r of topTags.recordset) {
    const cat = r.category_name || '(none)';
    console.log(`    ${String(r.n).padStart(7)} | ${cat} > ${r.group_name}`);
  }

  const coverage = await pool.request().query(`
    SELECT COUNT(DISTINCT ct.contact_keap_id) AS tagged,
           (SELECT COUNT(*) FROM silver.contact WHERE source_system = 'keap') AS keap_total
    FROM silver.contact_tag ct
  `);
  const c = coverage.recordset[0];
  console.log(`\n  Tagged contacts: ${c.tagged.toLocaleString()} / ${c.keap_total.toLocaleString()} Keap contacts (${((c.tagged/c.keap_total)*100).toFixed(1)}%)`);

  const tagDist = await pool.request().query(`
    SELECT
      CASE
        WHEN cnt = 1 THEN '1 tag'
        WHEN cnt BETWEEN 2 AND 5 THEN '2-5 tags'
        WHEN cnt BETWEEN 6 AND 10 THEN '6-10 tags'
        WHEN cnt BETWEEN 11 AND 20 THEN '11-20 tags'
        WHEN cnt > 20 THEN '20+ tags'
      END AS bucket,
      COUNT(*) AS contacts
    FROM (SELECT contact_keap_id, COUNT(*) AS cnt FROM silver.contact_tag GROUP BY contact_keap_id) x
    GROUP BY CASE
      WHEN cnt = 1 THEN '1 tag'
      WHEN cnt BETWEEN 2 AND 5 THEN '2-5 tags'
      WHEN cnt BETWEEN 6 AND 10 THEN '6-10 tags'
      WHEN cnt BETWEEN 11 AND 20 THEN '11-20 tags'
      WHEN cnt > 20 THEN '20+ tags'
    END
    ORDER BY MIN(cnt)
  `);
  console.log('\n  Tag distribution per contact:');
  for (const r of tagDist.recordset) {
    console.log(`    ${r.bucket.padEnd(10)} ${r.contacts.toLocaleString()} contacts`);
  }

  await pool.close();
  const totalTime = ((Date.now() - t0) / 1000 / 60).toFixed(1);
  console.log(`\nDone in ${totalTime} minutes.`);
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
