/**
 * Step 1.3c — Engagement Extraction (SAFE standalone with file lock)
 *
 * This script has a file-based lock to prevent concurrent runs.
 * It will NOT run if another instance is already executing.
 *
 * Sources: Keap Notes, DD Account Notes, DD Communications,
 *          Givebutter Communications, Kindful Activity,
 *          Keap Tags, DD Kindful Tags
 *
 * Run:  node scripts/ingest/03c_engagement_safe.js
 */

const fs = require('fs');
const path = require('path');
const sql = require('mssql');

const LOCK_FILE = path.join(__dirname, '.engagement_lock');
const LOG_FILE = '/tmp/sozo_engagement_safe.log';
const BATCH = 2000;
const CHUNK = 100;

// ── Logging to both console and file ──
function log(msg) {
  const line = `[${new Date().toISOString()}] ${msg}`;
  console.log(line);
  fs.appendFileSync(LOG_FILE, line + '\n');
}

// ── File-based process lock ──
function acquireLock() {
  if (fs.existsSync(LOCK_FILE)) {
    const content = fs.readFileSync(LOCK_FILE, 'utf8').trim();
    const pid = parseInt(content);
    // Check if the process is actually running
    try {
      process.kill(pid, 0); // signal 0 = just check existence
      log(`ABORT: Another instance running (PID ${pid}). Lock file: ${LOCK_FILE}`);
      process.exit(1);
    } catch (e) {
      // Process not running, stale lock
      log(`Removing stale lock (PID ${pid} not running)`);
    }
  }
  fs.writeFileSync(LOCK_FILE, String(process.pid));
  log(`Lock acquired (PID ${process.pid})`);
}

function releaseLock() {
  try { fs.unlinkSync(LOCK_FILE); } catch {}
}

// ── Env & DB ──
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(p)) { log('.env.local not found'); process.exit(1); }
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
    pool: { max: 5, min: 0, acquireTimeoutMillis: 300000 },
  };
}

const wait = ms => new Promise(r => setTimeout(r, ms));
const S = (v, len) => v == null ? null : String(v).substring(0, len);
const Lo = v => v == null ? null : String(v).toLowerCase().trim();
const dt = v => {
  if (!v) return null;
  const d = new Date(v);
  if (isNaN(d.getTime())) return null;
  const y = d.getUTCFullYear();
  if (y < 1753 || y > 9999) return null;
  const pad = (n, w = 2) => String(n).padStart(w, '0');
  return `${pad(y, 4)}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}T${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
};

function lit(v) {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') return String(v);
  return "N'" + String(v).replace(/'/g, "''") + "'";
}

async function retryQuery(pool, query, label) {
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      return await pool.request().batch(query);
    } catch (err) {
      if (attempt === 5) throw err;
      const backoff = Math.min(15000 * attempt, 60000);
      log(`  Retry ${attempt}/5 [${label}]: ${err.message.substring(0, 80)}`);
      await wait(backoff);
    }
  }
}

// Resume-aware batch insert
async function batchInsert(pool, label, lineageIds, tableName, colNames, mapFn, resumeFromRawId = 0) {
  if (!lineageIds.length) { log(`  - ${label}: skip (no files)`); return 0; }

  const lidList = lineageIds.map(l => `'${l}'`).join(',');
  const colList = colNames.join(', ');
  let lastId = resumeFromRawId, total = 0, batch = 0;

  if (resumeFromRawId > 0) {
    log(`  Resuming ${label} from raw.record.id > ${resumeFromRawId}`);
  }

  while (true) {
    const res = await retryQuery(pool,
      `SELECT TOP ${BATCH} id, source_id, data FROM raw.record ` +
      `WHERE lineage_id IN (${lidList}) AND id > ${lastId} ORDER BY id`,
      label);
    if (!res.recordset.length) break;
    lastId = res.recordset[res.recordset.length - 1].id;

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

    for (let i = 0; i < mapped.length; i += CHUNK) {
      const chunk = mapped.slice(i, i + CHUNK);
      const values = chunk.map(row =>
        '(' + colNames.map(c => lit(row[c])).join(',') + ')'
      ).join(',\n');

      if (values) {
        try {
          await pool.request().batch(`INSERT INTO ${tableName} (${colList}) VALUES ${values}`);
        } catch (err) {
          if (err.message.includes('Conversion failed') || err.message.includes('converting date')) {
            let saved = 0;
            for (const row of chunk) {
              const sv = '(' + colNames.map(c => lit(row[c])).join(',') + ')';
              try {
                await pool.request().batch(`INSERT INTO ${tableName} (${colList}) VALUES ${sv}`);
                saved++;
              } catch { /* skip bad row */ }
            }
            if (saved < chunk.length) {
              log(`    Batch fallback: ${saved}/${chunk.length} rows saved`);
            }
          } else {
            for (let attempt = 1; attempt <= 4; attempt++) {
              try {
                await wait(Math.min(15000 * attempt, 60000));
                await pool.request().batch(`INSERT INTO ${tableName} (${colList}) VALUES ${values}`);
                break;
              } catch (retryErr) {
                if (attempt === 4) throw retryErr;
                log(`    Insert retry ${attempt}/4 "${label}": ${retryErr.message.substring(0, 80)}`);
              }
            }
          }
        }
      }
    }

    total += mapped.length;
    batch++;
    // Log to file every batch, but only print to console every 10 batches
    if (batch % 10 === 0 || batch === 1) {
      log(`  ${label}: ${total.toLocaleString()} rows (batch ${batch})`);
    }

    if (res.recordset.length < BATCH) break;
    await wait(500);
  }

  log(`  DONE ${label}: ${total.toLocaleString()} rows (${batch} batches)`);
  return total;
}

// Column name arrays
const noteCN = ['person_id', 'note_text', 'author', 'created_at', 'source_id', 'source_ref'];
const commCN = ['person_id', 'channel', 'direction', 'subject', 'sent_at', 'source_id', 'source_ref'];
const actCN = ['person_id', 'activity_type', 'subject', 'body', 'occurred_at', 'source_id', 'source_ref'];
const tagCN = ['person_id', 'tag_value', 'source_id', 'source_ref'];

(async () => {
  // Clear log file
  fs.writeFileSync(LOG_FILE, '');

  acquireLock();
  loadEnv();
  const pool = await sql.connect(dbConfig());

  log('Step 1.3c — Safe Engagement Extraction');
  log('='.repeat(60));

  // Load file lineage
  const lr = await pool.request().query(
    `SELECT lineage_id, source_id, blob_path FROM meta.file_lineage WHERE status = 'loaded'`
  );
  const files = lr.recordset;
  log(`Files: ${files.length}`);

  // Source system grouping
  const srcSystems = {};
  const srcR = await pool.request().query(`SELECT source_id, name FROM meta.source_system`);
  for (const s of srcR.recordset) srcSystems[s.name] = s.source_id;

  const src = {
    keap: files.filter(f => f.source_id === srcSystems.keap),
    donor_direct: files.filter(f => f.source_id === srcSystems.donor_direct),
    givebutter: files.filter(f => f.source_id === srcSystems.givebutter),
  };

  const find = (arr, pred) => (arr || []).filter(f => pred(f.blob_path));
  const lids = arr => arr.map(f => f.lineage_id);

  // Check existing counts to determine what to skip/resume
  const counts = {};
  for (const [key, q] of [
    ['keap_notes', `SELECT COUNT(*) AS c FROM engagement.note WHERE source_ref LIKE 'keap:note:%'`],
    ['dd_notes', `SELECT COUNT(*) AS c FROM engagement.note WHERE source_ref LIKE 'dd:note:%'`],
    ['dd_comms', `SELECT COUNT(*) AS c FROM engagement.communication WHERE source_ref LIKE 'dd:comm:%'`],
    ['gb_comms', `SELECT COUNT(*) AS c FROM engagement.communication WHERE source_ref LIKE 'gb:comm:%'`],
    ['kindful_act', `SELECT COUNT(*) AS c FROM engagement.activity WHERE source_ref LIKE 'kindful:%'`],
    ['keap_tags', `SELECT COUNT(*) AS c FROM engagement.tag WHERE source_ref LIKE 'keap:tag:%'`],
    ['dd_tags', `SELECT COUNT(*) AS c FROM engagement.tag WHERE source_ref LIKE 'dd:kindful:%'`],
  ]) {
    const r = await pool.request().query(q);
    counts[key] = r.recordset[0].c;
  }
  log('Existing counts: ' + JSON.stringify(counts));

  // ── KEAP NOTES (resume-aware) ──
  let keapNotesResumeId = 0;
  if (counts.keap_notes > 0) {
    const r = await pool.request().query(
      `SELECT TOP 1 source_ref FROM engagement.note
       WHERE source_ref LIKE 'keap:note:%'
       ORDER BY id DESC`
    );
    if (r.recordset.length) {
      const parts = r.recordset[0].source_ref.split(':');
      keapNotesResumeId = parseInt(parts[2]) || 0;
    }
    log(`Keap Notes: ${counts.keap_notes} exist, resume from raw_id > ${keapNotesResumeId}`);
  }

  const keapNotes = find(src.keap, p => p.includes('Notes'));
  await batchInsert(pool, 'Keap Notes', lids(keapNotes),
    'engagement.note', noteCN, (d, raw) => {
      const text = [d.ActionDescription, d.CreationNotes].filter(Boolean).join('\n');
      if (!text) return null;
      return {
        person_id: null, note_text: S(text, 4000),
        author: S(((d['First Name'] || '') + ' ' + (d['Last Name'] || '')).trim(), 128),
        created_at: dt(d.ActionDate) || dt(d.CreationDate) || '1900-01-01T00:00:00',
        source_id: raw.source_id,
        source_ref: S('keap:note:' + raw.id + ':contact:' + (d.ContactId || ''), 256),
      };
    }, keapNotesResumeId);

  // ── DD ACCOUNT NOTES ──
  if (counts.dd_notes === 0) {
    const ddNotes = find(src.donor_direct, p => p.includes('AccountNotes'));
    await batchInsert(pool, 'DD Account Notes', lids(ddNotes),
      'engagement.note', noteCN, (d, raw) => {
        const text = [d.ShortComment, d.LongComment, d.Description].filter(Boolean).join('\n');
        return {
          person_id: null, note_text: S(text, 4000), author: null,
          created_at: dt(d.Date) || dt(d.CreatedDate) || '1900-01-01T00:00:00',
          source_id: raw.source_id,
          source_ref: S('dd:note:' + raw.id + ':acct:' + (d.AccountNumber || ''), 256),
        };
      });
  } else { log(`  Skipping DD Account Notes (${counts.dd_notes} exist)`); }

  // ── DD COMMUNICATIONS ──
  if (counts.dd_comms === 0) {
    const ddComms = find(src.donor_direct, p => p.includes('AccountCommunications') || p.includes('Communications'));
    await batchInsert(pool, 'DD Communications', lids(ddComms),
      'engagement.communication', commCN, (d, raw) => ({
        person_id: null, channel: S(d.CommunicationType, 32),
        direction: S(d.InboundOrOutbound, 16), subject: S(d.ShortComment, 512),
        sent_at: dt(d.Date), source_id: raw.source_id,
        source_ref: S('dd:comm:' + raw.id + ':acct:' + (d.AccountNumber || ''), 256),
      }));
  } else { log(`  Skipping DD Communications (${counts.dd_comms} exist)`); }

  // ── GIVEBUTTER COMMUNICATIONS ──
  if (counts.gb_comms === 0) {
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
  } else { log(`  Skipping Givebutter Communications (${counts.gb_comms} exist)`); }

  // ── KINDFUL ACTIVITY ──
  if (counts.kindful_act === 0) {
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
  } else { log(`  Skipping Kindful Activity (${counts.kindful_act} exist)`); }

  // ── KEAP TAGS ──
  if (counts.keap_tags === 0) {
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
  } else { log(`  Skipping Keap Tags (${counts.keap_tags} exist)`); }

  // ── DD KINDFUL TAGS ──
  if (counts.dd_tags === 0) {
    const ddKindful = find(src.donor_direct, p => p.includes('Kindful Donors'));
    await batchInsert(pool, 'DD Kindful Tags', lids(ddKindful),
      'engagement.tag', tagCN, (d, raw) => {
        const tags = d.Tags;
        if (!tags) return null;
        const items = String(tags).split(',').map(t => t.trim()).filter(Boolean);
        if (!items.length) return null;
        return items.map(t => ({
          person_id: null, tag_value: S(t, 512),
          source_id: raw.source_id,
          source_ref: S('dd:kindful:' + (d.Id || raw.id), 256),
        }));
      });
  } else { log(`  Skipping DD Kindful Tags (${counts.dd_tags} exist)`); }

  // Final counts
  log('');
  log('='.repeat(60));
  log('Engagement extraction results:');
  for (const [tbl, label] of [
    ['engagement.note', 'Notes'],
    ['engagement.communication', 'Communications'],
    ['engagement.activity', 'Activities'],
    ['engagement.tag', 'Tags'],
  ]) {
    const r = await pool.request().query(`SELECT COUNT(*) AS c FROM ${tbl}`);
    log(`  ${label}: ${r.recordset[0].c.toLocaleString()}`);
  }

  await pool.close();
  releaseLock();
  log('Done.');
})().catch(async err => {
  log('FATAL: ' + err.message);
  log(err.stack);
  releaseLock();
  process.exit(1);
});

// Clean up lock on unexpected termination
process.on('SIGTERM', () => { releaseLock(); process.exit(0); });
process.on('SIGINT', () => { releaseLock(); process.exit(0); });
process.on('uncaughtException', (err) => {
  log('UNCAUGHT: ' + err.message);
  releaseLock();
  process.exit(1);
});
