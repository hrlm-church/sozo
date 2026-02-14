/**
 * Step 1.4 — Batch Identity Resolution + Household Assignment
 *
 * Strategy: Read ALL staging.person_extract into Node.js memory, do grouping
 * and dedup in JS (zero DTU), then batch INSERT results via INSERT...VALUES.
 *
 * Three-pass identity merge:
 *   Pass 1: Email match (confidence 0.99) — groups by email
 *   Pass 2: Phone match (confidence 0.95) — for unmatched records with phone
 *   Pass 3: Remaining  (confidence 0.80) — one person per unmatched record
 *
 * Then assigns households by address + last name overlap, and links all
 * transactions/engagements to resolved person_ids via SQL crosswalk UPDATEs.
 *
 * Run: node scripts/ingest/04_resolve_identities.js
 */

const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const sql = require('mssql');

// ── env & db ────────────────────────────────────────────────────────────────
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

// ── helpers ──────────────────────────────────────────────────────────────────
const CHUNK = 100;
const wait = ms => new Promise(r => setTimeout(r, ms));

function lit(val) {
  if (val === null || val === undefined) return 'NULL';
  if (typeof val === 'number') return String(val);
  if (val instanceof Date) return `'${val.toISOString()}'`;
  return `N'${String(val).replace(/'/g, "''")}'`;
}

function normalizeEmail(e) {
  if (!e || !String(e).trim()) return null;
  const v = String(e).toLowerCase().trim();
  // Reject literal "null", "undefined", or clearly invalid values
  if (v === 'null' || v === 'undefined' || v === 'none' || v === 'n/a') return null;
  if (!v.includes('@')) return null;
  return v;
}

function normalizePhone(p) {
  if (!p || !String(p).trim()) return null;
  const raw = String(p).trim();
  // Reject garbage: URLs, addresses, multi-field data leaking into phone columns
  if (raw.includes('http') || raw.includes('www.')) return null;
  if (raw.includes(',') || raw.includes('|')) return null;     // CSV leakage
  if (/[a-zA-Z]{3,}/.test(raw)) return null;                   // contains 3+ letters = not a phone
  const digits = raw.replace(/\D/g, '');
  if (digits.length === 11 && digits[0] === '1') return digits.slice(1);
  if (digits.length === 10) return digits;
  if (digits.length >= 7 && digits.length <= 11) return digits; // cap at 11 digits
  return null;
}

// Max records per identity group — prevents transitive chain mega-groups
const MAX_GROUP_SIZE = 20;

function formatPhone(digits) {
  if (!digits) return null;
  if (digits.length === 10) {
    return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  }
  return digits;
}

function pickBestRecord(records) {
  let best = records[0];
  let bestScore = 0;
  for (const r of records) {
    let score = 0;
    if (r.first_name) score += 3;
    if (r.last_name) score += 3;
    if (r.email) score += 2;
    if (r.phone) score += 2;
    if (r.address_line1) score += 2;
    if (r.city) score += 1;
    if (r.state) score += 1;
    if (r.zip) score += 1;
    if (r.display_name) score += 1;
    if (score > bestScore) { best = r; bestScore = score; }
  }
  return best;
}

async function batchInsertValues(pool, tableName, colNames, rows) {
  if (!rows.length) return 0;
  const colList = colNames.join(',');
  let inserted = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const values = chunk.map(row =>
      '(' + colNames.map(c => lit(row[c])).join(',') + ')'
    ).join(',\n');
    try {
      await pool.request().batch(`INSERT INTO ${tableName} (${colList}) VALUES ${values}`);
      inserted += chunk.length;
    } catch (e) {
      // On batch failure (likely unique constraint), fall back to row-by-row
      for (const row of chunk) {
        const vals = '(' + colNames.map(c => lit(row[c])).join(',') + ')';
        try {
          await pool.request().batch(`INSERT INTO ${tableName} (${colList}) VALUES ${vals}`);
          inserted++;
        } catch (e2) { /* skip duplicates */ }
      }
    }
    if (i > 0 && i % 5000 === 0) await wait(200);
  }
  return inserted;
}

// ── main ────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();
  const fromStep = parseInt(process.argv.find(a => a.startsWith('--from='))?.split('=')[1] || '0', 10);
  console.log('Step 1.4 — Identity Resolution + Household Assignment');
  if (fromStep > 0) console.log(`  (resuming from step ${fromStep})`);
  console.log('='.repeat(60));

  const pool = await sql.connect(getDbConfig());

  try {
    if (fromStep < 7) {
    // ═══════════════════════════════════════════════════════════════════════
    // [1] CLEAR + LOAD
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[1] Clearing previous resolution data...');
    await pool.request().batch(`TRUNCATE TABLE household.member`);
    await pool.request().batch(`DELETE FROM household.unit`);
    await pool.request().batch(`DELETE FROM person.source_link`);
    await pool.request().batch(`DELETE FROM person.address`);
    await pool.request().batch(`DELETE FROM person.phone`);
    await pool.request().batch(`DELETE FROM person.email`);
    await pool.request().batch(`DELETE FROM person.profile`);

    console.log('  Loading all staging records into memory...');
    const allRecords = [];
    let lastId = 0;
    while (true) {
      const res = await pool.request().query(`
        SELECT TOP 5000 id, source_id, source_ref, first_name, last_name, display_name,
               email, email2, email3, phone, phone2, phone3,
               address_line1, address_line2, city, state, zip, country, company
        FROM staging.person_extract
        WHERE id > ${lastId}
        ORDER BY id
      `);
      if (!res.recordset.length) break;
      for (const r of res.recordset) allRecords.push(r);
      lastId = res.recordset[res.recordset.length - 1].id;
      if (res.recordset.length < 5000) break;
    }
    console.log(`  Loaded ${allRecords.length.toLocaleString()} staging records`);

    // ═══════════════════════════════════════════════════════════════════════
    // [2] PASS 1: Email dedup (confidence 0.99)
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[2] Pass 1 — Email dedup (confidence 0.99)...');

    // Build email → staging record index
    const emailIndex = {};  // email → [record indices]
    for (let i = 0; i < allRecords.length; i++) {
      const r = allRecords[i];
      for (const e of [r.email, r.email2, r.email3]) {
        const ne = normalizeEmail(e);
        if (ne) {
          if (!emailIndex[ne]) emailIndex[ne] = [];
          emailIndex[ne].push(i);
        }
      }
    }
    console.log(`  Unique emails: ${Object.keys(emailIndex).length.toLocaleString()}`);

    // Group records by connected emails (union-find)
    const resolved = new Set();  // indices of resolved records
    const personGroups = [];     // [{personId, records: [idx...], emails: Set, phones: Set}]
    const idxToGroup = new Map(); // fast lookup: record index → person group

    // Process email groups: merge records that share any email
    let cappedSkips = 0;
    for (const [email, indices] of Object.entries(emailIndex)) {
      // Find all unresolved records in this email group
      const unresolved = indices.filter(i => !resolved.has(i));
      if (unresolved.length === 0) continue;

      // Check if any of these records are already in a person group
      let existingGroup = null;
      for (const idx of unresolved) {
        if (idxToGroup.has(idx)) {
          existingGroup = idxToGroup.get(idx);
          break;
        }
      }

      if (existingGroup) {
        // Skip if group already at max size — prevents transitive mega-chains
        if (existingGroup.recordIndices.size >= MAX_GROUP_SIZE) {
          cappedSkips += unresolved.filter(i => !existingGroup.recordIndices.has(i)).length;
          continue;
        }
        // Add new records to existing group (up to cap)
        for (const idx of unresolved) {
          if (existingGroup.recordIndices.size >= MAX_GROUP_SIZE) { cappedSkips++; continue; }
          if (!existingGroup.recordIndices.has(idx)) {
            existingGroup.recordIndices.add(idx);
            idxToGroup.set(idx, existingGroup);
            resolved.add(idx);
            // Add this record's emails/phones to the group
            const r = allRecords[idx];
            for (const e of [r.email, r.email2, r.email3]) {
              const ne = normalizeEmail(e);
              if (ne) existingGroup.emails.add(ne);
            }
            for (const p of [r.phone, r.phone2, r.phone3]) {
              const np = normalizePhone(p);
              if (np) existingGroup.phones.add(np);
            }
          }
        }
      } else {
        // Create new person group
        const group = {
          personId: crypto.randomUUID(),
          recordIndices: new Set(),
          emails: new Set(),
          phones: new Set(),
          confidence: 0.99,
          matchMethod: 'email'
        };
        for (const idx of unresolved) {
          if (group.recordIndices.size >= MAX_GROUP_SIZE) { cappedSkips++; continue; }
          group.recordIndices.add(idx);
          idxToGroup.set(idx, group);
          resolved.add(idx);
          const r = allRecords[idx];
          for (const e of [r.email, r.email2, r.email3]) {
            const ne = normalizeEmail(e);
            if (ne) group.emails.add(ne);
          }
          for (const p of [r.phone, r.phone2, r.phone3]) {
            const np = normalizePhone(p);
            if (np) group.phones.add(np);
          }
        }
        personGroups.push(group);
      }
    }
    console.log(`  Pass 1: ${personGroups.length.toLocaleString()} persons from email match (${resolved.size.toLocaleString()} records resolved, ${cappedSkips.toLocaleString()} capped)`);

    // ═══════════════════════════════════════════════════════════════════════
    // [3] PASS 2: Phone dedup (confidence 0.95)
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[3] Pass 2 — Phone dedup (confidence 0.95)...');

    // Build phone → person group index (from pass 1)
    const phoneToGroup = {};
    for (const pg of personGroups) {
      for (const ph of pg.phones) {
        phoneToGroup[ph] = pg;
      }
    }

    // Build phone → unresolved record index
    const phoneIndex = {};
    for (let i = 0; i < allRecords.length; i++) {
      if (resolved.has(i)) continue;
      const r = allRecords[i];
      for (const p of [r.phone, r.phone2, r.phone3]) {
        const np = normalizePhone(p);
        if (np) {
          if (!phoneIndex[np]) phoneIndex[np] = [];
          phoneIndex[np].push(i);
        }
      }
    }

    let phoneMerged = 0;
    let phoneNew = 0;
    let phoneCapped = 0;

    for (const [phone, indices] of Object.entries(phoneIndex)) {
      const unresolved = indices.filter(i => !resolved.has(i));
      if (unresolved.length === 0) continue;

      // Check if this phone already belongs to a pass-1 person
      if (phoneToGroup[phone]) {
        const group = phoneToGroup[phone];
        if (group.recordIndices.size >= MAX_GROUP_SIZE) { phoneCapped += unresolved.length; continue; }
        for (const idx of unresolved) {
          if (group.recordIndices.size >= MAX_GROUP_SIZE) { phoneCapped++; continue; }
          group.recordIndices.add(idx);
          idxToGroup.set(idx, group);
          resolved.add(idx);
          const r = allRecords[idx];
          for (const e of [r.email, r.email2, r.email3]) {
            const ne = normalizeEmail(e);
            if (ne) group.emails.add(ne);
          }
        }
        phoneMerged += unresolved.length;
      } else {
        // Check if any of these records already in a phone-created group (fast lookup)
        let existingGroup = null;
        for (const idx of unresolved) {
          if (idxToGroup.has(idx)) {
            existingGroup = idxToGroup.get(idx);
            break;
          }
        }

        if (existingGroup) {
          if (existingGroup.recordIndices.size >= MAX_GROUP_SIZE) { phoneCapped += unresolved.length; continue; }
          for (const idx of unresolved) {
            if (existingGroup.recordIndices.size >= MAX_GROUP_SIZE) { phoneCapped++; continue; }
            if (!existingGroup.recordIndices.has(idx)) {
              existingGroup.recordIndices.add(idx);
              idxToGroup.set(idx, existingGroup);
              resolved.add(idx);
            }
          }
          existingGroup.phones.add(phone);
          phoneMerged += unresolved.length;
        } else {
          // Create new person
          const group = {
            personId: crypto.randomUUID(),
            recordIndices: new Set(),
            emails: new Set(),
            phones: new Set([phone]),
            confidence: 0.95,
            matchMethod: 'phone'
          };
          for (const idx of unresolved) {
            if (group.recordIndices.size >= MAX_GROUP_SIZE) { phoneCapped++; continue; }
            group.recordIndices.add(idx);
            idxToGroup.set(idx, group);
            resolved.add(idx);
            const r = allRecords[idx];
            for (const e of [r.email, r.email2, r.email3]) {
              const ne = normalizeEmail(e);
              if (ne) group.emails.add(ne);
            }
            for (const p of [r.phone, r.phone2, r.phone3]) {
              const np = normalizePhone(p);
              if (np) group.phones.add(np);
            }
          }
          personGroups.push(group);
          phoneNew++;
          phoneToGroup[phone] = group;
        }
      }
    }
    console.log(`  Pass 2: ${phoneNew.toLocaleString()} new persons, ${phoneMerged.toLocaleString()} merged, ${phoneCapped.toLocaleString()} capped`);

    // ═══════════════════════════════════════════════════════════════════════
    // [4] PASS 3: Remaining unmatched (confidence 0.80)
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[4] Pass 3 — Remaining unmatched (confidence 0.80)...');

    let pass3Count = 0;
    for (let i = 0; i < allRecords.length; i++) {
      if (resolved.has(i)) continue;
      const r = allRecords[i];
      const group = {
        personId: crypto.randomUUID(),
        recordIndices: new Set([i]),
        emails: new Set(),
        phones: new Set(),
        confidence: 0.80,
        matchMethod: 'unmatched'
      };
      for (const e of [r.email, r.email2, r.email3]) {
        const ne = normalizeEmail(e);
        if (ne) group.emails.add(ne);
      }
      for (const p of [r.phone, r.phone2, r.phone3]) {
        const np = normalizePhone(p);
        if (np) group.phones.add(np);
      }
      personGroups.push(group);
      resolved.add(i);
      pass3Count++;
    }
    console.log(`  Pass 3: ${pass3Count.toLocaleString()} singleton persons`);
    console.log(`  Total person groups: ${personGroups.length.toLocaleString()}`);

    // ═══════════════════════════════════════════════════════════════════════
    // [5] WRITE TO DATABASE — Batch INSERT all person data
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[5] Writing persons to database...');

    // 5a. person.profile
    const profileRows = personGroups.map(pg => {
      const records = [...pg.recordIndices].map(i => allRecords[i]);
      const best = pickBestRecord(records);
      return {
        id: pg.personId,
        display_name: best.display_name || [best.first_name, best.last_name].filter(Boolean).join(' ') || null,
        first_name: best.first_name || null,
        last_name: best.last_name || null,
        confidence: pg.confidence
      };
    });

    console.log(`  Inserting ${profileRows.length.toLocaleString()} person profiles...`);
    let profileInserted = 0;
    for (let i = 0; i < profileRows.length; i += CHUNK) {
      const chunk = profileRows.slice(i, i + CHUNK);
      const values = chunk.map(r =>
        `(${lit(r.id)}, ${lit(r.display_name)}, ${lit(r.first_name)}, ${lit(r.last_name)}, ${r.confidence})`
      ).join(',\n');
      await pool.request().batch(
        `INSERT INTO person.profile (id, display_name, first_name, last_name, confidence) VALUES ${values}`
      );
      profileInserted += chunk.length;
      if (profileInserted % 10000 === 0) {
        process.stdout.write(`\r  Profiles: ${profileInserted.toLocaleString()} / ${profileRows.length.toLocaleString()}`);
        await wait(200);
      }
    }
    console.log(`\r  ✓ Profiles: ${profileInserted.toLocaleString()}                    `);

    // 5b. person.email (skip duplicates)
    const emailRows = [];
    const seenEmails = new Set();
    for (const pg of personGroups) {
      let isPrimary = true;
      const records = [...pg.recordIndices].map(i => allRecords[i]);
      const best = pickBestRecord(records);
      for (const em of pg.emails) {
        if (seenEmails.has(em)) continue;
        seenEmails.add(em);
        emailRows.push({
          person_id: pg.personId,
          email: em,
          is_primary: isPrimary ? 1 : 0,
          source_id: best.source_id
        });
        isPrimary = false;
      }
    }
    console.log(`  Inserting ${emailRows.length.toLocaleString()} emails...`);
    const emailInserted = await batchInsertValues(pool, 'person.email',
      ['person_id', 'email', 'is_primary', 'source_id'], emailRows);
    console.log(`  ✓ Emails: ${emailInserted.toLocaleString()}`);

    // 5c. person.phone (skip duplicates)
    const phoneRows = [];
    const seenPhones = new Set();
    for (const pg of personGroups) {
      let isPrimary = true;
      const records = [...pg.recordIndices].map(i => allRecords[i]);
      const best = pickBestRecord(records);
      for (const ph of pg.phones) {
        if (seenPhones.has(ph)) continue;
        seenPhones.add(ph);
        phoneRows.push({
          person_id: pg.personId,
          phone_normalized: ph,
          phone_display: formatPhone(ph),
          is_primary: isPrimary ? 1 : 0,
          source_id: best.source_id
        });
        isPrimary = false;
      }
    }
    console.log(`  Inserting ${phoneRows.length.toLocaleString()} phones...`);
    const phoneInserted = await batchInsertValues(pool, 'person.phone',
      ['person_id', 'phone_normalized', 'phone_display', 'is_primary', 'source_id'], phoneRows);
    console.log(`  ✓ Phones: ${phoneInserted.toLocaleString()}`);

    // 5d. person.address (one per person — from best record)
    const addrRows = [];
    for (const pg of personGroups) {
      const records = [...pg.recordIndices].map(i => allRecords[i]);
      const best = pickBestRecord(records);
      if (best.address_line1 && String(best.address_line1).trim()) {
        addrRows.push({
          person_id: pg.personId,
          line1: best.address_line1,
          line2: best.address_line2 || null,
          city: best.city || null,
          state: best.state || null,
          zip: best.zip || null,
          country: best.country || 'US',
          is_primary: 1,
          source_id: best.source_id
        });
      }
    }
    console.log(`  Inserting ${addrRows.length.toLocaleString()} addresses...`);
    const addrInserted = await batchInsertValues(pool, 'person.address',
      ['person_id', 'line1', 'line2', 'city', 'state', 'zip', 'country', 'is_primary', 'source_id'], addrRows);
    console.log(`  ✓ Addresses: ${addrInserted.toLocaleString()}`);

    // 5e. person.source_link
    const linkRows = [];
    const seenLinks = new Set();
    for (const pg of personGroups) {
      for (const idx of pg.recordIndices) {
        const r = allRecords[idx];
        const key = `${r.source_id}:${r.source_ref}`;
        if (seenLinks.has(key)) continue;
        seenLinks.add(key);
        linkRows.push({
          person_id: pg.personId,
          source_id: r.source_id,
          source_record_id: r.source_ref,
          match_method: pg.matchMethod,
          confidence: pg.confidence
        });
      }
    }
    console.log(`  Inserting ${linkRows.length.toLocaleString()} source links...`);
    const linkInserted = await batchInsertValues(pool, 'person.source_link',
      ['person_id', 'source_id', 'source_record_id', 'match_method', 'confidence'], linkRows);
    console.log(`  ✓ Source links: ${linkInserted.toLocaleString()}`);

    // 5f. Update staging.person_extract with resolved_person_id (batch UPDATE)
    console.log('  Updating staging resolved_person_id...');
    let updatedStaging = 0;
    for (const pg of personGroups) {
      const ids = [...pg.recordIndices].map(i => allRecords[i].id);
      // Batch update in chunks
      for (let i = 0; i < ids.length; i += 500) {
        const chunk = ids.slice(i, i + 500);
        const idList = chunk.join(',');
        await pool.request().batch(
          `UPDATE staging.person_extract SET resolved_person_id = '${pg.personId}' WHERE id IN (${idList})`
        );
        updatedStaging += chunk.length;
      }
      if (updatedStaging % 20000 === 0 && updatedStaging > 0) {
        process.stdout.write(`\r  Staging updated: ${updatedStaging.toLocaleString()}`);
        await wait(100);
      }
    }
    console.log(`\r  ✓ Staging updated: ${updatedStaging.toLocaleString()}                    `);

    // ═══════════════════════════════════════════════════════════════════════
    // [6] HOUSEHOLD ASSIGNMENT
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[6] Household assignment...');

    // Build household groups in JS from address + last name
    const hhGroupMap = {};  // "addr_key::ln_prefix" → [{personId, lastName, displayName}]
    for (const pg of personGroups) {
      const records = [...pg.recordIndices].map(i => allRecords[i]);
      const best = pickBestRecord(records);
      if (!best.address_line1 || !String(best.address_line1).trim()) continue;
      if (!best.city || !String(best.city).trim()) continue;
      if (!best.last_name || !String(best.last_name).trim()) continue;

      const addrKey = [
        String(best.address_line1).toLowerCase().trim(),
        String(best.city).toLowerCase().trim(),
        (best.state || '').toLowerCase().trim()
      ].join('|');
      const lnPrefix = String(best.last_name).toUpperCase().trim().substring(0, 3);
      const key = `${addrKey}::${lnPrefix}`;

      if (!hhGroupMap[key]) hhGroupMap[key] = [];
      hhGroupMap[key].push({
        personId: pg.personId,
        lastName: best.last_name,
        displayName: best.display_name || [best.first_name, best.last_name].filter(Boolean).join(' ')
      });
    }

    // Create multi-person households
    const hhUnitRows = [];
    const hhMemberRows = [];
    const personsInHousehold = new Set();

    for (const [key, members] of Object.entries(hhGroupMap)) {
      const hhId = crypto.randomUUID();
      const primaryLast = members[0].lastName;
      const hhName = `The ${primaryLast} Household`;

      hhUnitRows.push({ id: hhId, name: hhName });

      for (const m of members) {
        if (!personsInHousehold.has(m.personId)) {
          hhMemberRows.push({
            household_id: hhId,
            person_id: m.personId,
            role: members.length > 1 ? 'member' : 'primary'
          });
          personsInHousehold.add(m.personId);
        }
      }
    }

    // Create single-person households for everyone else
    for (const pg of personGroups) {
      if (personsInHousehold.has(pg.personId)) continue;
      const records = [...pg.recordIndices].map(i => allRecords[i]);
      const best = pickBestRecord(records);

      const hhId = crypto.randomUUID();
      const hhName = best.last_name
        ? `The ${best.last_name} Household`
        : best.display_name
          ? `${best.display_name} Household`
          : 'Unknown Household';

      hhUnitRows.push({ id: hhId, name: hhName });
      hhMemberRows.push({
        household_id: hhId,
        person_id: pg.personId,
        role: 'primary'
      });
    }

    console.log(`  Inserting ${hhUnitRows.length.toLocaleString()} households...`);
    const hhInserted = await batchInsertValues(pool, 'household.unit', ['id', 'name'], hhUnitRows);
    console.log(`  ✓ Households: ${hhInserted.toLocaleString()}`);

    console.log(`  Inserting ${hhMemberRows.length.toLocaleString()} household members...`);
    const memInserted = await batchInsertValues(pool, 'household.member',
      ['household_id', 'person_id', 'role'], hhMemberRows);
    console.log(`  ✓ Members: ${memInserted.toLocaleString()}`);

    } else { console.log('\n[1-6] Skipping (resume mode — persons/households already written)'); }

    // ═══════════════════════════════════════════════════════════════════════
    // [7] LINK TRANSACTIONS/ENGAGEMENTS TO PERSON_ID
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[7] Linking transactions/engagements to resolved persons...');

    // Create temp crosswalk table for efficient JOINs
    console.log('  Building crosswalk temp table...');
    await pool.request().batch(`
      IF OBJECT_ID('tempdb..#xwalk') IS NOT NULL DROP TABLE #xwalk;
      SELECT
        sl.person_id,
        sl.source_record_id AS source_ref
      INTO #xwalk
      FROM person.source_link sl;
      CREATE INDEX IX_xwalk_ref ON #xwalk(source_ref);
    `);

    // Also create a Keap contact crosswalk: extract contact_id from source_ref
    await pool.request().batch(`
      IF OBJECT_ID('tempdb..#keap_xwalk') IS NOT NULL DROP TABLE #keap_xwalk;
      SELECT
        person_id,
        SUBSTRING(source_ref, CHARINDEX(':contact:', source_ref) + 9, 50) AS contact_id
      INTO #keap_xwalk
      FROM #xwalk
      WHERE source_ref LIKE 'keap:contact:%';
      CREATE INDEX IX_keap_cid ON #keap_xwalk(contact_id);
    `);

    // DD account crosswalk
    await pool.request().batch(`
      IF OBJECT_ID('tempdb..#dd_xwalk') IS NOT NULL DROP TABLE #dd_xwalk;
      SELECT
        person_id,
        SUBSTRING(source_ref, CHARINDEX(':account:', source_ref) + 9, 50) AS acct_num
      INTO #dd_xwalk
      FROM #xwalk
      WHERE source_ref LIKE 'dd:account:%';
      CREATE INDEX IX_dd_acct ON #dd_xwalk(acct_num);
    `);

    // Helper: run linkage query with extended timeout (15 min)
    const linkQ = async (label, sql) => {
      const r = pool.request();
      r.timeout = 900000; // 15 min
      const result = await r.batch(sql);
      const affected = result.rowsAffected ? result.rowsAffected.reduce((a, b) => a + b, 0) : '?';
      console.log(`    ${label}: ${affected} rows linked`);
      await wait(500);
    };

    // Link donations
    console.log('  Linking donations...');
    // Keap donations: extract contact_id from source_ref using SUBSTRING + exact JOIN
    await linkQ('Keap donations', `
      UPDATE d
      SET d.person_id = kx.person_id
      FROM giving.donation d
      JOIN #keap_xwalk kx ON kx.contact_id =
        SUBSTRING(d.source_ref, CHARINDEX(':contact:', d.source_ref) + 9, 50)
      WHERE d.person_id IS NULL AND d.source_id = 4
        AND d.source_ref LIKE '%:contact:%'
    `);

    // DD donations: extract acct_num from source_ref using SUBSTRING + exact JOIN
    await linkQ('DD donations', `
      UPDATE d
      SET d.person_id = dx.person_id
      FROM giving.donation d
      JOIN #dd_xwalk dx ON dx.acct_num =
        SUBSTRING(d.source_ref, CHARINDEX(':acct:', d.source_ref) + 6, 50)
      WHERE d.person_id IS NULL AND d.source_id = 2
        AND d.source_ref LIKE '%:acct:%'
    `);

    // Other donations - try direct source_ref match
    await linkQ('Other donations', `
      UPDATE d
      SET d.person_id = xw.person_id
      FROM giving.donation d
      JOIN #xwalk xw ON d.source_ref = xw.source_ref
      WHERE d.person_id IS NULL
    `);

    // Link commerce tables
    console.log('  Linking commerce records...');

    for (const tbl of ['commerce.payment', 'commerce.invoice', 'commerce.[order]', 'commerce.subscription']) {
      const shortName = tbl.replace('commerce.', '').replace('[', '').replace(']', '');
      // Keap
      await linkQ(`Keap ${shortName}`, `
        UPDATE t
        SET t.person_id = kx.person_id
        FROM ${tbl} t
        JOIN #keap_xwalk kx ON kx.contact_id =
          SUBSTRING(t.source_ref, CHARINDEX(':contact:', t.source_ref) + 9, 50)
        WHERE t.person_id IS NULL AND t.source_id = 4
          AND t.source_ref LIKE '%:contact:%'
      `);

      // DD
      await linkQ(`DD ${shortName}`, `
        UPDATE t
        SET t.person_id = dx.person_id
        FROM ${tbl} t
        JOIN #dd_xwalk dx ON dx.acct_num =
          SUBSTRING(t.source_ref, CHARINDEX(':acct:', t.source_ref) + 6, 50)
        WHERE t.person_id IS NULL AND t.source_id = 2
          AND t.source_ref LIKE '%:acct:%'
      `);
    }

    // Link engagement tables
    console.log('  Linking engagement records...');

    // Notes - Keap (360K rows, do in batches of 50K)
    let noteLinked = 0;
    while (true) {
      const r = pool.request();
      r.timeout = 900000;
      const res = await r.batch(`
        UPDATE TOP (50000) n
        SET n.person_id = kx.person_id
        FROM engagement.note n
        JOIN #keap_xwalk kx ON kx.contact_id =
          SUBSTRING(n.source_ref, CHARINDEX(':contact:', n.source_ref) + 9, 50)
        WHERE n.person_id IS NULL AND n.source_ref LIKE 'keap:%:contact:%'
      `);
      const affected = res.rowsAffected ? res.rowsAffected.reduce((a, b) => a + b, 0) : 0;
      noteLinked += affected;
      if (affected === 0) break;
      console.log(`    Keap notes: ${noteLinked} linked so far...`);
      await wait(500);
    }
    console.log(`    Keap notes: ${noteLinked} total linked`);

    // Notes - DD
    await linkQ('DD notes', `
      UPDATE n
      SET n.person_id = dx.person_id
      FROM engagement.note n
      JOIN #dd_xwalk dx ON dx.acct_num =
        SUBSTRING(n.source_ref, CHARINDEX(':acct:', n.source_ref) + 6, 50)
      WHERE n.person_id IS NULL AND n.source_ref LIKE 'dd:%:acct:%'
    `);

    // Communications - DD
    await linkQ('DD communications', `
      UPDATE c
      SET c.person_id = dx.person_id
      FROM engagement.communication c
      JOIN #dd_xwalk dx ON dx.acct_num =
        SUBSTRING(c.source_ref, CHARINDEX(':acct:', c.source_ref) + 6, 50)
      WHERE c.person_id IS NULL AND c.source_ref LIKE 'dd:%:acct:%'
    `);

    // Activities - extract email from source_ref and match
    await linkQ('Kindful activities', `
      UPDATE a
      SET a.person_id = pe.person_id
      FROM engagement.activity a
      JOIN person.email pe ON pe.email =
        SUBSTRING(a.source_ref, CHARINDEX(':email:', a.source_ref) + 7, 256)
      WHERE a.person_id IS NULL AND a.source_ref LIKE 'kindful:%:email:%'
    `);

    // Tags - Keap contact match
    await linkQ('Keap tags', `
      UPDATE t
      SET t.person_id = kx.person_id
      FROM engagement.tag t
      JOIN #keap_xwalk kx ON kx.contact_id =
        SUBSTRING(t.source_ref, CHARINDEX(':contact:', t.source_ref) + 9, 50)
      WHERE t.person_id IS NULL AND t.source_ref LIKE 'keap:%:contact:%'
    `);

    // Tags - direct source_ref match to source_link
    await linkQ('Other tags', `
      UPDATE t
      SET t.person_id = xw.person_id
      FROM engagement.tag t
      JOIN #xwalk xw ON t.source_ref = xw.source_ref
      WHERE t.person_id IS NULL AND t.source_ref IS NOT NULL
    `);

    // Drop temp tables
    await pool.request().batch(`
      DROP TABLE IF EXISTS #xwalk;
      DROP TABLE IF EXISTS #keap_xwalk;
      DROP TABLE IF EXISTS #dd_xwalk;
    `);

    // ═══════════════════════════════════════════════════════════════════════
    // [8] LINKAGE STATS
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n[8] Linkage statistics:');

    const tables = [
      ['giving.donation', 'Donations'],
      ['commerce.invoice', 'Invoices'],
      ['commerce.payment', 'Payments'],
      ['commerce.[order]', 'Orders'],
      ['commerce.subscription', 'Subscriptions'],
      ['engagement.note', 'Notes'],
      ['engagement.communication', 'Communications'],
      ['engagement.activity', 'Activities'],
      ['engagement.tag', 'Tags']
    ];

    for (const [tbl, label] of tables) {
      const res = await pool.request().query(`
        SELECT COUNT(*) AS total,
          SUM(CASE WHEN person_id IS NOT NULL THEN 1 ELSE 0 END) AS linked
        FROM ${tbl}
      `);
      const r = res.recordset[0];
      if (r.total > 0) {
        const pct = ((r.linked / r.total) * 100).toFixed(1);
        console.log(`  ${label.padEnd(16)} ${r.linked.toLocaleString()} / ${r.total.toLocaleString()} (${pct}%)`);
      }
    }

    // ═══════════════════════════════════════════════════════════════════════
    // [9] SUMMARY
    // ═══════════════════════════════════════════════════════════════════════
    console.log('\n' + '='.repeat(60));
    console.log('Identity Resolution Summary:');

    const personCount = await pool.request().query('SELECT COUNT(*) AS cnt FROM person.profile');
    const emailCount = await pool.request().query('SELECT COUNT(*) AS cnt FROM person.email');
    const phoneCount = await pool.request().query('SELECT COUNT(*) AS cnt FROM person.phone');
    const addrCount = await pool.request().query('SELECT COUNT(*) AS cnt FROM person.address');
    const linkCount = await pool.request().query('SELECT COUNT(*) AS cnt FROM person.source_link');
    const hhCount = await pool.request().query('SELECT COUNT(*) AS cnt FROM household.unit');
    const hhMemCount = await pool.request().query('SELECT COUNT(*) AS cnt FROM household.member');

    console.log(`  Persons:      ${personCount.recordset[0].cnt.toLocaleString()}`);
    console.log(`  Emails:       ${emailCount.recordset[0].cnt.toLocaleString()}`);
    console.log(`  Phones:       ${phoneCount.recordset[0].cnt.toLocaleString()}`);
    console.log(`  Addresses:    ${addrCount.recordset[0].cnt.toLocaleString()}`);
    console.log(`  Source links: ${linkCount.recordset[0].cnt.toLocaleString()}`);
    console.log(`  Households:   ${hhCount.recordset[0].cnt.toLocaleString()}`);
    console.log(`  HH members:   ${hhMemCount.recordset[0].cnt.toLocaleString()}`);

    const confBreakdown = await pool.request().query(`
      SELECT confidence, COUNT(*) AS cnt FROM person.profile GROUP BY confidence ORDER BY confidence DESC
    `);
    console.log('\n  Confidence breakdown:');
    for (const r of confBreakdown.recordset) {
      console.log(`    ${r.confidence}: ${r.cnt.toLocaleString()} persons`);
    }

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
