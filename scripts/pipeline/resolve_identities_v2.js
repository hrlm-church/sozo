/**
 * Identity Resolution for sozov2 Silver Layer
 *
 * Matches contacts across source systems using:
 *   1. GB cross-references (dd_number, keap_number)
 *   2. Email matching
 *   3. Phone matching
 *   4. Name + postal code matching
 *
 * Creates silver.identity_map linking each contact to a master_id.
 *
 * Usage: node scripts/pipeline/resolve_identities_v2.js
 */
const fs = require('fs');
const path = require('path');
const sql = require('mssql');

function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

const wait = ms => new Promise(r => setTimeout(r, ms));

// ── Union-Find ─────────────────────────────────────────

class UnionFind {
  constructor() { this.parent = new Map(); this.rank = new Map(); }

  find(x) {
    if (!this.parent.has(x)) { this.parent.set(x, x); this.rank.set(x, 0); }
    if (this.parent.get(x) !== x) this.parent.set(x, this.find(this.parent.get(x)));
    return this.parent.get(x);
  }

  union(a, b) {
    const ra = this.find(a), rb = this.find(b);
    if (ra === rb) return;
    const rkA = this.rank.get(ra), rkB = this.rank.get(rb);
    if (rkA < rkB) this.parent.set(ra, rb);
    else if (rkA > rkB) this.parent.set(rb, ra);
    else { this.parent.set(rb, ra); this.rank.set(ra, rkA + 1); }
  }
}

// ── Normalization helpers ──────────────────────────────

function normEmail(e) {
  if (!e) return null;
  const s = e.trim().toLowerCase();
  if (!s || !s.includes('@') || s.length < 5) return null;
  return s;
}

function normPhone(p) {
  if (!p) return null;
  const digits = p.replace(/\D/g, '');
  if (digits.length < 7) return null;
  // Take last 10 digits (strip country code)
  return digits.length > 10 ? digits.slice(-10) : digits;
}

function normNameZip(last, zip) {
  if (!last || !zip) return null;
  const l = last.trim().toLowerCase().replace(/[^a-z]/g, '');
  const z = zip.trim().replace(/\D/g, '').substring(0, 5);
  if (l.length < 2 || z.length < 5) return null;
  return `${l}|${z}`;
}

async function main() {
  loadEnv();
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST, database: 'sozov2',
    user: process.env.SOZO_SQL_USER, password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 120000,
    pool: { max: 3, min: 1 }
  });

  console.log('Identity Resolution v2\n');

  // ── Step 1: Load all contacts ──────────────────────
  console.log('Step 1: Loading contacts...');
  const allContacts = [];
  let lastId = 0;
  while (true) {
    const res = await pool.request().query(`
      SELECT TOP 5000 contact_id, source_system, source_id,
             first_name, last_name, email_primary, phone_primary,
             postal_code, dd_number, keap_number, gb_external_id
      FROM silver.contact WHERE contact_id > ${lastId} ORDER BY contact_id
    `);
    if (res.recordset.length === 0) break;
    allContacts.push(...res.recordset);
    lastId = res.recordset[res.recordset.length - 1].contact_id;
  }
  console.log(`  Loaded ${allContacts.length.toLocaleString()} contacts`);

  // Build source indexes
  const keapById = new Map();  // source_id → contact_id (for keap)
  const ddById = new Map();    // source_id → contact_id (for dd)

  for (const c of allContacts) {
    if (c.source_system === 'keap') keapById.set(c.source_id, c.contact_id);
    if (c.source_system === 'donor_direct') ddById.set(c.source_id, c.contact_id);
  }

  // ── Step 2: Enrich DD contacts with emails/phones ──
  console.log('Step 2: Enriching DD contacts with emails/phones...');
  const ddEmails = new Map(); // contact_source_id → email
  const ddPhones = new Map();

  const emailRes = await pool.request().query(`
    SELECT contact_source_id, email_address
    FROM silver.contact_email
    WHERE source_system = 'donor_direct' AND is_primary = 1
  `);
  for (const r of emailRes.recordset) {
    ddEmails.set(r.contact_source_id, r.email_address);
  }

  const phoneRes = await pool.request().query(`
    SELECT contact_source_id, phone_number
    FROM silver.contact_phone
    WHERE source_system = 'donor_direct' AND is_primary = 1
  `);
  for (const r of phoneRes.recordset) {
    ddPhones.set(r.contact_source_id, r.phone_number);
  }
  console.log(`  DD enrichment: ${ddEmails.size} emails, ${ddPhones.size} phones`);

  // Inject DD emails/phones into contacts
  for (const c of allContacts) {
    if (c.source_system === 'donor_direct') {
      if (!c.email_primary) c.email_primary = ddEmails.get(c.source_id) || null;
      if (!c.phone_primary) c.phone_primary = ddPhones.get(c.source_id) || null;
    }
  }

  // Also load ALL DD emails (not just primary) for broader matching
  const ddAllEmails = new Map(); // contact_source_id → [emails]
  const allEmailRes = await pool.request().query(`
    SELECT contact_source_id, email_address
    FROM silver.contact_email WHERE source_system = 'donor_direct'
  `);
  for (const r of allEmailRes.recordset) {
    if (!ddAllEmails.has(r.contact_source_id)) ddAllEmails.set(r.contact_source_id, []);
    ddAllEmails.get(r.contact_source_id).push(r.email_address);
  }

  // ── Step 3: Union-Find clustering ──────────────────
  const uf = new UnionFind();
  let mergesByMethod = { crossref: 0, email: 0, phone: 0, namezip: 0 };

  // Phase A: GB cross-references
  console.log('Step 3a: GB cross-reference matching...');
  for (const c of allContacts) {
    if (c.source_system !== 'givebutter') continue;

    // GB → Keap match
    if (c.keap_number && keapById.has(c.keap_number)) {
      uf.union(c.contact_id, keapById.get(c.keap_number));
      mergesByMethod.crossref++;
    }
    // GB → DD match
    if (c.dd_number && ddById.has(c.dd_number)) {
      uf.union(c.contact_id, ddById.get(c.dd_number));
      mergesByMethod.crossref++;
    }
  }
  console.log(`  Cross-ref merges: ${mergesByMethod.crossref}`);

  // Phase B: Email matching
  console.log('Step 3b: Email matching...');
  const emailIndex = new Map(); // normalized email → [contact_ids]

  for (const c of allContacts) {
    const emails = [normEmail(c.email_primary)];

    // Add DD secondary emails
    if (c.source_system === 'donor_direct' && ddAllEmails.has(c.source_id)) {
      for (const e of ddAllEmails.get(c.source_id)) {
        emails.push(normEmail(e));
      }
    }

    for (const em of emails) {
      if (!em) continue;
      if (!emailIndex.has(em)) emailIndex.set(em, []);
      emailIndex.get(em).push(c.contact_id);
    }
  }

  for (const [email, ids] of emailIndex) {
    if (ids.length < 2) continue;
    for (let i = 1; i < ids.length; i++) {
      uf.union(ids[0], ids[i]);
      mergesByMethod.email++;
    }
  }
  console.log(`  Email merges: ${mergesByMethod.email}`);

  // Phase C: Phone matching
  console.log('Step 3c: Phone matching...');
  const phoneIndex = new Map();

  for (const c of allContacts) {
    const np = normPhone(c.phone_primary);
    if (!np) continue;
    if (!phoneIndex.has(np)) phoneIndex.set(np, []);
    phoneIndex.get(np).push(c.contact_id);
  }

  for (const [phone, ids] of phoneIndex) {
    if (ids.length < 2) continue;
    for (let i = 1; i < ids.length; i++) {
      uf.union(ids[0], ids[i]);
      mergesByMethod.phone++;
    }
  }
  console.log(`  Phone merges: ${mergesByMethod.phone}`);

  // Phase D: Name + Zip matching (conservative)
  console.log('Step 3d: Name + Zip matching...');
  const nameZipIndex = new Map();

  for (const c of allContacts) {
    const key = normNameZip(c.last_name, c.postal_code);
    if (!key) continue;
    if (!nameZipIndex.has(key)) nameZipIndex.set(key, []);
    nameZipIndex.get(key).push(c.contact_id);
  }

  for (const [key, ids] of nameZipIndex) {
    if (ids.length < 2 || ids.length > 5) continue; // skip if too many (common name+zip)
    // Only merge if first names also match (first 3 chars)
    const contacts = ids.map(id => allContacts.find(c => c.contact_id === id));
    for (let i = 1; i < contacts.length; i++) {
      const a = contacts[0], b = contacts[i];
      if (!a || !b || !a.first_name || !b.first_name) continue;
      const fn1 = a.first_name.trim().toLowerCase().substring(0, 3);
      const fn2 = b.first_name.trim().toLowerCase().substring(0, 3);
      if (fn1 === fn2) {
        uf.union(a.contact_id, b.contact_id);
        mergesByMethod.namezip++;
      }
    }
  }
  console.log(`  Name+Zip merges: ${mergesByMethod.namezip}`);

  // ── Step 4: Assign master IDs ─────────────────────
  console.log('\nStep 4: Assigning master IDs...');

  // Group contacts by cluster root
  const clusters = new Map(); // root → [contact_ids]
  for (const c of allContacts) {
    const root = uf.find(c.contact_id);
    if (!clusters.has(root)) clusters.set(root, []);
    clusters.get(root).push(c.contact_id);
  }

  const singletons = [...clusters.values()].filter(c => c.length === 1).length;
  const merged = [...clusters.values()].filter(c => c.length > 1);
  const mergedContacts = merged.reduce((sum, c) => sum + c.length, 0);

  console.log(`  Total clusters: ${clusters.size.toLocaleString()}`);
  console.log(`  Singletons (unique people): ${singletons.toLocaleString()}`);
  console.log(`  Merged clusters: ${merged.length.toLocaleString()} (covering ${mergedContacts.toLocaleString()} contacts)`);
  console.log(`  Largest cluster: ${Math.max(...[...clusters.values()].map(c => c.length))} contacts`);

  // Distribution
  const dist = {};
  for (const ids of clusters.values()) {
    const size = Math.min(ids.length, 10);
    dist[size] = (dist[size] || 0) + 1;
  }
  console.log('  Cluster size distribution:');
  for (const [size, count] of Object.entries(dist).sort((a, b) => a[0] - b[0])) {
    console.log(`    ${size === '10' ? '10+' : size} contacts: ${count.toLocaleString()} clusters`);
  }

  // ── Step 5: Create identity_map table ──────────────
  console.log('\nStep 5: Writing identity_map...');

  await pool.request().query(`
    IF OBJECT_ID('silver.identity_map', 'U') IS NOT NULL DROP TABLE silver.identity_map
  `);
  await pool.request().query(`
    CREATE TABLE silver.identity_map (
      contact_id  INT NOT NULL,
      master_id   INT NOT NULL,
      is_primary  BIT DEFAULT 0,
      source_system VARCHAR(20),
      source_id   VARCHAR(100),
      match_method VARCHAR(20)
    )
  `);
  await pool.request().query(`
    CREATE INDEX ix_idmap_contact ON silver.identity_map (contact_id)
  `);
  await pool.request().query(`
    CREATE INDEX ix_idmap_master ON silver.identity_map (master_id)
  `);

  // Assign sequential master_ids and determine primary record per cluster
  let masterSeq = 1;
  const BATCH = 100;
  let totalWritten = 0;
  let batch = [];

  for (const [root, ids] of clusters) {
    const masterId = masterSeq++;
    // Pick primary: prefer keap (most data), then dd, then gb
    const contactsInCluster = ids.map(id => allContacts.find(c => c.contact_id === id)).filter(Boolean);
    contactsInCluster.sort((a, b) => {
      const order = { keap: 0, donor_direct: 1, givebutter: 2, woocommerce: 3, shopify: 4, subbly: 5, tickera: 6, mailchimp: 7 };
      return (order[a.source_system] ?? 3) - (order[b.source_system] ?? 3);
    });

    for (let i = 0; i < contactsInCluster.length; i++) {
      const c = contactsInCluster[i];
      // Determine match method
      let method = 'none';
      if (ids.length === 1) method = 'singleton';
      else if (c.source_system === 'givebutter' && (c.keap_number || c.dd_number)) method = 'crossref';
      else if (normEmail(c.email_primary)) method = 'email';
      else if (normPhone(c.phone_primary)) method = 'phone';
      else method = 'namezip';

      batch.push(`(${c.contact_id}, ${masterId}, ${i === 0 ? 1 : 0}, '${c.source_system}', N'${(c.source_id || '').replace(/'/g, "''")}', '${method}')`);

      if (batch.length >= BATCH) {
        await pool.request().query(
          `INSERT INTO silver.identity_map (contact_id, master_id, is_primary, source_system, source_id, match_method) VALUES ${batch.join(',')}`
        );
        totalWritten += batch.length;
        batch = [];
        await wait(150);
        if (totalWritten % 10000 < BATCH) {
          process.stdout.write(`  ${totalWritten.toLocaleString()} rows...\r`);
        }
      }
    }
  }

  // Flush remaining
  if (batch.length > 0) {
    await pool.request().query(
      `INSERT INTO silver.identity_map (contact_id, master_id, is_primary, source_system, source_id, match_method) VALUES ${batch.join(',')}`
    );
    totalWritten += batch.length;
  }
  console.log(`  Wrote ${totalWritten.toLocaleString()} identity map rows`);

  // ── Step 6: Create gold.person — unified master view ──
  console.log('\nStep 6: Creating gold.person view...');

  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
      EXEC('CREATE SCHEMA gold')
  `);

  await pool.request().query(`
    IF OBJECT_ID('gold.person', 'V') IS NOT NULL DROP VIEW gold.person
  `);

  await pool.request().query(`
    CREATE VIEW gold.person AS
    SELECT
      im.master_id,
      c.first_name,
      c.last_name,
      COALESCE(c.first_name + ' ' + c.last_name, c.first_name, c.last_name, 'Unknown') AS display_name,
      c.email_primary,
      c.phone_primary,
      c.address_line1,
      c.city,
      c.state,
      c.postal_code,
      c.country,
      c.date_of_birth,
      c.gender,
      c.spouse_name,
      c.household_name,
      c.organization_name,
      c.source_system AS primary_source,
      c.created_at,
      (SELECT COUNT(DISTINCT im2.source_system) FROM silver.identity_map im2 WHERE im2.master_id = im.master_id) AS source_count,
      (SELECT STRING_AGG(ss, ', ') FROM (SELECT DISTINCT im2.source_system AS ss FROM silver.identity_map im2 WHERE im2.master_id = im.master_id) x) AS source_systems
    FROM silver.identity_map im
    JOIN silver.contact c ON c.contact_id = im.contact_id
    WHERE im.is_primary = 1
  `);

  // ── Step 7: Create gold.person_giving — donation summary ──
  console.log('Step 7: Creating gold.person_giving view...');

  await pool.request().query(`
    IF OBJECT_ID('gold.person_giving', 'V') IS NOT NULL DROP VIEW gold.person_giving
  `);

  await pool.request().query(`
    CREATE VIEW gold.person_giving AS
    SELECT
      p.master_id,
      p.display_name,
      p.email_primary,
      p.primary_source,
      p.source_systems,
      ISNULL(g.donation_count, 0) AS donation_count,
      ISNULL(g.total_given, 0) AS total_given,
      g.avg_gift,
      g.largest_gift,
      g.first_gift_date,
      g.last_gift_date,
      DATEDIFF(DAY, g.last_gift_date, GETDATE()) AS days_since_last,
      g.fund_count,
      g.active_months
    FROM gold.person p
    LEFT JOIN (
      SELECT
        im.master_id,
        COUNT(*) AS donation_count,
        SUM(d.amount) AS total_given,
        AVG(d.amount) AS avg_gift,
        MAX(d.amount) AS largest_gift,
        MIN(d.donated_at) AS first_gift_date,
        MAX(d.donated_at) AS last_gift_date,
        COUNT(DISTINCT d.fund_code) AS fund_count,
        COUNT(DISTINCT FORMAT(d.donated_at, 'yyyy-MM')) AS active_months
      FROM silver.donation d
      JOIN silver.identity_map im ON im.source_system = d.source_system
        AND im.source_id = d.contact_source_id
      GROUP BY im.master_id
    ) g ON g.master_id = p.master_id
  `);

  // ── Step 8: Create gold.person_commerce — order/purchase summary ──
  console.log('Step 8: Creating gold.person_commerce view...');

  await pool.request().query(`
    IF OBJECT_ID('gold.person_commerce', 'V') IS NOT NULL DROP VIEW gold.person_commerce
  `);

  await pool.request().query(`
    CREATE VIEW gold.person_commerce AS
    SELECT
      p.master_id,
      p.display_name,
      p.email_primary,
      ISNULL(o.order_count, 0) AS order_count,
      ISNULL(o.total_spent, 0) AS total_spent,
      o.first_order_date,
      o.last_order_date,
      ISNULL(inv.invoice_count, 0) AS invoice_count,
      ISNULL(inv.total_invoiced, 0) AS total_invoiced,
      ISNULL(inv.total_paid, 0) AS total_paid,
      ISNULL(pay.payment_count, 0) AS payment_count,
      ISNULL(pay.total_payments, 0) AS total_payments
    FROM gold.person p
    LEFT JOIN (
      SELECT
        im.master_id,
        COUNT(*) AS order_count,
        SUM(oi.price_per_unit * oi.qty) AS total_spent,
        MIN(o.created_at) AS first_order_date,
        MAX(o.created_at) AS last_order_date
      FROM silver.[order] o
      JOIN silver.identity_map im ON im.source_system = 'keap'
        AND TRY_CAST(im.source_id AS INT) = o.contact_keap_id
      LEFT JOIN silver.order_item oi ON oi.order_keap_id = o.keap_id
      GROUP BY im.master_id
    ) o ON o.master_id = p.master_id
    LEFT JOIN (
      SELECT
        im.master_id,
        COUNT(*) AS invoice_count,
        SUM(i.total) AS total_invoiced,
        SUM(i.total_paid) AS total_paid
      FROM silver.invoice i
      JOIN silver.identity_map im ON im.source_system = 'keap'
        AND TRY_CAST(im.source_id AS INT) = i.contact_keap_id
      GROUP BY im.master_id
    ) inv ON inv.master_id = p.master_id
    LEFT JOIN (
      SELECT
        im.master_id,
        COUNT(*) AS payment_count,
        SUM(py.amount) AS total_payments
      FROM silver.payment py
      JOIN silver.identity_map im ON im.source_system = 'keap'
        AND TRY_CAST(im.source_id AS INT) = py.contact_keap_id
      GROUP BY im.master_id
    ) pay ON pay.master_id = p.master_id
  `);

  // ── Step 9: Create gold.person_engagement — notes/comms summary ──
  console.log('Step 9: Creating gold.person_engagement view...');

  await pool.request().query(`
    IF OBJECT_ID('gold.person_engagement', 'V') IS NOT NULL DROP VIEW gold.person_engagement
  `);

  await pool.request().query(`
    CREATE VIEW gold.person_engagement AS
    SELECT
      p.master_id,
      p.display_name,
      ISNULL(n.note_count, 0) AS note_count,
      n.last_note_date,
      ISNULL(cm.comm_count, 0) AS comm_count,
      cm.last_comm_date,
      ISNULL(n.note_count, 0) + ISNULL(cm.comm_count, 0) AS total_interactions
    FROM gold.person p
    LEFT JOIN (
      SELECT
        im.master_id,
        COUNT(*) AS note_count,
        MAX(nt.created_at) AS last_note_date
      FROM silver.note nt
      JOIN silver.identity_map im ON im.source_system = nt.source_system
        AND im.source_id = nt.contact_source_id
      GROUP BY im.master_id
    ) n ON n.master_id = p.master_id
    LEFT JOIN (
      SELECT
        im.master_id,
        COUNT(*) AS comm_count,
        MAX(cm.comm_date) AS last_comm_date
      FROM silver.communication cm
      JOIN silver.identity_map im ON im.source_system = cm.source_system
        AND im.source_id = cm.contact_source_id
      GROUP BY im.master_id
    ) cm ON cm.master_id = p.master_id
  `);

  // ── Step 10: Create gold.constituent_360 — full picture ──
  console.log('Step 10: Creating gold.constituent_360 view...');

  await pool.request().query(`
    IF OBJECT_ID('gold.constituent_360', 'V') IS NOT NULL DROP VIEW gold.constituent_360
  `);

  await pool.request().query(`
    CREATE VIEW gold.constituent_360 AS
    SELECT
      p.master_id,
      p.display_name,
      p.first_name,
      p.last_name,
      p.email_primary,
      p.phone_primary,
      p.city,
      p.state,
      p.postal_code,
      p.date_of_birth,
      p.gender,
      p.spouse_name,
      p.household_name,
      p.organization_name,
      p.primary_source,
      p.source_count,
      p.source_systems,
      -- Giving
      ISNULL(g.donation_count, 0) AS donation_count,
      ISNULL(g.total_given, 0) AS total_given,
      g.avg_gift,
      g.largest_gift,
      g.first_gift_date,
      g.last_gift_date,
      DATEDIFF(DAY, g.last_gift_date, GETDATE()) AS days_since_last_gift,
      -- Commerce
      ISNULL(c.order_count, 0) AS order_count,
      ISNULL(c.total_spent, 0) AS total_spent,
      c.last_order_date,
      ISNULL(c.total_payments, 0) AS total_payments,
      -- Engagement
      ISNULL(e.note_count, 0) AS note_count,
      ISNULL(e.comm_count, 0) AS comm_count,
      ISNULL(e.total_interactions, 0) AS total_interactions,
      -- Composite
      ISNULL(g.total_given, 0) + ISNULL(c.total_spent, 0) AS lifetime_value
    FROM gold.person p
    LEFT JOIN gold.person_giving g ON g.master_id = p.master_id
    LEFT JOIN gold.person_commerce c ON c.master_id = p.master_id
    LEFT JOIN gold.person_engagement e ON e.master_id = p.master_id
  `);

  // ── Step 11: Create gold.monthly_trends ──
  console.log('Step 11: Creating gold.monthly_trends view...');

  await pool.request().query(`
    IF OBJECT_ID('gold.monthly_trends', 'V') IS NOT NULL DROP VIEW gold.monthly_trends
  `);

  await pool.request().query(`
    CREATE VIEW gold.monthly_trends AS
    SELECT
      FORMAT(d.donated_at, 'yyyy-MM') AS month,
      YEAR(d.donated_at) AS yr,
      MONTH(d.donated_at) AS mo,
      COUNT(*) AS donation_count,
      COUNT(DISTINCT im.master_id) AS unique_donors,
      SUM(d.amount) AS total_amount,
      AVG(d.amount) AS avg_gift,
      MAX(d.amount) AS max_gift,
      d.source_system
    FROM silver.donation d
    LEFT JOIN silver.identity_map im ON im.source_system = d.source_system
      AND im.source_id = d.contact_source_id
    WHERE d.donated_at IS NOT NULL
    GROUP BY FORMAT(d.donated_at, 'yyyy-MM'), YEAR(d.donated_at), MONTH(d.donated_at), d.source_system
  `);

  // ── Step 12: Create gold.product_summary ──
  console.log('Step 12: Creating gold.product_summary view...');

  await pool.request().query(`
    IF OBJECT_ID('gold.product_summary', 'V') IS NOT NULL DROP VIEW gold.product_summary
  `);

  await pool.request().query(`
    CREATE VIEW gold.product_summary AS
    SELECT
      p.keap_id AS product_id,
      p.name AS product_name,
      p.price AS list_price,
      p.sku,
      p.status,
      ISNULL(s.times_ordered, 0) AS times_ordered,
      ISNULL(s.total_qty, 0) AS total_qty_sold,
      ISNULL(s.total_revenue, 0) AS total_revenue,
      s.first_ordered,
      s.last_ordered
    FROM silver.product p
    LEFT JOIN (
      SELECT
        oi.product_keap_id,
        COUNT(*) AS times_ordered,
        SUM(oi.qty) AS total_qty,
        SUM(oi.price_per_unit * oi.qty) AS total_revenue,
        MIN(oi.created_at) AS first_ordered,
        MAX(oi.created_at) AS last_ordered
      FROM silver.order_item oi
      WHERE oi.product_keap_id IS NOT NULL
      GROUP BY oi.product_keap_id
    ) s ON s.product_keap_id = p.keap_id
  `);

  // ── Step 13: Create gold.subscription_health ──
  console.log('Step 13: Creating gold.subscription_health view...');

  await pool.request().query(`
    IF OBJECT_ID('gold.subscription_health', 'V') IS NOT NULL DROP VIEW gold.subscription_health
  `);

  await pool.request().query(`
    CREATE VIEW gold.subscription_health AS
    SELECT
      sub.subscription_id,
      p.master_id,
      p.display_name,
      p.email_primary,
      sub.billing_amount,
      sub.billing_cycle,
      sub.frequency,
      sub.status,
      sub.start_date,
      sub.end_date,
      sub.next_bill_date,
      sub.reason_stopped,
      sub.product_id,
      pr.name AS product_name,
      CASE
        WHEN sub.status = 'Active' AND sub.next_bill_date < GETDATE() THEN 'at_risk'
        WHEN sub.status = 'Active' THEN 'active'
        WHEN sub.status = 'Inactive' AND sub.reason_stopped IS NOT NULL THEN 'churned'
        ELSE LOWER(ISNULL(sub.status, 'unknown'))
      END AS health_status
    FROM silver.subscription sub
    JOIN silver.identity_map im ON im.source_system = 'keap'
      AND TRY_CAST(im.source_id AS INT) = sub.contact_keap_id
    JOIN gold.person p ON p.master_id = im.master_id
    LEFT JOIN silver.product pr ON pr.keap_id = sub.product_id
  `);

  // ── Final summary ─────────────────────────────────
  console.log('\n' + '='.repeat(60));
  console.log('IDENTITY RESOLUTION COMPLETE');
  console.log('='.repeat(60));
  console.log(`Contacts processed: ${allContacts.length.toLocaleString()}`);
  console.log(`Unique people (master IDs): ${clusters.size.toLocaleString()}`);
  console.log(`Dedup ratio: ${((1 - clusters.size / allContacts.length) * 100).toFixed(1)}%`);
  console.log(`\nMerges by method:`);
  console.log(`  Cross-reference (GB→Keap/DD): ${mergesByMethod.crossref.toLocaleString()}`);
  console.log(`  Email match: ${mergesByMethod.email.toLocaleString()}`);
  console.log(`  Phone match: ${mergesByMethod.phone.toLocaleString()}`);
  console.log(`  Name+Zip match: ${mergesByMethod.namezip.toLocaleString()}`);
  console.log(`\nGold views created:`);
  console.log(`  gold.person — unified master contact`);
  console.log(`  gold.person_giving — donation summary per person`);
  console.log(`  gold.person_commerce — order/invoice summary per person`);
  console.log(`  gold.person_engagement — notes/comms summary per person`);
  console.log(`  gold.constituent_360 — full picture (giving + commerce + engagement)`);
  console.log(`  gold.monthly_trends — giving trends by month + source`);
  console.log(`  gold.product_summary — product performance`);
  console.log(`  gold.subscription_health — subscription status`);

  await pool.close();
  console.log('\nDone.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
