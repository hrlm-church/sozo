/**
 * Create & Populate Product Classification Table — sozov2
 *
 * Creates silver.product_classification and classifies every product by
 * revenue_category: donation, commerce, subscription, event, shipping, tax, discount, unknown.
 *
 * Two row types:
 *   1. Product-level (product_keap_id set) — matched via oi.product_keap_id
 *   2. Item-name-level (item_name_pattern set) — matched for orphaned items (product_keap_id = 0/NULL)
 *
 * Idempotent: safe to re-run (drops and recreates).
 *
 * Usage: node scripts/pipeline/create_product_classification.js
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

// ── Classification rules ────────────────────────────────────────────

function classifyProduct(name, price) {
  const n = (name || '').trim();
  const nl = n.toLowerCase();
  const p = parseFloat(price) || 0;

  // ── DONATION ──────────────────────────────────────────────────────
  if (n.startsWith('Donation:')) return { cat: 'donation', sub: n.includes('Monthly') ? 'recurring' : 'one_time' };
  if (nl.includes('giving donation')) return { cat: 'donation', sub: 'giving_program' };
  if (n.startsWith('Freedom Partner')) return { cat: 'donation', sub: 'freedom_partner' };
  if (n === 'Become a Founding Farmer') return { cat: 'donation', sub: 'founding_farmer' };
  if (n === 'Clothed in Dignity') return { cat: 'donation', sub: 'clothed_in_dignity' };
  if (n.startsWith('Crazy for Jesus Giving')) return { cat: 'donation', sub: 'crazy_for_jesus' };
  if (n.startsWith('A Night of Worship')) return { cat: 'donation', sub: 'name_your_price' };
  // Name-your-price workshops (listed at $0 or $1)
  if (p <= 1 && (
    nl.includes('workshop') ||
    nl.includes('how to talk to your kids') ||
    nl.includes("let's talk about sex") ||
    nl.includes('bffs mom')
  )) return { cat: 'donation', sub: 'name_your_price' };

  // ── SUBSCRIPTION ──────────────────────────────────────────────────
  if (n.startsWith('TGS')) return { cat: 'subscription', sub: 'true_girl_box' };
  if (nl.includes('physical monthly subscription')) return { cat: 'subscription', sub: 'monthly_box' };
  if (nl.includes('physical yearly subscription')) return { cat: 'subscription', sub: 'yearly_box' };
  if (nl.includes('monthly physical subscription')) return { cat: 'subscription', sub: 'monthly_box' };
  if (nl.includes('yearly physical subscription')) return { cat: 'subscription', sub: 'yearly_box' };
  if (nl.includes('digital subscription')) return { cat: 'subscription', sub: 'digital_box' };
  if (nl === 'true girl subscription box' || nl === 'true girl subscription box.') return { cat: 'subscription', sub: 'true_girl_box' };
  if (nl.includes('spanish digital subscription')) return { cat: 'subscription', sub: 'digital_box' };
  if (nl === 'true girl box fulfillment') return { cat: 'subscription', sub: 'box_fulfillment' };
  if (nl.includes('brave box')) return { cat: 'subscription', sub: 'brave_box' };
  if (nl === 'digital gift card') return { cat: 'subscription', sub: 'gift_card' };

  // ── EVENT ─────────────────────────────────────────────────────────
  if (nl.includes('pajama party')) return { cat: 'event', sub: 'pajama_party_tour' };
  if (nl.includes('crazy hair')) return { cat: 'event', sub: 'crazy_hair_tour' };
  if (n.startsWith('CHT')) return { cat: 'event', sub: 'crazy_hair_tour' };
  if (nl.includes('pop-up') || nl.includes('pop up')) return { cat: 'event', sub: 'pop_up_party' };
  if (nl.includes('master class')) return { cat: 'event', sub: 'master_class' };
  if (n.startsWith('True Girl Global')) return { cat: 'event', sub: 'true_girl_global' };
  if (n.startsWith('True Girl Live')) return { cat: 'event', sub: 'farm_event' };
  if (nl.includes('livestream')) return { cat: 'event', sub: 'livestream' };
  if (nl.includes('b2bb') && (nl.includes('tour') || nl.includes('ga ') || nl.includes('general admission') || nl.includes('vip') || nl.includes('meet') || nl.includes('behind'))) return { cat: 'event', sub: 'b2bb_tour' };
  if (nl.startsWith('fall \'') || nl.startsWith('fall \'')) {
    if (nl.includes('b2bb') || nl.includes('born to be brave')) return { cat: 'event', sub: 'b2bb_tour' };
  }
  if (nl.includes('flourish')) return { cat: 'event', sub: 'flourish' };
  if (nl.includes('sponsorship') || (nl.includes('sponsor') && p >= 1000)) return { cat: 'event', sub: 'sponsorship' };
  // Tour ticket patterns: "City, ST - Tier" or "City ST - Tier"
  if (nl.includes('general admission') || nl.includes('ga group') || nl.includes('vip meet') || nl.includes('vip behind') || nl.includes('premium pass')) {
    return { cat: 'event', sub: 'tour_ticket' };
  }
  // FALL B2BB patterns
  if (/^FALL\s+'?\d{2}/i.test(n) && (nl.includes('b2bb') || nl.includes('born'))) return { cat: 'event', sub: 'b2bb_tour' };

  // ── SHIPPING ──────────────────────────────────────────────────────
  if (nl === 'shipping' || nl === 'shipping & handling' || nl.includes('shipping')) return { cat: 'shipping', sub: null };

  // ── TAX ───────────────────────────────────────────────────────────
  if (nl === 'tax') return { cat: 'tax', sub: null };

  // ── COMMERCE (default) ────────────────────────────────────────────
  return { cat: 'commerce', sub: null };
}

// Orphaned item-name classifications (product_keap_id = 0 or NULL)
const ORPHAN_RULES = [
  { pattern: 'Special Category', cat: 'commerce', sub: 'tour_merch' },
  { pattern: 'Product', cat: 'commerce', sub: 'books_media' },
  { pattern: 'Shipping', cat: 'shipping', sub: null },
  { pattern: 'Free Shipping', cat: 'shipping', sub: null },
  { pattern: 'International Shipping', cat: 'shipping', sub: null },
  { pattern: 'Charge for Shipping', cat: 'shipping', sub: null },
  { pattern: 'Cost for shipping 2 B2BB boxes', cat: 'shipping', sub: null },
  { pattern: 'Tax', cat: 'tax', sub: null },
  { pattern: 'Transaction Fee', cat: 'tax', sub: null },
  { pattern: 'Special', cat: 'discount', sub: null },
  { pattern: 'Discount for New Subscription', cat: 'discount', sub: null },
  { pattern: 'TPFREE', cat: 'discount', sub: null },
  { pattern: 'CHRISTIAN MISS', cat: 'discount', sub: null },
  { pattern: 'Payment Difference (1 Girl Plan to 2 Girl Plan)', cat: 'commerce', sub: null },
  { pattern: 'Donation: Pure Freedom Before 13 (Monthly)', cat: 'donation', sub: 'recurring' },
  { pattern: 'Donation: MyTrueGirl', cat: 'donation', sub: 'one_time' },
  { pattern: 'Stanley', cat: 'commerce', sub: null },
  { pattern: 'B2BB socks', cat: 'commerce', sub: null },
  { pattern: 'Charge for B2BB Box', cat: 'subscription', sub: 'brave_box' },
  { pattern: 'Miriam: Becoming a Girl of Courage: Book', cat: 'commerce', sub: 'books_media' },
  { pattern: 'True Girl: The Original Album', cat: 'commerce', sub: null },
  { pattern: 'TG Charm bracelet ($5) and 4 charms ($2 each)', cat: 'commerce', sub: null },
  { pattern: 'Texas VIP Pass', cat: 'event', sub: 'pop_up_party' },
  { pattern: 'Columbus, OH Show', cat: 'event', sub: null },
  { pattern: 'General Admission Group - Broadview Heights, OH', cat: 'event', sub: 'crazy_hair_tour' },
  { pattern: 'CHT Spring 25 | Cypress, TX | Behind the Scenes', cat: 'event', sub: 'crazy_hair_tour' },
  { pattern: 'Pajama Party Tour: October 22- Indian Trail, NC VIP Meet and Greet', cat: 'event', sub: 'pajama_party_tour' },
];

// ── Main ────────────────────────────────────────────────────────────

async function main() {
  loadEnv();
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 120000,
  });

  // 1. Drop and recreate table
  console.log('Creating silver.product_classification...');
  await pool.request().query(`
    IF OBJECT_ID('silver.product_classification', 'U') IS NOT NULL
      DROP TABLE silver.product_classification
  `);
  await pool.request().query(`
    CREATE TABLE silver.product_classification (
      classification_id  INT IDENTITY(1,1) PRIMARY KEY,
      product_keap_id    INT NULL,
      item_name_pattern  NVARCHAR(500) NULL,
      revenue_category   VARCHAR(50) NOT NULL,
      subcategory        VARCHAR(100) NULL,
      match_priority     INT DEFAULT 100,
      notes              NVARCHAR(500) NULL,
      created_at         DATETIME2 DEFAULT GETDATE()
    )
  `);

  // 2. Load all products from silver.product
  console.log('Loading products from silver.product...');
  const products = await pool.request().query(`
    SELECT keap_id, name, price, sku, status, is_digital, shippable
    FROM silver.product
    ORDER BY keap_id
  `);
  console.log(`  Found ${products.recordset.length} products`);

  // 3. Classify each product
  const counts = {};
  const rows = [];
  for (const prod of products.recordset) {
    const { cat, sub } = classifyProduct(prod.name, prod.price);
    counts[cat] = (counts[cat] || 0) + 1;
    rows.push({ keap_id: prod.keap_id, name: prod.name, cat, sub });
  }

  console.log('\nClassification summary (products):');
  for (const [cat, cnt] of Object.entries(counts).sort((a, b) => b[1] - a[1])) {
    console.log(`  ${cat}: ${cnt}`);
  }

  // 4. Batch insert product-level rows
  console.log('\nInserting product classifications...');
  const BATCH = 50;
  for (let i = 0; i < rows.length; i += BATCH) {
    const batch = rows.slice(i, i + BATCH);
    const values = batch.map(r => {
      const name = (r.name || '').replace(/'/g, "''").substring(0, 400);
      const sub = r.sub ? `N'${r.sub}'` : 'NULL';
      return `(${r.keap_id}, NULL, '${r.cat}', ${sub}, 10, N'${name}')`;
    }).join(',\n    ');
    await pool.request().query(`
      INSERT INTO silver.product_classification
        (product_keap_id, item_name_pattern, revenue_category, subcategory, match_priority, notes)
      VALUES ${values}
    `);
  }
  console.log(`  Inserted ${rows.length} product-level rows`);

  // 5. Insert item-name pattern rows for orphaned items
  console.log('\nInserting orphan item-name rules...');
  for (const rule of ORPHAN_RULES) {
    const pattern = rule.pattern.replace(/'/g, "''");
    const sub = rule.sub ? `N'${rule.sub}'` : 'NULL';
    await pool.request().query(`
      INSERT INTO silver.product_classification
        (product_keap_id, item_name_pattern, revenue_category, subcategory, match_priority, notes)
      VALUES (NULL, N'${pattern}', '${rule.cat}', ${sub}, 50, N'orphan item-name rule')
    `);
  }
  console.log(`  Inserted ${ORPHAN_RULES.length} item-name rules`);

  // 6. Add indexes
  console.log('\nCreating indexes...');
  await pool.request().query(`
    CREATE NONCLUSTERED INDEX IX_pc_product_keap_id
      ON silver.product_classification(product_keap_id)
      WHERE product_keap_id IS NOT NULL
  `);
  await pool.request().query(`
    CREATE NONCLUSTERED INDEX IX_pc_item_name_pattern
      ON silver.product_classification(item_name_pattern)
      WHERE item_name_pattern IS NOT NULL
  `);

  // 7. Verification: check how well we cover order_item rows
  console.log('\nVerification — order item coverage:');
  const coverage = await pool.request().query(`
    SELECT
      COALESCE(pc.revenue_category, pcn.revenue_category, 'unclassified') AS category,
      COUNT(*) AS line_items,
      SUM(oi.price_per_unit * oi.qty) AS total_revenue
    FROM silver.order_item oi
    LEFT JOIN silver.product_classification pc
      ON pc.product_keap_id = oi.product_keap_id
      AND pc.product_keap_id IS NOT NULL
      AND oi.product_keap_id > 0
    LEFT JOIN silver.product_classification pcn
      ON pcn.item_name_pattern = oi.item_name
      AND pcn.product_keap_id IS NULL
      AND (oi.product_keap_id IS NULL OR oi.product_keap_id = 0)
    GROUP BY COALESCE(pc.revenue_category, pcn.revenue_category, 'unclassified')
    ORDER BY total_revenue DESC
  `);
  for (const row of coverage.recordset) {
    console.log(`  ${row.category}: ${row.line_items.toLocaleString()} items, $${(row.total_revenue || 0).toLocaleString()}`);
  }

  // 8. Check for unclassified items
  const unclassified = await pool.request().query(`
    SELECT TOP 20 oi.item_name, oi.product_keap_id,
      COUNT(*) AS cnt, SUM(oi.price_per_unit * oi.qty) AS revenue
    FROM silver.order_item oi
    LEFT JOIN silver.product_classification pc
      ON pc.product_keap_id = oi.product_keap_id
      AND pc.product_keap_id IS NOT NULL
      AND oi.product_keap_id > 0
    LEFT JOIN silver.product_classification pcn
      ON pcn.item_name_pattern = oi.item_name
      AND pcn.product_keap_id IS NULL
      AND (oi.product_keap_id IS NULL OR oi.product_keap_id = 0)
    WHERE pc.revenue_category IS NULL AND pcn.revenue_category IS NULL
    GROUP BY oi.item_name, oi.product_keap_id
    ORDER BY SUM(oi.price_per_unit * oi.qty) DESC
  `);
  if (unclassified.recordset.length > 0) {
    console.log('\nUnclassified items (defaulting to commerce):');
    for (const row of unclassified.recordset) {
      console.log(`  "${row.item_name}" (keap_id=${row.product_keap_id}): ${row.cnt} items, $${(row.revenue || 0).toLocaleString()}`);
    }
  } else {
    console.log('\nAll items classified!');
  }

  await pool.close();
  console.log('\nDone.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
