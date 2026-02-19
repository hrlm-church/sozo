/**
 * Build RAG Index — Generate person 360 documents, embed, and push to Azure AI Search.
 *
 * For each person in serving.person_360, generates a rich text document describing
 * their giving, commerce, events, subscriptions, tags, and engagement. Then generates
 * vector embeddings using OpenAI text-embedding-3-small and pushes to Azure AI Search.
 *
 * Usage: node scripts/pipeline/build_rag_index.js [--from=N] [--batch=100] [--dry-run]
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

const INDEX_NAME = 'sozo-360-v1';
const EMBED_MODEL = 'text-embedding-3-small';
const EMBED_BATCH = 50;      // texts per embedding API call
const SEARCH_BATCH = 500;    // docs per search upload batch
const PERSON_BATCH = 1000;   // persons per SQL batch

// ── Embedding ────────────────────────────────────────────────────────────────

async function embedTexts(texts) {
  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('OPENAI_API_KEY not set');

  const results = [];
  for (let i = 0; i < texts.length; i += EMBED_BATCH) {
    const batch = texts.slice(i, i + EMBED_BATCH);
    const res = await fetch('https://api.openai.com/v1/embeddings', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${apiKey}` },
      body: JSON.stringify({ model: EMBED_MODEL, input: batch }),
    });
    if (!res.ok) {
      const err = await res.text();
      throw new Error(`Embedding API error ${res.status}: ${err.substring(0, 200)}`);
    }
    const body = await res.json();
    for (const d of body.data) {
      results.push(d.embedding);
    }
    if (i + EMBED_BATCH < texts.length) await wait(200); // rate limit
  }
  return results;
}

// ── Search upload ────────────────────────────────────────────────────────────

async function uploadDocs(docs) {
  const serviceName = process.env.SOZO_SEARCH_SERVICE_NAME;
  const adminKey = process.env.SOZO_SEARCH_ADMIN_KEY;

  const endpoint = `https://${serviceName}.search.windows.net/indexes/${INDEX_NAME}/docs/index?api-version=2024-07-01`;

  for (let i = 0; i < docs.length; i += SEARCH_BATCH) {
    const batch = docs.slice(i, i + SEARCH_BATCH);
    const res = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'api-key': adminKey },
      body: JSON.stringify({ value: batch.map(d => ({ '@search.action': 'mergeOrUpload', ...d })) }),
    });
    if (!res.ok) {
      const err = await res.text();
      console.error(`  Search upload error ${res.status}: ${err.substring(0, 200)}`);
    }
    if (i + SEARCH_BATCH < docs.length) await wait(500);
  }
}

// ── Document generation ─────────────────────────────────────────────────────

function fmt$(n) {
  if (!n || n === 0) return '$0';
  if (n >= 1000000) return `$${(n/1000000).toFixed(1)}M`;
  if (n >= 1000) return `$${(n/1000).toFixed(1)}K`;
  return `$${n.toFixed(0)}`;
}

function buildPersonDoc(person, donations, tags, events, subscriptions, stripeCharges, wooOrders) {
  const lines = [];

  lines.push(`PERSON: ${person.display_name}`);
  if (person.email) lines.push(`EMAIL: ${person.email}`);
  const loc = [person.city, person.state, person.postal_code].filter(Boolean).join(', ');
  if (loc) lines.push(`LOCATION: ${loc}`);
  lines.push(`LIFECYCLE: ${person.lifecycle_stage || 'prospect'}`);
  lines.push(`PRIMARY SOURCE: ${person.primary_source || 'unknown'}`);

  // Giving
  if (person.donation_count > 0) {
    lines.push(`GIVING: ${fmt$(person.lifetime_giving)} across ${person.donation_count} gifts (avg ${fmt$(person.avg_gift)}). First: ${person.first_gift_date || 'N/A'}, Last: ${person.last_gift_date || 'N/A'}.`);
    if (donations.length > 0) {
      const funds = [...new Set(donations.map(d => d.fund).filter(Boolean))].slice(0, 5);
      const sources = [...new Set(donations.map(d => d.source_system).filter(Boolean))];
      if (funds.length) lines.push(`  Funds: ${funds.join(', ')}`);
      if (sources.length) lines.push(`  Sources: ${sources.join(', ')}`);
    }
  }

  // Keap Commerce
  if (person.order_count > 0) {
    lines.push(`KEAP COMMERCE: ${person.order_count} orders, ${fmt$(person.total_spent)} total.`);
  }

  // WooCommerce
  if (person.woo_order_count > 0) {
    lines.push(`WOOCOMMERCE: ${person.woo_order_count} orders, ${fmt$(person.woo_total_spent)} total.`);
    if (wooOrders.length > 0) {
      const products = [...new Set(wooOrders.map(o => o.product_name).filter(Boolean))].slice(0, 5);
      if (products.length) lines.push(`  Products: ${products.join(', ')}`);
    }
  }

  // Shopify
  if (person.shopify_order_count > 0) {
    lines.push(`SHOPIFY: ${person.shopify_order_count} orders, ${fmt$(person.shopify_total_spent)} total.`);
  }

  // Events
  if (person.ticket_count > 0) {
    lines.push(`EVENTS: ${person.ticket_count} tickets.`);
    if (events.length > 0) {
      const eventNames = [...new Set(events.map(e => e.event_name).filter(Boolean))].slice(0, 5);
      if (eventNames.length) lines.push(`  Events: ${eventNames.join(', ')}`);
    }
  }

  // Subscriptions
  if (person.subbly_sub_count > 0) {
    lines.push(`SUBSCRIPTIONS: ${person.subbly_sub_count} Subbly subscriptions. Active: ${person.subbly_active ? 'Yes' : 'No'}.`);
    if (subscriptions.length > 0) {
      const products = [...new Set(subscriptions.map(s => s.product_name).filter(Boolean))];
      const statuses = [...new Set(subscriptions.map(s => s.subscription_status).filter(Boolean))];
      if (products.length) lines.push(`  Products: ${products.join(', ')}`);
      if (statuses.length) lines.push(`  Status: ${statuses.join(', ')}`);
    }
  }

  // Stripe
  if (person.stripe_charge_count > 0) {
    lines.push(`STRIPE: ${person.stripe_charge_count} charges, ${fmt$(person.stripe_total)} total.`);
  }

  // Tags
  if (tags.length > 0) {
    const tagValues = [...new Set(tags.map(t => t.tag_value).filter(Boolean))].slice(0, 20);
    lines.push(`TAGS (${person.tag_count || tags.length}): ${tagValues.join(', ')}`);
  }

  // Engagement
  if (person.note_count > 0 || person.comm_count > 0) {
    const parts = [];
    if (person.note_count > 0) parts.push(`${person.note_count} notes`);
    if (person.comm_count > 0) parts.push(`${person.comm_count} communications`);
    lines.push(`ENGAGEMENT: ${parts.join(', ')}`);
  }

  return lines.join('\n');
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main() {
  loadEnv();
  const args = process.argv.slice(2);
  const fromArg = args.find(a => a.startsWith('--from='));
  const fromId = fromArg ? parseInt(fromArg.split('=')[1]) : 0;
  const dryRun = args.includes('--dry-run');

  if (!process.env.OPENAI_API_KEY) {
    console.error('OPENAI_API_KEY required for embeddings');
    process.exit(1);
  }
  if (!process.env.SOZO_SEARCH_SERVICE_NAME || !process.env.SOZO_SEARCH_ADMIN_KEY) {
    console.error('SOZO_SEARCH_SERVICE_NAME and SOZO_SEARCH_ADMIN_KEY required');
    process.exit(1);
  }

  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST, database: 'sozov2',
    user: process.env.SOZO_SQL_USER, password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 120000,
    pool: { max: 3, min: 1 },
  });

  console.log('Connected to sozov2.');

  // Get total person count
  const countRes = await pool.request().query('SELECT COUNT(*) AS n FROM serving.person_360');
  const totalPersons = countRes.recordset[0].n;
  console.log(`Total persons: ${totalPersons.toLocaleString()}`);
  console.log(`Starting from person_id > ${fromId}`);
  if (dryRun) console.log('DRY RUN — will generate docs but not embed or upload');

  let processed = 0;
  let lastId = fromId;
  const startTime = Date.now();

  while (true) {
    // Batch of persons
    const persons = await pool.request().query(`
      SELECT TOP ${PERSON_BATCH} person_id, display_name, first_name, last_name, email, phone,
        city, state, postal_code, primary_source, lifecycle_stage,
        donation_count, lifetime_giving, avg_gift, largest_gift, first_gift_date, last_gift_date,
        order_count, total_spent, woo_order_count, woo_total_spent,
        shopify_order_count, shopify_total_spent, ticket_count,
        subbly_sub_count, subbly_active, stripe_charge_count, stripe_total,
        tag_count, note_count, comm_count
      FROM serving.person_360
      WHERE person_id > ${lastId}
      ORDER BY person_id
    `);

    if (persons.recordset.length === 0) break;
    lastId = persons.recordset[persons.recordset.length - 1].person_id;

    const personIds = persons.recordset.map(p => p.person_id);
    const idList = personIds.join(',');

    // Batch load related data for all persons in this batch
    const [donationsRes, tagsRes, eventsRes, subsRes, stripeRes, wooRes] = await Promise.all([
      pool.request().query(`SELECT person_id, amount, fund, source_system FROM serving.donation_detail WHERE person_id IN (${idList})`).catch(() => ({ recordset: [] })),
      pool.request().query(`SELECT person_id, tag_value FROM serving.tag_detail WHERE person_id IN (${idList})`).catch(() => ({ recordset: [] })),
      pool.request().query(`SELECT person_id, event_name FROM serving.event_detail WHERE person_id IN (${idList})`).catch(() => ({ recordset: [] })),
      pool.request().query(`SELECT person_id, product_name, subscription_status FROM serving.subscription_detail WHERE person_id IN (${idList})`).catch(() => ({ recordset: [] })),
      pool.request().query(`SELECT person_id FROM serving.stripe_charge_detail WHERE person_id IN (${idList}) AND person_id > 0`).catch(() => ({ recordset: [] })),
      pool.request().query(`SELECT person_id, product_name FROM serving.woo_order_detail WHERE person_id IN (${idList}) AND person_id > 0`).catch(() => ({ recordset: [] })),
    ]);

    // Group by person_id
    const groupBy = (rows, key = 'person_id') => {
      const map = {};
      for (const r of rows) { (map[r[key]] = map[r[key]] || []).push(r); }
      return map;
    };
    const donByPerson = groupBy(donationsRes.recordset);
    const tagByPerson = groupBy(tagsRes.recordset);
    const evtByPerson = groupBy(eventsRes.recordset);
    const subByPerson = groupBy(subsRes.recordset);
    const strByPerson = groupBy(stripeRes.recordset);
    const wooByPerson = groupBy(wooRes.recordset);

    // Generate documents
    const docs = [];
    const texts = [];

    for (const person of persons.recordset) {
      const pid = person.person_id;
      const content = buildPersonDoc(
        person,
        donByPerson[pid] || [],
        tagByPerson[pid] || [],
        evtByPerson[pid] || [],
        subByPerson[pid] || [],
        strByPerson[pid] || [],
        wooByPerson[pid] || [],
      );

      const tagText = (tagByPerson[pid] || []).map(t => t.tag_value).filter(Boolean).join(', ');
      const loc = [person.city, person.state, person.postal_code].filter(Boolean).join(', ');

      docs.push({
        id: `person-${pid}`,
        doc_type: 'person_360',
        person_id: pid,
        display_name: person.display_name || 'Unknown',
        email: person.email || '',
        location: loc,
        lifecycle_stage: person.lifecycle_stage || 'prospect',
        content,
        tags_text: tagText.substring(0, 32000),
        giving_total: person.lifetime_giving || 0,
        order_count: (person.order_count || 0) + (person.woo_order_count || 0) + (person.shopify_order_count || 0),
        event_count: person.ticket_count || 0,
        has_subscription: (person.subbly_active || 0) > 0,
      });
      texts.push(content.substring(0, 8000)); // embedding input limit
    }

    if (!dryRun) {
      // Generate embeddings
      const embeddings = await embedTexts(texts);

      // Attach embeddings to docs
      for (let i = 0; i < docs.length; i++) {
        docs[i].content_vector = embeddings[i];
      }

      // Upload to Azure AI Search
      await uploadDocs(docs);
    }

    processed += docs.length;
    const elapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
    const rate = (processed / ((Date.now() - startTime) / 1000)).toFixed(1);
    console.log(`  ${processed.toLocaleString()} / ${totalPersons.toLocaleString()} persons (${elapsed} min, ${rate}/sec) — last_id=${lastId}`);

    await wait(300);
  }

  const totalElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
  console.log(`\nDone. ${processed.toLocaleString()} documents indexed in ${totalElapsed} minutes.`);

  await pool.close();
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
