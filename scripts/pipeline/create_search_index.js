/**
 * Create Azure AI Search vector index for Sozo 360 person profiles.
 *
 * Creates the 'sozo-360-v1' index with:
 * - Standard searchable fields (name, email, location, content)
 * - Vector field for semantic search (1536-dim text-embedding-3-small)
 * - Filterable/facetable fields for lifecycle, giving, etc.
 *
 * Usage: node scripts/pipeline/create_search_index.js [--delete-first]
 */
const fs = require('fs');
const path = require('path');

function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

const INDEX_NAME = 'sozo-360-v1';

const INDEX_SCHEMA = {
  name: INDEX_NAME,
  fields: [
    { name: 'id', type: 'Edm.String', key: true, filterable: true },
    { name: 'doc_type', type: 'Edm.String', filterable: true, facetable: true },
    { name: 'person_id', type: 'Edm.Int32', filterable: true, sortable: true },
    { name: 'display_name', type: 'Edm.String', searchable: true, filterable: true, sortable: true },
    { name: 'email', type: 'Edm.String', searchable: true, filterable: true },
    { name: 'location', type: 'Edm.String', searchable: true, filterable: true },
    { name: 'lifecycle_stage', type: 'Edm.String', filterable: true, facetable: true },
    { name: 'content', type: 'Edm.String', searchable: true },
    {
      name: 'content_vector',
      type: 'Collection(Edm.Single)',
      searchable: true,
      dimensions: 1536,
      vectorSearchProfile: 'sozo-vector-profile',
    },
    { name: 'tags_text', type: 'Edm.String', searchable: true },
    { name: 'giving_total', type: 'Edm.Double', filterable: true, sortable: true, facetable: true },
    { name: 'order_count', type: 'Edm.Int32', filterable: true, sortable: true },
    { name: 'event_count', type: 'Edm.Int32', filterable: true, sortable: true },
    { name: 'has_subscription', type: 'Edm.Boolean', filterable: true, facetable: true },
  ],
  vectorSearch: {
    algorithms: [{ name: 'hnsw-algo', kind: 'hnsw', hnswParameters: { m: 4, efConstruction: 400, efSearch: 500, metric: 'cosine' } }],
    profiles: [{ name: 'sozo-vector-profile', algorithm: 'hnsw-algo' }],
  },
};

async function main() {
  loadEnv();
  const deleteFirst = process.argv.includes('--delete-first');

  const serviceName = process.env.SOZO_SEARCH_SERVICE_NAME;
  const adminKey = process.env.SOZO_SEARCH_ADMIN_KEY;

  if (!serviceName || !adminKey) {
    console.error('Missing SOZO_SEARCH_SERVICE_NAME or SOZO_SEARCH_ADMIN_KEY');
    process.exit(1);
  }

  const baseUrl = `https://${serviceName}.search.windows.net`;
  const headers = { 'Content-Type': 'application/json', 'api-key': adminKey };
  const apiVersion = 'api-version=2024-07-01';

  // Delete existing index if requested
  if (deleteFirst) {
    console.log(`Deleting existing index '${INDEX_NAME}'...`);
    const delRes = await fetch(`${baseUrl}/indexes/${INDEX_NAME}?${apiVersion}`, { method: 'DELETE', headers });
    console.log(`  ${delRes.status} ${delRes.statusText}`);
  }

  // Check if index exists
  const checkRes = await fetch(`${baseUrl}/indexes/${INDEX_NAME}?${apiVersion}`, { headers });
  if (checkRes.ok) {
    const idx = await checkRes.json();
    console.log(`Index '${INDEX_NAME}' already exists with ${idx.fields?.length} fields.`);
    console.log('Run with --delete-first to recreate.');
    return;
  }

  // Create index
  console.log(`Creating index '${INDEX_NAME}'...`);
  const createRes = await fetch(`${baseUrl}/indexes?${apiVersion}`, {
    method: 'POST',
    headers,
    body: JSON.stringify(INDEX_SCHEMA),
  });

  if (!createRes.ok) {
    const err = await createRes.text();
    console.error(`FAILED (${createRes.status}): ${err}`);
    process.exit(1);
  }

  const created = await createRes.json();
  console.log(`Index '${created.name}' created with ${created.fields?.length} fields.`);

  // Verify
  const verifyRes = await fetch(`${baseUrl}/indexes/${INDEX_NAME}?${apiVersion}`, { headers });
  if (verifyRes.ok) {
    console.log('Verification: OK');
  }

  console.log('\nDone. Next: run build_rag_index.js to populate with person 360 documents.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
