/**
 * Create Azure AI Search index for Sozo conversation memory.
 * Index: sozo-memory-v1 — stores embedded conversation summaries for semantic retrieval.
 *
 * Usage: node scripts/pipeline/create_memory_search_index.js [--delete-first]
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

const INDEX_NAME = 'sozo-memory-v1';

const INDEX_SCHEMA = {
  name: INDEX_NAME,
  fields: [
    { name: 'id', type: 'Edm.String', key: true, filterable: true },
    { name: 'doc_type', type: 'Edm.String', filterable: true, facetable: true },
    { name: 'owner_email', type: 'Edm.String', filterable: true },
    { name: 'conversation_id', type: 'Edm.String', filterable: true },
    { name: 'title', type: 'Edm.String', searchable: true },
    { name: 'content', type: 'Edm.String', searchable: true },
    {
      name: 'content_vector',
      type: 'Collection(Edm.Single)',
      searchable: true,
      dimensions: 1536,
      vectorSearchProfile: 'memory-vector-profile',
    },
    { name: 'topics', type: 'Edm.String', searchable: true },
    { name: 'category', type: 'Edm.String', filterable: true, facetable: true },
    { name: 'confidence', type: 'Edm.Double', filterable: true, sortable: true },
    { name: 'created_at', type: 'Edm.DateTimeOffset', filterable: true, sortable: true },
  ],
  vectorSearch: {
    algorithms: [{
      name: 'hnsw-memory-algo',
      kind: 'hnsw',
      hnswParameters: { m: 4, efConstruction: 400, efSearch: 500, metric: 'cosine' },
    }],
    profiles: [{ name: 'memory-vector-profile', algorithm: 'hnsw-memory-algo' }],
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

  if (deleteFirst) {
    console.log(`Deleting existing index '${INDEX_NAME}'...`);
    const delRes = await fetch(`${baseUrl}/indexes/${INDEX_NAME}?${apiVersion}`, { method: 'DELETE', headers });
    console.log(`  ${delRes.status} ${delRes.statusText}`);
  }

  const checkRes = await fetch(`${baseUrl}/indexes/${INDEX_NAME}?${apiVersion}`, { headers });
  if (checkRes.ok) {
    const idx = await checkRes.json();
    console.log(`Index '${INDEX_NAME}' already exists with ${idx.fields?.length} fields.`);
    console.log('Run with --delete-first to recreate.');
    return;
  }

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

  const verifyRes = await fetch(`${baseUrl}/indexes/${INDEX_NAME}?${apiVersion}`, { headers });
  if (verifyRes.ok) {
    console.log('Verification: OK');
  }

  console.log('\nDone. Memory search index created.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
