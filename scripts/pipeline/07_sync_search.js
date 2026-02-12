const { withDb, loadEnvFile } = require('./_db');

loadEnvFile();

const serviceName = process.env.SOZO_SEARCH_SERVICE_NAME;
const apiKey = process.env.SOZO_SEARCH_ADMIN_KEY;
const baseIndexName = process.env.SOZO_SEARCH_INDEX_NAME || 'sozo-insights';
let activeIndexName = baseIndexName;

if (!serviceName || !apiKey) {
  console.error('ERROR: SOZO_SEARCH_SERVICE_NAME and SOZO_SEARCH_ADMIN_KEY are required.');
  process.exit(1);
}

const api = (path, opts = {}) =>
  fetch(`https://${serviceName}.search.windows.net${path}`, {
    ...opts,
    headers: {
      'Content-Type': 'application/json',
      'api-key': apiKey,
      ...(opts.headers || {}),
    },
  });

async function ensureIndex(indexName) {
  const body = {
    name: indexName,
    fields: [
      { name: 'id', type: 'Edm.String', key: true, searchable: true, filterable: true, sortable: true },
      { name: 'docType', type: 'Edm.String', searchable: true, filterable: true, sortable: false },
      { name: 'title', type: 'Edm.String', searchable: true },
      { name: 'summary', type: 'Edm.String', searchable: true },
      { name: 'sourceSystem', type: 'Edm.String', searchable: true, filterable: true, sortable: false },
      { name: 'entityId', type: 'Edm.String', searchable: true, filterable: true, sortable: false },
      { name: 'updatedAt', type: 'Edm.DateTimeOffset', searchable: false, filterable: true, sortable: true },
    ],
    semantic: {
      configurations: [
        {
          name: 'default',
          prioritizedFields: {
            titleField: { fieldName: 'title' },
            prioritizedContentFields: [{ fieldName: 'summary' }],
          },
        },
      ],
    },
  };

  const res = await api(`/indexes/${encodeURIComponent(indexName)}?api-version=2024-07-01`, {
    method: 'PUT',
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const text = await res.text();
    throw new Error(`Index ensure failed: ${res.status} ${text}`);
  }
}

async function loadDocs() {
  return withDb(async (pool) => {
    const persons = await pool.request().query(`
SELECT
  CAST(person_id AS VARCHAR(64)) AS entity_id,
  display_name,
  primary_email,
  primary_phone,
  updated_at
FROM gold.person
ORDER BY updated_at DESC;
`);

    const households = await pool.request().query(`
SELECT
  CAST(household_id AS VARCHAR(64)) AS entity_id,
  household_name,
  household_status,
  updated_at
FROM gold.household
ORDER BY updated_at DESC;
`);

    const personDocs = persons.recordset.map((r) => ({
      '@search.action': 'mergeOrUpload',
      id: `person-${r.entity_id}`,
      docType: 'person',
      title: r.display_name || 'Unknown Person',
      summary: `Email: ${r.primary_email || 'n/a'} | Phone: ${r.primary_phone || 'n/a'}`,
      sourceSystem: 'canonical',
      entityId: r.entity_id,
      updatedAt: new Date(r.updated_at).toISOString(),
    }));

    const householdDocs = households.recordset.map((r) => ({
      '@search.action': 'mergeOrUpload',
      id: `household-${r.entity_id}`,
      docType: 'household',
      title: r.household_name || 'Unknown Household',
      summary: `Status: ${r.household_status || 'unknown'}`,
      sourceSystem: 'canonical',
      entityId: r.entity_id,
      updatedAt: new Date(r.updated_at).toISOString(),
    }));

    return [...personDocs, ...householdDocs];
  });
}

async function pushDocs(indexName, docs) {
  const chunkSize = 500;
  let total = 0;

  for (let i = 0; i < docs.length; i += chunkSize) {
    const chunk = docs.slice(i, i + chunkSize);
    // eslint-disable-next-line no-await-in-loop
    const res = await api(`/indexes/${encodeURIComponent(indexName)}/docs/index?api-version=2024-07-01`, {
      method: 'POST',
      body: JSON.stringify({ value: chunk }),
    });

    if (!res.ok) {
      const text = await res.text();
      throw new Error(`Document sync failed: ${res.status} ${text}`);
    }

    // eslint-disable-next-line no-await-in-loop
    const payload = await res.json();
    total += payload.value?.length || 0;
  }

  return total;
}

async function main() {
  try {
    await ensureIndex(activeIndexName);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    if (message.includes('CannotChangeExistingField') || message.includes('Existing field')) {
      activeIndexName = `${baseIndexName}-v2`;
      await ensureIndex(activeIndexName);
    } else {
      throw error;
    }
  }
  const docs = await loadDocs();
  if (docs.length === 0) {
    console.log(`OK: index ${activeIndexName} exists, no docs to sync yet.`);
    return;
  }
  const count = await pushDocs(activeIndexName, docs);
  console.log(`OK: synced ${count} person/household insight docs to index ${activeIndexName}.`);
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
