const { withDb } = require('./_db');

const splitTags = (value) => String(value || '')
  .split(/[,;|]/)
  .map((x) => x.trim())
  .filter(Boolean);

const sqlSafe = (value) => String(value || '').replace(/'/g, "''");

async function loadTagMap(pool) {
  const rs = await pool.request().query(`
SELECT source_system, tag_value, tag_prefix, signal_group, confidence, needs_review
FROM meta.tag_signal_map
WHERE is_active = 1;
`);
  const map = new Map();
  for (const row of rs.recordset) {
    const key = `${String(row.source_system).toLowerCase()}|${String(row.tag_value).toLowerCase()}`;
    map.set(key, {
      tag_prefix: row.tag_prefix || 'Unscoped',
      signal_group: row.signal_group || 'unknown_unmapped',
      confidence: Number(row.confidence || 0.4),
      needs_review: row.needs_review ? 1 : 0,
    });
  }
  return map;
}

async function loadDonorDirectTags(pool, tagMap) {
  const rows = await pool.request().query(`
SELECT
  r.batch_id,
  r.source_system,
  r.source_record_id,
  r.file_path,
  r.record_json,
  cw.canonical_id AS person_id
FROM bronze.raw_record r
LEFT JOIN gold.crosswalk cw
  ON cw.canonical_type='person'
 AND cw.source_system=r.source_system
 AND cw.source_record_id=r.source_record_id
WHERE r.source_system='donor_direct'
  AND r.record_json LIKE '%"Tags"%';
`);

  const records = [];
  for (const row of rows.recordset) {
    let payload = null;
    try {
      payload = JSON.parse(row.record_json);
    } catch {
      payload = null;
    }
    if (!payload) continue;
    const tagsCell = payload.Tags || payload.tags || payload['Tag'];
    const tags = splitTags(tagsCell);
    if (!tags.length) continue;

    for (const tag of tags) {
      const key = `${String(row.source_system).toLowerCase()}|${String(tag).toLowerCase()}`;
      const mapped = tagMap.get(key) || {
        tag_prefix: tag.includes('->') ? tag.split('->')[0].trim() : 'Unscoped',
        signal_group: 'unknown_unmapped',
        confidence: 0.4,
        needs_review: 1,
      };

      records.push({
        batch_id: row.batch_id,
        person_id: row.person_id || null,
        source_system: row.source_system,
        source_record_id: row.source_record_id,
        file_path: row.file_path,
        tag_value: tag,
        tag_prefix: mapped.tag_prefix || 'Unscoped',
        signal_group: mapped.signal_group || 'unknown_unmapped',
        confidence: Number(mapped.confidence || 0.4),
        needs_review: mapped.needs_review ? 1 : 0,
      });
    }
  }

  await pool.request().batch(`
DELETE FROM silver.person_tag_signal
WHERE source_system='donor_direct';
`);

  const chunkSize = 200;
  for (let i = 0; i < records.length; i += chunkSize) {
    const chunk = records.slice(i, i + chunkSize);
    const values = chunk.map((r) => (
      `(${r.person_id ? `'${r.person_id}'` : 'NULL'},'${sqlSafe(r.source_system)}','${sqlSafe(r.source_record_id)}',N'${sqlSafe(r.tag_value)}','${sqlSafe(r.tag_prefix)}','${sqlSafe(r.signal_group)}',${r.confidence.toFixed(2)},${r.needs_review},'${r.batch_id}','${sqlSafe(r.file_path)}')`
    )).join(',\n');

    // eslint-disable-next-line no-await-in-loop
    await pool.request().batch(`
INSERT INTO silver.person_tag_signal(
  person_id,source_system,source_record_id,tag_value,tag_prefix,signal_group,confidence,needs_review,batch_id,file_path
)
VALUES
${values};
`);
  }

  return records.length;
}

async function rebuildSignalFacts(pool) {
  await pool.request().batch(`
DELETE FROM gold.signal_fact
WHERE signal_source IN ('person_tag_signal','payment_transaction','donation_transaction','ticket_sale','subscription_contract','engagement_activity');
`);

  await pool.request().batch(`
INSERT INTO gold.signal_fact(
  canonical_type,canonical_id,source_system,source_record_id,signal_source,signal_group,signal_name,signal_value_text,signal_value_number,signal_ts,confidence,batch_id,file_path
)
SELECT
  'person',
  pts.person_id,
  pts.source_system,
  pts.source_record_id,
  'person_tag_signal',
  pts.signal_group,
  'tag',
  pts.tag_value,
  NULL,
  NULL,
  pts.confidence,
  pts.batch_id,
  pts.file_path
FROM silver.person_tag_signal pts;
`);

  await pool.request().batch(`
INSERT INTO gold.signal_fact(
  canonical_type,canonical_id,source_system,source_record_id,signal_source,signal_group,signal_name,signal_value_text,signal_value_number,signal_ts,confidence,batch_id,file_path
)
SELECT
  'person',
  p.person_id,
  p.source_system,
  p.source_record_id,
  'payment_transaction',
  'payments_financial_events',
  COALESCE(p.status,'payment'),
  p.currency,
  TRY_CONVERT(decimal(18,4),p.amount),
  p.payment_ts,
  0.98,
  NULL,
  NULL
FROM gold.payment_transaction p;
`);

  await pool.request().batch(`
INSERT INTO gold.signal_fact(
  canonical_type,canonical_id,source_system,source_record_id,signal_source,signal_group,signal_name,signal_value_text,signal_value_number,signal_ts,confidence,batch_id,file_path
)
SELECT
  'person',
  d.person_id,
  d.source_system,
  d.source_record_id,
  'donation_transaction',
  'fundraising_giving',
  'donation',
  d.currency,
  TRY_CONVERT(decimal(18,4),d.amount),
  d.donation_ts,
  0.98,
  NULL,
  NULL
FROM gold.donation_transaction d;
`);

  await pool.request().batch(`
INSERT INTO gold.signal_fact(
  canonical_type,canonical_id,source_system,source_record_id,signal_source,signal_group,signal_name,signal_value_text,signal_value_number,signal_ts,confidence,batch_id,file_path
)
SELECT
  'person',
  t.person_id,
  t.source_system,
  t.source_record_id,
  'ticket_sale',
  'event_ticketing_attendance',
  'tickets_purchased',
  NULL,
  TRY_CONVERT(decimal(18,4),t.tickets_purchased),
  t.purchased_at,
  0.95,
  NULL,
  NULL
FROM gold.ticket_sale t;
`);

  await pool.request().batch(`
INSERT INTO gold.signal_fact(
  canonical_type,canonical_id,source_system,source_record_id,signal_source,signal_group,signal_name,signal_value_text,signal_value_number,signal_ts,confidence,batch_id,file_path
)
SELECT
  'person',
  s.person_id,
  s.source_system,
  s.source_record_id,
  'subscription_contract',
  'subscription_box_lifecycle',
  COALESCE(s.status,'subscription'),
  s.plan_type,
  TRY_CONVERT(decimal(18,4),s.quantity),
  TRY_CONVERT(datetime2,s.start_date),
  0.95,
  NULL,
  NULL
FROM gold.subscription_contract s;
`);

  await pool.request().batch(`
INSERT INTO gold.signal_fact(
  canonical_type,canonical_id,source_system,source_record_id,signal_source,signal_group,signal_name,signal_value_text,signal_value_number,signal_ts,confidence,batch_id,file_path
)
SELECT
  'person',
  e.person_id,
  e.source_system,
  e.source_record_id,
  'engagement_activity',
  'engagement_behavior',
  COALESCE(e.activity_type,'engagement'),
  e.subject,
  NULL,
  e.activity_ts,
  0.9,
  NULL,
  NULL
FROM gold.engagement_activity e;
`);
}

async function main() {
  await withDb(async (pool) => {
    const tagMap = await loadTagMap(pool);
    const loadedTagRows = await loadDonorDirectTags(pool, tagMap);
    await rebuildSignalFacts(pool);

    const counts = await pool.request().query(`
SELECT
  (SELECT COUNT(1) FROM silver.person_tag_signal) AS person_tag_signal_rows,
  (SELECT COUNT(1) FROM gold.signal_fact) AS signal_fact_rows,
  (SELECT COUNT(1) FROM silver.person_tag_signal WHERE needs_review=1) AS tag_review_rows;
`);

    console.log(JSON.stringify({
      loaded_tag_rows: loadedTagRows,
      person_tag_signal_rows: Number(counts.recordset[0].person_tag_signal_rows),
      signal_fact_rows: Number(counts.recordset[0].signal_fact_rows),
      tag_review_rows: Number(counts.recordset[0].tag_review_rows),
    }, null, 2));
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
