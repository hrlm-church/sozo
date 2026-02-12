const { withDb } = require('./_db');

const normalize = (v) => (v ? String(v).trim().toLowerCase() : null);
const emailDomain = (email) => {
  const e = normalize(email);
  if (!e || !e.includes('@')) return null;
  return e.split('@')[1];
};

async function ensureHousehold(pool, name) {
  const found = await pool.request().input('name', name).query('SELECT TOP (1) household_id FROM gold.household WHERE household_name=@name');
  if (found.recordset.length) return found.recordset[0].household_id;

  const created = await pool.request().input('name', name).batch(`
DECLARE @id UNIQUEIDENTIFIER = NEWID();
INSERT INTO gold.household(household_id, household_name, household_status) VALUES(@id, @name, 'active');
SELECT @id AS household_id;
`);
  return created.recordset[0].household_id;
}

async function ensurePerson(pool, personRow) {
  const email = normalize(personRow.email);
  const phone = normalize(personRow.phone);
  let personId = null;
  let method = 'source_record';
  let confidence = 0.8;

  if (email) {
    const match = await pool.request().input('email', email).query(`
SELECT TOP (1) person_id FROM gold.contact_point
WHERE contact_type='email' AND LOWER(contact_value)=@email;
`);
    if (match.recordset.length) {
      personId = match.recordset[0].person_id;
      method = 'deterministic_email';
      confidence = 0.99;
    }
  }

  if (!personId && phone) {
    const match = await pool.request().input('phone', phone).query(`
SELECT TOP (1) person_id FROM gold.contact_point
WHERE contact_type='phone' AND LOWER(contact_value)=@phone;
`);
    if (match.recordset.length) {
      personId = match.recordset[0].person_id;
      method = 'deterministic_phone';
      confidence = 0.95;
    }
  }

  if (!personId) {
    const created = await pool.request()
      .input('name', personRow.full_name || 'Unknown Person')
      .input('email', personRow.email || null)
      .input('phone', personRow.phone || null)
      .batch(`
DECLARE @id UNIQUEIDENTIFIER = NEWID();
INSERT INTO gold.person(person_id, display_name, primary_email, primary_phone, confidence_score)
VALUES(@id, @name, @email, @phone, 0.80);
SELECT @id AS person_id;
`);
    personId = created.recordset[0].person_id;
  }

  if (email) {
    await pool.request().input('person_id', personId).input('value', email).batch(`
IF NOT EXISTS (SELECT 1 FROM gold.contact_point WHERE person_id=@person_id AND contact_type='email' AND LOWER(contact_value)=LOWER(@value))
INSERT INTO gold.contact_point(person_id, contact_type, contact_value, is_primary)
VALUES(@person_id, 'email', @value, 1);
`);
  }

  if (phone) {
    await pool.request().input('person_id', personId).input('value', phone).batch(`
IF NOT EXISTS (SELECT 1 FROM gold.contact_point WHERE person_id=@person_id AND contact_type='phone' AND LOWER(contact_value)=LOWER(@value))
INSERT INTO gold.contact_point(person_id, contact_type, contact_value, is_primary)
VALUES(@person_id, 'phone', @value, 0);
`);
  }

  return { personId, method, confidence };
}

async function main() {
  await withDb(async (pool) => {
    const sourcePeople = await pool.request().query(`
SELECT DISTINCT
  source_system, source_record_id, full_name, email, phone
FROM silver.person_source
ORDER BY source_system, source_record_id;
`);

    let linked = 0;
    for (const row of sourcePeople.recordset) {
      // eslint-disable-next-line no-await-in-loop
      const resolved = await ensurePerson(pool, row);

      const possibleMatch = !row.email && !!row.full_name ? 1 : 0;
      const confidence = possibleMatch ? 0.6 : resolved.confidence;

      // eslint-disable-next-line no-await-in-loop
      await pool.request()
        .input('person_id', resolved.personId)
        .input('source_system', row.source_system)
        .input('source_record_id', row.source_record_id)
        .input('match_method', resolved.method)
        .input('confidence_score', confidence)
        .input('possible_match', possibleMatch)
        .batch(`
IF NOT EXISTS (SELECT 1 FROM gold.crosswalk WHERE canonical_type='person' AND source_system=@source_system AND source_record_id=@source_record_id)
BEGIN
  INSERT INTO gold.crosswalk(canonical_type, canonical_id, source_system, source_record_id, match_method, match_confidence, possible_match)
  VALUES('person', @person_id, @source_system, @source_record_id, @match_method, @confidence_score, @possible_match);

  INSERT INTO gold.identity_resolution(person_id, source_system, source_record_id, match_method, confidence_score, possible_match)
  VALUES(@person_id, @source_system, @source_record_id, @match_method, @confidence_score, @possible_match);
END
`);

      const hhName = emailDomain(row.email) ? `${emailDomain(row.email)} household` : `${row.source_system} household`;
      // eslint-disable-next-line no-await-in-loop
      const householdId = await ensureHousehold(pool, hhName);

      // eslint-disable-next-line no-await-in-loop
      await pool.request()
        .input('person_id', resolved.personId)
        .input('household_id', householdId)
        .batch(`
IF NOT EXISTS (
  SELECT 1 FROM gold.relationship
  WHERE left_entity_type='person' AND left_entity_id=@person_id
    AND right_entity_type='household' AND right_entity_id=@household_id
    AND relationship_type='member_of'
)
INSERT INTO gold.relationship(left_entity_type,left_entity_id,right_entity_type,right_entity_id,relationship_type,confidence_score)
VALUES('person',@person_id,'household',@household_id,'member_of',0.85);
`);

      linked += 1;
    }

    await pool.request().batch(`
INSERT INTO gold.source_file_lineage(lineage_id,batch_id,source_system,file_path,file_hash,row_count,status,ingested_at)
SELECT l.lineage_id,l.batch_id,l.source_system,l.file_path,l.file_hash,l.row_count,l.status,l.ingested_at
FROM meta.source_file_lineage l
LEFT JOIN gold.source_file_lineage g ON g.lineage_id = l.lineage_id
WHERE g.lineage_id IS NULL;
`);

    await pool.request().batch(`
INSERT INTO gold.payment_transaction(person_id,source_system,source_record_id,amount,currency,payment_ts,status)
SELECT
  cw.canonical_id,
  t.source_system,
  t.source_record_id,
  t.amount,
  COALESCE(t.currency,'USD'),
  t.transaction_ts,
  t.status
FROM silver.transaction_source t
LEFT JOIN gold.crosswalk cw
  ON cw.canonical_type='person'
 AND cw.source_system=t.source_system
 AND cw.source_record_id=t.person_ref
WHERE NOT EXISTS (
  SELECT 1 FROM gold.payment_transaction p
  WHERE p.source_system=t.source_system AND p.source_record_id=t.source_record_id
);
`);

    await pool.request().batch(`
INSERT INTO gold.engagement_activity(person_id,source_system,source_record_id,activity_type,subject,activity_ts)
SELECT
  cw.canonical_id,
  e.source_system,
  e.source_record_id,
  e.engagement_type,
  e.subject,
  e.occurred_at
FROM silver.engagement_source e
LEFT JOIN gold.crosswalk cw
  ON cw.canonical_type='person'
 AND cw.source_system=e.source_system
 AND cw.source_record_id=e.person_ref
WHERE NOT EXISTS (
  SELECT 1 FROM gold.engagement_activity g
  WHERE g.source_system=e.source_system AND g.source_record_id=e.source_record_id
);
`);

    const counts = await pool.request().query(`
SELECT
 (SELECT COUNT(1) FROM gold.person) AS person_count,
 (SELECT COUNT(1) FROM gold.household) AS household_count,
 (SELECT COUNT(1) FROM gold.crosswalk) AS crosswalk_count,
 (SELECT COUNT(1) FROM gold.identity_resolution) AS identity_count;
`);

    const c = counts.recordset[0];
    console.log(`OK: gold canonical build complete (linked=${linked}, person=${c.person_count}, household=${c.household_count}, crosswalk=${c.crosswalk_count}, identity=${c.identity_count})`);
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
