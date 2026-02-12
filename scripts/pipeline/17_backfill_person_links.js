const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    const updates = [];

    const payment = await pool.request().query(`
;WITH candidates AS (
  SELECT
    pt.payment_transaction_id,
    cw.canonical_id AS person_id
  FROM gold.payment_transaction pt
  LEFT JOIN silver.transaction_source st
    ON st.source_system = pt.source_system
   AND st.source_record_id = pt.source_record_id
  LEFT JOIN gold.crosswalk cw
    ON cw.canonical_type = 'person'
   AND cw.source_system = st.source_system
   AND cw.source_record_id = st.person_ref
  WHERE pt.person_id IS NULL
    AND cw.canonical_id IS NOT NULL
)
UPDATE pt
SET pt.person_id = c.person_id
FROM gold.payment_transaction pt
JOIN candidates c ON c.payment_transaction_id = pt.payment_transaction_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.payment_transaction', updated_rows: Number(payment.recordset[0].updated_rows) });

    const engagement = await pool.request().query(`
;WITH candidates AS (
  SELECT
    ga.engagement_activity_id,
    cw.canonical_id AS person_id
  FROM gold.engagement_activity ga
  LEFT JOIN silver.engagement_source se
    ON se.source_system = ga.source_system
   AND se.source_record_id = ga.source_record_id
  LEFT JOIN gold.crosswalk cw
    ON cw.canonical_type = 'person'
   AND cw.source_system = se.source_system
   AND cw.source_record_id = se.person_ref
  WHERE ga.person_id IS NULL
    AND cw.canonical_id IS NOT NULL
)
UPDATE ga
SET ga.person_id = c.person_id
FROM gold.engagement_activity ga
JOIN candidates c ON c.engagement_activity_id = ga.engagement_activity_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.engagement_activity', updated_rows: Number(engagement.recordset[0].updated_rows) });

    const ticket = await pool.request().query(`
;WITH candidates AS (
  SELECT
    ts.ticket_sale_id,
    cw.canonical_id AS person_id
  FROM gold.ticket_sale ts
  LEFT JOIN gold.crosswalk cw
    ON cw.canonical_type = 'person'
   AND cw.source_system = ts.source_system
   AND cw.source_record_id = REPLACE(ts.source_record_id, ':ticket', '')
  WHERE ts.person_id IS NULL
    AND cw.canonical_id IS NOT NULL
)
UPDATE ts
SET ts.person_id = c.person_id
FROM gold.ticket_sale ts
JOIN candidates c ON c.ticket_sale_id = ts.ticket_sale_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.ticket_sale', updated_rows: Number(ticket.recordset[0].updated_rows) });

    const subscription = await pool.request().query(`
;WITH candidates AS (
  SELECT
    sc.subscription_contract_id,
    cw.canonical_id AS person_id
  FROM gold.subscription_contract sc
  LEFT JOIN gold.crosswalk cw
    ON cw.canonical_type = 'person'
   AND cw.source_system = sc.source_system
   AND cw.source_record_id = REPLACE(sc.source_record_id, ':sub', '')
  WHERE sc.person_id IS NULL
    AND cw.canonical_id IS NOT NULL
)
UPDATE sc
SET sc.person_id = c.person_id
FROM gold.subscription_contract sc
JOIN candidates c ON c.subscription_contract_id = sc.subscription_contract_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.subscription_contract', updated_rows: Number(subscription.recordset[0].updated_rows) });

    const pledge = await pool.request().query(`
;WITH candidates AS (
  SELECT
    pc.pledge_commitment_id,
    cw.canonical_id AS person_id
  FROM gold.pledge_commitment pc
  LEFT JOIN gold.crosswalk cw
    ON cw.canonical_type = 'person'
   AND cw.source_system = pc.source_system
   AND cw.source_record_id = REPLACE(pc.source_record_id, ':pledge', '')
  WHERE pc.person_id IS NULL
    AND cw.canonical_id IS NOT NULL
)
UPDATE pc
SET pc.person_id = c.person_id
FROM gold.pledge_commitment pc
JOIN candidates c ON c.pledge_commitment_id = pc.pledge_commitment_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.pledge_commitment', updated_rows: Number(pledge.recordset[0].updated_rows) });

    // Fallback strategy: derive email from bronze JSON payload and map by gold.contact_point(email).
    const paymentByEmail = await pool.request().query(`
;WITH candidates AS (
  SELECT
    pt.payment_transaction_id,
    cp.person_id
  FROM gold.payment_transaction pt
  JOIN bronze.raw_record br
    ON br.source_system = pt.source_system
   AND br.source_record_id = pt.source_record_id
  CROSS APPLY (
    SELECT LOWER(LTRIM(RTRIM(COALESCE(
      JSON_VALUE(br.record_json,'$.Email'),
      JSON_VALUE(br.record_json,'$.email'),
      JSON_VALUE(br.record_json,'$."Email Address"'),
      JSON_VALUE(br.record_json,'$."EmailAddress"'),
      JSON_VALUE(br.record_json,'$.ContactEmail'),
      JSON_VALUE(br.record_json,'$.CustomerEmail'),
      JSON_VALUE(br.record_json,'$.customer_email')
    )))) AS email_key
  ) em
  JOIN gold.contact_point cp
    ON cp.contact_type = 'email'
   AND LOWER(cp.contact_value) = em.email_key
  WHERE pt.person_id IS NULL
    AND em.email_key IS NOT NULL
    AND em.email_key <> ''
)
UPDATE pt
SET pt.person_id = c.person_id
FROM gold.payment_transaction pt
JOIN candidates c ON c.payment_transaction_id = pt.payment_transaction_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.payment_transaction(email_fallback)', updated_rows: Number(paymentByEmail.recordset[0].updated_rows) });

    const engagementByEmail = await pool.request().query(`
;WITH candidates AS (
  SELECT
    ga.engagement_activity_id,
    cp.person_id
  FROM gold.engagement_activity ga
  JOIN bronze.raw_record br
    ON br.source_system = ga.source_system
   AND br.source_record_id = ga.source_record_id
  CROSS APPLY (
    SELECT LOWER(LTRIM(RTRIM(COALESCE(
      JSON_VALUE(br.record_json,'$.Email'),
      JSON_VALUE(br.record_json,'$.email'),
      JSON_VALUE(br.record_json,'$."Email Address"'),
      JSON_VALUE(br.record_json,'$."EmailAddress"'),
      JSON_VALUE(br.record_json,'$.ContactEmail'),
      JSON_VALUE(br.record_json,'$.CustomerEmail'),
      JSON_VALUE(br.record_json,'$.customer_email')
    )))) AS email_key
  ) em
  JOIN gold.contact_point cp
    ON cp.contact_type = 'email'
   AND LOWER(cp.contact_value) = em.email_key
  WHERE ga.person_id IS NULL
    AND em.email_key IS NOT NULL
    AND em.email_key <> ''
)
UPDATE ga
SET ga.person_id = c.person_id
FROM gold.engagement_activity ga
JOIN candidates c ON c.engagement_activity_id = ga.engagement_activity_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.engagement_activity(email_fallback)', updated_rows: Number(engagementByEmail.recordset[0].updated_rows) });

    const ticketByEmail = await pool.request().query(`
;WITH base AS (
  SELECT
    ts.ticket_sale_id,
    ts.source_system,
    REPLACE(ts.source_record_id, ':ticket', '') AS base_source_record_id
  FROM gold.ticket_sale ts
  WHERE ts.person_id IS NULL
),
candidates AS (
  SELECT
    b.ticket_sale_id,
    cp.person_id
  FROM base b
  JOIN bronze.raw_record br
    ON br.source_system = b.source_system
   AND br.source_record_id = b.base_source_record_id
  CROSS APPLY (
    SELECT LOWER(LTRIM(RTRIM(COALESCE(
      JSON_VALUE(br.record_json,'$.Email'),
      JSON_VALUE(br.record_json,'$.email'),
      JSON_VALUE(br.record_json,'$."Email Address"'),
      JSON_VALUE(br.record_json,'$."EmailAddress"'),
      JSON_VALUE(br.record_json,'$.ContactEmail'),
      JSON_VALUE(br.record_json,'$.CustomerEmail'),
      JSON_VALUE(br.record_json,'$.customer_email')
    )))) AS email_key
  ) em
  JOIN gold.contact_point cp
    ON cp.contact_type = 'email'
   AND LOWER(cp.contact_value) = em.email_key
  WHERE em.email_key IS NOT NULL
    AND em.email_key <> ''
)
UPDATE ts
SET ts.person_id = c.person_id
FROM gold.ticket_sale ts
JOIN candidates c ON c.ticket_sale_id = ts.ticket_sale_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.ticket_sale(email_fallback)', updated_rows: Number(ticketByEmail.recordset[0].updated_rows) });

    const subscriptionByEmail = await pool.request().query(`
;WITH base AS (
  SELECT
    sc.subscription_contract_id,
    sc.source_system,
    REPLACE(sc.source_record_id, ':sub', '') AS base_source_record_id
  FROM gold.subscription_contract sc
  WHERE sc.person_id IS NULL
),
candidates AS (
  SELECT
    b.subscription_contract_id,
    cp.person_id
  FROM base b
  JOIN bronze.raw_record br
    ON br.source_system = b.source_system
   AND br.source_record_id = b.base_source_record_id
  CROSS APPLY (
    SELECT LOWER(LTRIM(RTRIM(COALESCE(
      JSON_VALUE(br.record_json,'$.Email'),
      JSON_VALUE(br.record_json,'$.email'),
      JSON_VALUE(br.record_json,'$."Email Address"'),
      JSON_VALUE(br.record_json,'$."EmailAddress"'),
      JSON_VALUE(br.record_json,'$.ContactEmail'),
      JSON_VALUE(br.record_json,'$.CustomerEmail'),
      JSON_VALUE(br.record_json,'$.customer_email')
    )))) AS email_key
  ) em
  JOIN gold.contact_point cp
    ON cp.contact_type = 'email'
   AND LOWER(cp.contact_value) = em.email_key
  WHERE em.email_key IS NOT NULL
    AND em.email_key <> ''
)
UPDATE sc
SET sc.person_id = c.person_id
FROM gold.subscription_contract sc
JOIN candidates c ON c.subscription_contract_id = sc.subscription_contract_id;

SELECT @@ROWCOUNT AS updated_rows;
`);
    updates.push({ table: 'gold.subscription_contract(email_fallback)', updated_rows: Number(subscriptionByEmail.recordset[0].updated_rows) });

    const summary = await pool.request().query(`
SELECT
  (SELECT COUNT(1) FROM gold.payment_transaction WHERE person_id IS NULL) AS payment_null_person,
  (SELECT COUNT(1) FROM gold.engagement_activity WHERE person_id IS NULL) AS engagement_null_person,
  (SELECT COUNT(1) FROM gold.ticket_sale WHERE person_id IS NULL) AS ticket_null_person,
  (SELECT COUNT(1) FROM gold.subscription_contract WHERE person_id IS NULL) AS subscription_null_person,
  (SELECT COUNT(1) FROM gold.pledge_commitment WHERE person_id IS NULL) AS pledge_null_person;
`);

    console.log(JSON.stringify({
      updates,
      remaining_null_person: summary.recordset[0],
    }, null, 2));
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
