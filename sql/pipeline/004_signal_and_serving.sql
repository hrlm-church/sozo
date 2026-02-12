IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'serving') EXEC('CREATE SCHEMA serving');

IF OBJECT_ID('silver.person_tag_signal', 'U') IS NULL
CREATE TABLE silver.person_tag_signal (
  person_tag_signal_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  tag_value NVARCHAR(512) NOT NULL,
  tag_prefix VARCHAR(128) NULL,
  signal_group VARCHAR(64) NOT NULL,
  confidence DECIMAL(5,2) NOT NULL DEFAULT 0,
  needs_review BIT NOT NULL DEFAULT 0,
  batch_id UNIQUEIDENTIFIER NULL,
  file_path VARCHAR(512) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'UX_person_tag_signal_source_key'
    AND object_id = OBJECT_ID('silver.person_tag_signal')
)
CREATE UNIQUE INDEX UX_person_tag_signal_source_key
ON silver.person_tag_signal(source_system, source_record_id, tag_value);

IF OBJECT_ID('gold.signal_fact', 'U') IS NULL
CREATE TABLE gold.signal_fact (
  signal_fact_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  canonical_type VARCHAR(32) NOT NULL,
  canonical_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  signal_source VARCHAR(64) NOT NULL,
  signal_group VARCHAR(64) NOT NULL,
  signal_name VARCHAR(128) NOT NULL,
  signal_value_text NVARCHAR(512) NULL,
  signal_value_number DECIMAL(18,4) NULL,
  signal_ts DATETIME2 NULL,
  confidence DECIMAL(5,2) NULL,
  batch_id UNIQUEIDENTIFIER NULL,
  file_path VARCHAR(512) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'IX_signal_fact_canonical_group'
    AND object_id = OBJECT_ID('gold.signal_fact')
)
CREATE INDEX IX_signal_fact_canonical_group
ON gold.signal_fact(canonical_type, canonical_id, signal_group);

IF NOT EXISTS (
  SELECT 1
  FROM sys.indexes
  WHERE name = 'IX_signal_fact_source_key'
    AND object_id = OBJECT_ID('gold.signal_fact')
)
CREATE INDEX IX_signal_fact_source_key
ON gold.signal_fact(source_system, source_record_id, signal_source);

EXEC('
CREATE OR ALTER VIEW gold.person_360 AS
WITH pay AS (
  SELECT person_id, COUNT(1) AS payment_count, SUM(COALESCE(amount,0)) AS payment_amount, MAX(payment_ts) AS last_payment_ts
  FROM gold.payment_transaction
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
don AS (
  SELECT person_id, COUNT(1) AS donation_count, SUM(COALESCE(amount,0)) AS donation_amount, MAX(donation_ts) AS last_donation_ts
  FROM gold.donation_transaction
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
tix AS (
  SELECT person_id, COUNT(1) AS ticket_sale_count, SUM(COALESCE(tickets_purchased,0)) AS tickets_total
  FROM gold.ticket_sale
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
subx AS (
  SELECT person_id,
         COUNT(1) AS subscription_count,
         SUM(CASE WHEN status = ''active'' THEN 1 ELSE 0 END) AS active_subscription_count
  FROM gold.subscription_contract
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
eng AS (
  SELECT person_id, COUNT(1) AS engagement_count, MAX(activity_ts) AS last_engagement_ts
  FROM gold.engagement_activity
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
tags AS (
  SELECT person_id,
         COUNT(1) AS tag_count,
         SUM(CASE WHEN needs_review = 1 THEN 1 ELSE 0 END) AS tag_review_count
  FROM silver.person_tag_signal
  WHERE person_id IS NOT NULL
  GROUP BY person_id
),
hh AS (
  SELECT r.left_entity_id AS person_id, r.right_entity_id AS household_id
  FROM gold.relationship r
  WHERE r.left_entity_type = ''person''
    AND r.right_entity_type = ''household''
    AND r.relationship_type = ''member_of''
)
SELECT
  p.person_id,
  p.display_name,
  p.primary_email,
  p.primary_phone,
  hh.household_id,
  h.household_name,
  COALESCE(pay.payment_count,0) AS payment_count,
  COALESCE(pay.payment_amount,0) AS payment_amount,
  pay.last_payment_ts,
  COALESCE(don.donation_count,0) AS donation_count,
  COALESCE(don.donation_amount,0) AS donation_amount,
  don.last_donation_ts,
  COALESCE(tix.ticket_sale_count,0) AS ticket_sale_count,
  COALESCE(tix.tickets_total,0) AS tickets_total,
  COALESCE(subx.subscription_count,0) AS subscription_count,
  COALESCE(subx.active_subscription_count,0) AS active_subscription_count,
  COALESCE(eng.engagement_count,0) AS engagement_count,
  eng.last_engagement_ts,
  COALESCE(tags.tag_count,0) AS tag_count,
  COALESCE(tags.tag_review_count,0) AS tag_review_count,
  p.created_at,
  p.updated_at
FROM gold.person p
LEFT JOIN hh ON hh.person_id = p.person_id
LEFT JOIN gold.household h ON h.household_id = hh.household_id
LEFT JOIN pay ON pay.person_id = p.person_id
LEFT JOIN don ON don.person_id = p.person_id
LEFT JOIN tix ON tix.person_id = p.person_id
LEFT JOIN subx ON subx.person_id = p.person_id
LEFT JOIN eng ON eng.person_id = p.person_id
LEFT JOIN tags ON tags.person_id = p.person_id;
');

EXEC('
CREATE OR ALTER VIEW gold.household_360 AS
WITH members AS (
  SELECT r.right_entity_id AS household_id, r.left_entity_id AS person_id
  FROM gold.relationship r
  WHERE r.left_entity_type = ''person''
    AND r.right_entity_type = ''household''
    AND r.relationship_type = ''member_of''
),
pay AS (
  SELECT m.household_id,
         COUNT(1) AS payment_count,
         SUM(COALESCE(pt.amount,0)) AS payment_amount,
         MAX(pt.payment_ts) AS last_payment_ts
  FROM members m
  JOIN gold.payment_transaction pt ON pt.person_id = m.person_id
  GROUP BY m.household_id
),
subs AS (
  SELECT m.household_id,
         COUNT(1) AS subscription_count,
         SUM(CASE WHEN sc.status = ''active'' THEN 1 ELSE 0 END) AS active_subscription_count
  FROM members m
  JOIN gold.subscription_contract sc ON sc.person_id = m.person_id
  GROUP BY m.household_id
)
SELECT
  h.household_id,
  h.household_name,
  h.household_status,
  COUNT(DISTINCT m.person_id) AS member_count,
  COALESCE(pay.payment_count,0) AS payment_count,
  COALESCE(pay.payment_amount,0) AS payment_amount,
  pay.last_payment_ts,
  COALESCE(subs.subscription_count,0) AS subscription_count,
  COALESCE(subs.active_subscription_count,0) AS active_subscription_count,
  h.created_at,
  h.updated_at
FROM gold.household h
LEFT JOIN members m ON m.household_id = h.household_id
LEFT JOIN pay ON pay.household_id = h.household_id
LEFT JOIN subs ON subs.household_id = h.household_id
GROUP BY
  h.household_id,
  h.household_name,
  h.household_status,
  pay.payment_count,
  pay.payment_amount,
  pay.last_payment_ts,
  subs.subscription_count,
  subs.active_subscription_count,
  h.created_at,
  h.updated_at;
');

EXEC('
CREATE OR ALTER VIEW gold.organization_360 AS
WITH rel AS (
  SELECT r.right_entity_id AS organization_id, r.left_entity_id AS person_id
  FROM gold.relationship r
  WHERE r.left_entity_type = ''person'' AND r.right_entity_type = ''organization''
)
SELECT
  o.organization_id,
  o.organization_name,
  o.organization_type,
  COUNT(DISTINCT rel.person_id) AS linked_people_count,
  o.created_at,
  o.updated_at
FROM gold.organization o
LEFT JOIN rel ON rel.organization_id = o.organization_id
GROUP BY
  o.organization_id,
  o.organization_name,
  o.organization_type,
  o.created_at,
  o.updated_at;
');

EXEC('
CREATE OR ALTER VIEW serving.v_person_overview AS
SELECT * FROM gold.person_360;
');

EXEC('
CREATE OR ALTER VIEW serving.v_household_overview AS
SELECT * FROM gold.household_360;
');

EXEC('
CREATE OR ALTER VIEW serving.v_organization_overview AS
SELECT * FROM gold.organization_360;
');

EXEC('
CREATE OR ALTER VIEW serving.v_signal_explorer AS
SELECT
  sf.canonical_type,
  sf.canonical_id,
  sf.source_system,
  sf.source_record_id,
  sf.signal_source,
  sf.signal_group,
  sf.signal_name,
  sf.signal_value_text,
  sf.signal_value_number,
  sf.signal_ts,
  sf.confidence,
  sf.batch_id,
  sf.file_path,
  sf.created_at
FROM gold.signal_fact sf;
');
