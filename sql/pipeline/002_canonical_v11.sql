IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold_intel') EXEC('CREATE SCHEMA gold_intel');

IF OBJECT_ID('meta.source_to_canonical_rule', 'U') IS NULL
CREATE TABLE meta.source_to_canonical_rule (
  rule_id INT IDENTITY(1,1) PRIMARY KEY,
  source_system VARCHAR(64) NOT NULL,
  file_pattern VARCHAR(256) NOT NULL,
  canonical_entity VARCHAR(128) NOT NULL,
  rule_type VARCHAR(64) NOT NULL,
  rule_description VARCHAR(1000) NULL,
  is_active BIT NOT NULL DEFAULT 1,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.event', 'U') IS NULL
CREATE TABLE gold.event (
  event_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  event_name VARCHAR(256) NULL,
  event_date DATE NULL,
  event_start_time VARCHAR(32) NULL,
  location_name VARCHAR(256) NULL,
  location_address VARCHAR(256) NULL,
  location_city_state VARCHAR(128) NULL,
  venue_promoter VARCHAR(256) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.ticket_sale', 'U') IS NULL
CREATE TABLE gold.ticket_sale (
  ticket_sale_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  event_id UNIQUEIDENTIFIER NULL,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  tickets_purchased INT NULL,
  gross_amount DECIMAL(18,2) NULL,
  coupon_code VARCHAR(128) NULL,
  referral_code VARCHAR(128) NULL,
  purchased_at DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.pledge_commitment', 'U') IS NULL
CREATE TABLE gold.pledge_commitment (
  pledge_commitment_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  commitment_type VARCHAR(64) NULL,
  amount_committed DECIMAL(18,2) NULL,
  cadence VARCHAR(64) NULL,
  status VARCHAR(64) NULL,
  committed_at DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.refund_chargeback', 'U') IS NULL
CREATE TABLE gold.refund_chargeback (
  refund_chargeback_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  reason VARCHAR(256) NULL,
  amount DECIMAL(18,2) NULL,
  occurred_at DATETIME2 NULL,
  status VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.appeal', 'U') IS NULL
CREATE TABLE gold.appeal (
  appeal_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  appeal_name VARCHAR(256) NULL,
  appeal_type VARCHAR(64) NULL,
  campaign_name VARCHAR(256) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.designation', 'U') IS NULL
CREATE TABLE gold.designation (
  designation_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  designation_name VARCHAR(256) NULL,
  designation_type VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.support_case', 'U') IS NULL
CREATE TABLE gold.support_case (
  support_case_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  case_subject VARCHAR(256) NULL,
  case_status VARCHAR(64) NULL,
  opened_at DATETIME2 NULL,
  closed_at DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.subscription_contract', 'U') IS NULL
CREATE TABLE gold.subscription_contract (
  subscription_contract_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  plan_type VARCHAR(128) NULL,
  start_date DATE NULL,
  renewal_date DATE NULL,
  next_box_month VARCHAR(32) NULL,
  quantity INT NULL,
  is_gift BIT NULL,
  status VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.subscription_status_history', 'U') IS NULL
CREATE TABLE gold.subscription_status_history (
  subscription_status_history_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  subscription_contract_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  status VARCHAR(64) NOT NULL,
  status_reason VARCHAR(256) NULL,
  effective_date DATE NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.subscription_shipment', 'U') IS NULL
CREATE TABLE gold.subscription_shipment (
  subscription_shipment_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  subscription_contract_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  shipment_month VARCHAR(32) NULL,
  quantity INT NULL,
  shipstation_counter INT NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold_intel.feature_snapshot', 'U') IS NULL
CREATE TABLE gold_intel.feature_snapshot (
  feature_snapshot_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  canonical_type VARCHAR(32) NOT NULL,
  canonical_id UNIQUEIDENTIFIER NOT NULL,
  feature_name VARCHAR(128) NOT NULL,
  feature_value_float FLOAT NULL,
  feature_value_text VARCHAR(256) NULL,
  computed_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold_intel.model_score', 'U') IS NULL
CREATE TABLE gold_intel.model_score (
  model_score_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  canonical_type VARCHAR(32) NOT NULL,
  canonical_id UNIQUEIDENTIFIER NOT NULL,
  model_name VARCHAR(128) NOT NULL,
  model_version VARCHAR(32) NOT NULL,
  score FLOAT NOT NULL,
  score_band VARCHAR(32) NOT NULL,
  rationale VARCHAR(512) NULL,
  scored_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold_intel.microsegment_membership', 'U') IS NULL
CREATE TABLE gold_intel.microsegment_membership (
  microsegment_membership_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  canonical_type VARCHAR(32) NOT NULL,
  canonical_id UNIQUEIDENTIFIER NOT NULL,
  segment_code VARCHAR(64) NOT NULL,
  segment_name VARCHAR(128) NOT NULL,
  reason_codes VARCHAR(512) NULL,
  valid_from DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  valid_to DATETIME2 NULL
);

IF OBJECT_ID('gold_intel.sentiment_signal', 'U') IS NULL
CREATE TABLE gold_intel.sentiment_signal (
  sentiment_signal_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  canonical_type VARCHAR(32) NOT NULL,
  canonical_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  sentiment_label VARCHAR(32) NOT NULL,
  sentiment_score FLOAT NOT NULL,
  topic VARCHAR(128) NULL,
  model_name VARCHAR(64) NOT NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold_intel.next_best_action', 'U') IS NULL
CREATE TABLE gold_intel.next_best_action (
  next_best_action_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  snapshot_date DATE NOT NULL,
  canonical_type VARCHAR(32) NOT NULL,
  canonical_id UNIQUEIDENTIFIER NOT NULL,
  action_code VARCHAR(64) NOT NULL,
  action_channel VARCHAR(32) NOT NULL,
  expected_lift FLOAT NOT NULL,
  confidence FLOAT NOT NULL,
  rationale VARCHAR(512) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);
