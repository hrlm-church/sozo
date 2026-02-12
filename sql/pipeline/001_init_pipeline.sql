IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'meta') EXEC('CREATE SCHEMA meta');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'bronze') EXEC('CREATE SCHEMA bronze');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'silver') EXEC('CREATE SCHEMA silver');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold') EXEC('CREATE SCHEMA gold');

IF OBJECT_ID('meta.source_system', 'U') IS NULL
CREATE TABLE meta.source_system (
  source_system VARCHAR(64) NOT NULL PRIMARY KEY,
  display_name VARCHAR(128) NOT NULL,
  is_active BIT NOT NULL DEFAULT 1,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('meta.ingestion_mapping', 'U') IS NULL
CREATE TABLE meta.ingestion_mapping (
  mapping_id INT IDENTITY(1,1) PRIMARY KEY,
  source_system VARCHAR(64) NOT NULL,
  entity_name VARCHAR(128) NOT NULL,
  file_pattern VARCHAR(256) NOT NULL,
  parser_name VARCHAR(64) NOT NULL,
  target_table VARCHAR(128) NOT NULL,
  notes VARCHAR(512) NULL,
  is_active BIT NOT NULL DEFAULT 1,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('meta.source_file_lineage', 'U') IS NULL
CREATE TABLE meta.source_file_lineage (
  lineage_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  batch_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  file_hash VARCHAR(64) NOT NULL,
  ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  row_count INT NOT NULL DEFAULT 0,
  status VARCHAR(32) NOT NULL,
  error_message VARCHAR(1000) NULL
);

IF OBJECT_ID('bronze.raw_record', 'U') IS NULL
CREATE TABLE bronze.raw_record (
  raw_record_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  batch_id UNIQUEIDENTIFIER NOT NULL,
  lineage_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  row_number INT NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  record_hash VARCHAR(64) NOT NULL,
  record_json NVARCHAR(MAX) NOT NULL,
  ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_raw_record_source_file' AND object_id = OBJECT_ID('bronze.raw_record'))
CREATE INDEX IX_raw_record_source_file ON bronze.raw_record(source_system, file_path, row_number);

IF OBJECT_ID('silver.person_source', 'U') IS NULL
CREATE TABLE silver.person_source (
  silver_person_source_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  batch_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  full_name VARCHAR(256) NULL,
  email VARCHAR(256) NULL,
  phone VARCHAR(64) NULL,
  address_line1 VARCHAR(256) NULL,
  city VARCHAR(128) NULL,
  state VARCHAR(64) NULL,
  postal_code VARCHAR(32) NULL,
  quality_flags NVARCHAR(1000) NULL,
  ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('silver.transaction_source', 'U') IS NULL
CREATE TABLE silver.transaction_source (
  silver_transaction_source_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  batch_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  person_ref VARCHAR(200) NULL,
  transaction_ref VARCHAR(200) NULL,
  amount DECIMAL(18,2) NULL,
  currency VARCHAR(16) NULL,
  status VARCHAR(64) NULL,
  transaction_ts DATETIME2 NULL,
  quality_flags NVARCHAR(1000) NULL,
  ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('silver.engagement_source', 'U') IS NULL
CREATE TABLE silver.engagement_source (
  silver_engagement_source_id BIGINT IDENTITY(1,1) PRIMARY KEY,
  batch_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  person_ref VARCHAR(200) NULL,
  engagement_type VARCHAR(128) NULL,
  subject VARCHAR(256) NULL,
  occurred_at DATETIME2 NULL,
  notes NVARCHAR(2000) NULL,
  quality_flags NVARCHAR(1000) NULL,
  ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.person', 'U') IS NULL
CREATE TABLE gold.person (
  person_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  display_name VARCHAR(256) NULL,
  primary_email VARCHAR(256) NULL,
  primary_phone VARCHAR(64) NULL,
  confidence_score DECIMAL(5,2) NOT NULL DEFAULT 0,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.household', 'U') IS NULL
CREATE TABLE gold.household (
  household_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  household_name VARCHAR(256) NULL,
  household_status VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.organization', 'U') IS NULL
CREATE TABLE gold.organization (
  organization_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  organization_name VARCHAR(256) NULL,
  organization_type VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
  updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.address', 'U') IS NULL
CREATE TABLE gold.address (
  address_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  line1 VARCHAR(256) NULL,
  line2 VARCHAR(256) NULL,
  city VARCHAR(128) NULL,
  [state] VARCHAR(64) NULL,
  postal_code VARCHAR(32) NULL,
  country VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.contact_point', 'U') IS NULL
CREATE TABLE gold.contact_point (
  contact_point_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  contact_type VARCHAR(32) NOT NULL,
  contact_value VARCHAR(256) NOT NULL,
  is_primary BIT NOT NULL DEFAULT 0,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.identity_resolution', 'U') IS NULL
CREATE TABLE gold.identity_resolution (
  identity_resolution_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  match_method VARCHAR(64) NOT NULL,
  confidence_score DECIMAL(5,2) NOT NULL,
  possible_match BIT NOT NULL DEFAULT 0,
  resolved_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.relationship', 'U') IS NULL
CREATE TABLE gold.relationship (
  relationship_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  left_entity_type VARCHAR(32) NOT NULL,
  left_entity_id UNIQUEIDENTIFIER NOT NULL,
  right_entity_type VARCHAR(32) NOT NULL,
  right_entity_id UNIQUEIDENTIFIER NOT NULL,
  relationship_type VARCHAR(64) NOT NULL,
  confidence_score DECIMAL(5,2) NOT NULL DEFAULT 0,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.donation_transaction', 'U') IS NULL
CREATE TABLE gold.donation_transaction (
  donation_transaction_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  amount DECIMAL(18,2) NULL,
  currency VARCHAR(16) NULL,
  donation_ts DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.payment_transaction', 'U') IS NULL
CREATE TABLE gold.payment_transaction (
  payment_transaction_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  amount DECIMAL(18,2) NULL,
  currency VARCHAR(16) NULL,
  payment_ts DATETIME2 NULL,
  status VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.recurring_plan', 'U') IS NULL
CREATE TABLE gold.recurring_plan (
  recurring_plan_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  amount DECIMAL(18,2) NULL,
  cadence VARCHAR(64) NULL,
  status VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.invoice', 'U') IS NULL
CREATE TABLE gold.invoice (
  invoice_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  invoice_number VARCHAR(128) NULL,
  total_amount DECIMAL(18,2) NULL,
  invoice_ts DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.invoice_line', 'U') IS NULL
CREATE TABLE gold.invoice_line (
  invoice_line_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  invoice_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  line_description VARCHAR(512) NULL,
  amount DECIMAL(18,2) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.[order]', 'U') IS NULL
CREATE TABLE gold.[order] (
  order_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  order_number VARCHAR(128) NULL,
  order_ts DATETIME2 NULL,
  total_amount DECIMAL(18,2) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.order_line', 'U') IS NULL
CREATE TABLE gold.order_line (
  order_line_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  order_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  line_description VARCHAR(512) NULL,
  amount DECIMAL(18,2) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.campaign', 'U') IS NULL
CREATE TABLE gold.campaign (
  campaign_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  campaign_name VARCHAR(256) NULL,
  campaign_type VARCHAR(64) NULL,
  start_ts DATETIME2 NULL,
  end_ts DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.fund', 'U') IS NULL
CREATE TABLE gold.fund (
  fund_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  fund_name VARCHAR(256) NULL,
  fund_category VARCHAR(64) NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.engagement_activity', 'U') IS NULL
CREATE TABLE gold.engagement_activity (
  engagement_activity_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  activity_type VARCHAR(128) NULL,
  subject VARCHAR(256) NULL,
  activity_ts DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.consent_preferences', 'U') IS NULL
CREATE TABLE gold.consent_preferences (
  consent_preferences_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  channel VARCHAR(64) NULL,
  consent_status VARCHAR(64) NULL,
  effective_ts DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.communication_touch', 'U') IS NULL
CREATE TABLE gold.communication_touch (
  communication_touch_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  person_id UNIQUEIDENTIFIER NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  channel VARCHAR(64) NULL,
  touch_direction VARCHAR(32) NULL,
  touch_ts DATETIME2 NULL,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.crosswalk', 'U') IS NULL
CREATE TABLE gold.crosswalk (
  crosswalk_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  canonical_type VARCHAR(32) NOT NULL,
  canonical_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  source_record_id VARCHAR(200) NOT NULL,
  match_method VARCHAR(64) NOT NULL,
  match_confidence DECIMAL(5,2) NOT NULL,
  possible_match BIT NOT NULL DEFAULT 0,
  created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
);

IF OBJECT_ID('gold.source_file_lineage', 'U') IS NULL
CREATE TABLE gold.source_file_lineage (
  source_file_lineage_id UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
  lineage_id UNIQUEIDENTIFIER NOT NULL,
  batch_id UNIQUEIDENTIFIER NOT NULL,
  source_system VARCHAR(64) NOT NULL,
  file_path VARCHAR(512) NOT NULL,
  file_hash VARCHAR(64) NOT NULL,
  row_count INT NOT NULL,
  status VARCHAR(32) NOT NULL,
  ingested_at DATETIME2 NOT NULL
);
