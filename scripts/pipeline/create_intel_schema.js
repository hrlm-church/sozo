/**
 * Create intelligence layer schema for Sozo.
 * Tables: intel.metric_definition, intel.metric_synonym, intel.dimension_definition,
 *         intel.metric_dimension_allowlist, intel.metric_snapshot, intel.insight,
 *         intel.insight_entity_link, intel.query_plan, intel.query_plan_example_utterance,
 *         intel.semantic_policy, intel.user_preference, intel.person_score
 *
 * Usage: node scripts/pipeline/create_intel_schema.js
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

async function main() {
  loadEnv();
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 60000,
  });

  console.log('Creating intel schema for Sozo intelligence layer...\n');

  // ── Schema ─────────────────────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'intel')
      EXEC('CREATE SCHEMA intel')
  `);
  console.log('  intel schema OK');

  // ── 1. metric_definition ───────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'metric_definition'
    )
    CREATE TABLE intel.metric_definition (
      metric_id              INT IDENTITY(1,1) PRIMARY KEY,
      metric_key             NVARCHAR(128)  NOT NULL UNIQUE,
      display_name           NVARCHAR(256)  NOT NULL,
      description            NVARCHAR(2000) NULL,
      metric_type            NVARCHAR(32)   NOT NULL DEFAULT 'aggregate',
      unit                   NVARCHAR(32)   NOT NULL DEFAULT 'count',
      format_hint            NVARCHAR(64)   NULL,
      grain                  NVARCHAR(64)   NOT NULL,
      default_time_window    NVARCHAR(64)   NULL,
      sql_expression         NVARCHAR(MAX)  NOT NULL,
      depends_on_metric_keys NVARCHAR(2000) NULL,
      is_certified           BIT            NOT NULL DEFAULT 1,
      is_active              BIT            NOT NULL DEFAULT 1,
      owner_team             NVARCHAR(128)  NULL,
      created_at             DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      updated_at             DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    )
  `);
  console.log('  intel.metric_definition OK');

  // ── 2. metric_synonym ─────────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'metric_synonym'
    )
    BEGIN
      CREATE TABLE intel.metric_synonym (
        metric_synonym_id  INT IDENTITY(1,1) PRIMARY KEY,
        metric_key         NVARCHAR(128) NOT NULL,
        synonym            NVARCHAR(256) NOT NULL,
        weight             INT           NOT NULL DEFAULT 100,
        is_active          BIT           NOT NULL DEFAULT 1,
        created_at         DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_ms_metric FOREIGN KEY (metric_key)
          REFERENCES intel.metric_definition(metric_key)
      );
      CREATE INDEX IX_metric_synonym_syn ON intel.metric_synonym(synonym)
        INCLUDE(metric_key, weight, is_active);
    END
  `);
  console.log('  intel.metric_synonym OK');

  // ── 3. dimension_definition ────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'dimension_definition'
    )
    CREATE TABLE intel.dimension_definition (
      dimension_id           INT IDENTITY(1,1) PRIMARY KEY,
      dimension_key          NVARCHAR(128)  NOT NULL UNIQUE,
      display_name           NVARCHAR(256)  NOT NULL,
      description            NVARCHAR(2000) NULL,
      source_table           NVARCHAR(256)  NOT NULL,
      source_column          NVARCHAR(256)  NULL,
      data_type              NVARCHAR(64)   NOT NULL,
      is_time_dimension      BIT            NOT NULL DEFAULT 0,
      allowed_values_json    NVARCHAR(MAX)  NULL,
      allowed_operators_json NVARCHAR(MAX)  NOT NULL,
      canonicalization_rule  NVARCHAR(2000) NULL,
      is_active              BIT            NOT NULL DEFAULT 1,
      created_at             DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      updated_at             DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    )
  `);
  console.log('  intel.dimension_definition OK');

  // ── 4. metric_dimension_allowlist ──────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'metric_dimension_allowlist'
    )
    CREATE TABLE intel.metric_dimension_allowlist (
      metric_key     NVARCHAR(128) NOT NULL,
      dimension_key  NVARCHAR(128) NOT NULL,
      allow_group_by BIT           NOT NULL DEFAULT 1,
      allow_filter   BIT           NOT NULL DEFAULT 1,
      notes          NVARCHAR(512) NULL,
      PRIMARY KEY (metric_key, dimension_key),
      CONSTRAINT FK_mda_metric FOREIGN KEY (metric_key)
        REFERENCES intel.metric_definition(metric_key),
      CONSTRAINT FK_mda_dim FOREIGN KEY (dimension_key)
        REFERENCES intel.dimension_definition(dimension_key)
    )
  `);
  console.log('  intel.metric_dimension_allowlist OK');

  // ── 5. metric_snapshot ─────────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'metric_snapshot'
    )
    BEGIN
      CREATE TABLE intel.metric_snapshot (
        snapshot_id           BIGINT IDENTITY(1,1) PRIMARY KEY,
        metric_key            NVARCHAR(128) NOT NULL,
        as_of_date            DATE          NOT NULL,
        start_date            DATE          NULL,
        end_date              DATE          NULL,
        segment_key           NVARCHAR(256) NULL,
        value_decimal         DECIMAL(18,4) NULL,
        value_int             BIGINT        NULL,
        value_text            NVARCHAR(4000) NULL,
        prior_value_decimal   DECIMAL(18,4) NULL,
        delta_pct             DECIMAL(10,4) NULL,
        computed_at           DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_ms_snap_metric FOREIGN KEY (metric_key)
          REFERENCES intel.metric_definition(metric_key)
      );
      CREATE INDEX IX_metric_snapshot_lookup
        ON intel.metric_snapshot(metric_key, as_of_date)
        INCLUDE(value_decimal, value_int, segment_key);
    END
  `);
  console.log('  intel.metric_snapshot OK');

  // ── 6. insight ─────────────────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'insight'
    )
    BEGIN
      CREATE TABLE intel.insight (
        insight_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
        insight_type       NVARCHAR(64)   NOT NULL,
        severity           INT            NOT NULL DEFAULT 3,
        title              NVARCHAR(300)  NOT NULL,
        summary            NVARCHAR(2000) NOT NULL,
        detail_markdown    NVARCHAR(MAX)  NULL,
        metric_key         NVARCHAR(128)  NULL,
        segment_key        NVARCHAR(256)  NULL,
        as_of_date         DATE           NOT NULL,
        current_value      DECIMAL(18,4)  NULL,
        baseline_value     DECIMAL(18,4)  NULL,
        delta_pct          FLOAT          NULL,
        evidence_json      NVARCHAR(MAX)  NULL,
        actions_json       NVARCHAR(MAX)  NULL,
        status             NVARCHAR(32)   NOT NULL DEFAULT 'open',
        is_active          BIT            NOT NULL DEFAULT 1,
        created_at         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at         DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
      );
      CREATE INDEX IX_insight_date ON intel.insight(as_of_date, insight_type)
        INCLUDE(severity, status, is_active);
    END
  `);
  console.log('  intel.insight OK');

  // ── 7. insight_entity_link ─────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'insight_entity_link'
    )
    BEGIN
      CREATE TABLE intel.insight_entity_link (
        link_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
        insight_id    BIGINT        NOT NULL,
        entity_type   NVARCHAR(64)  NOT NULL,
        entity_id     NVARCHAR(128) NOT NULL,
        role          NVARCHAR(64)  NULL,
        metadata_json NVARCHAR(MAX) NULL,
        created_at    DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT FK_iel_insight FOREIGN KEY (insight_id)
          REFERENCES intel.insight(insight_id) ON DELETE CASCADE
      );
      CREATE INDEX IX_iel_entity ON intel.insight_entity_link(entity_type, entity_id);
    END
  `);
  console.log('  intel.insight_entity_link OK');

  // ── 8. query_plan ──────────────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'query_plan'
    )
    CREATE TABLE intel.query_plan (
      plan_id         BIGINT IDENTITY(1,1) PRIMARY KEY,
      plan_key        NVARCHAR(128)  NOT NULL UNIQUE,
      title           NVARCHAR(200)  NOT NULL,
      description     NVARCHAR(1000) NULL,
      plan_json       NVARCHAR(MAX)  NOT NULL,
      compiled_sql    NVARCHAR(MAX)  NULL,
      widget_json     NVARCHAR(MAX)  NULL,
      is_certified    BIT            NOT NULL DEFAULT 0,
      usage_count     INT            NOT NULL DEFAULT 0,
      success_count   INT            NOT NULL DEFAULT 0,
      created_by      NVARCHAR(128)  NULL,
      created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      updated_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    )
  `);
  console.log('  intel.query_plan OK');

  // ── 9. query_plan_example_utterance ────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'query_plan_example_utterance'
    )
    CREATE TABLE intel.query_plan_example_utterance (
      example_id  BIGINT IDENTITY(1,1) PRIMARY KEY,
      plan_key    NVARCHAR(128)  NOT NULL,
      utterance   NVARCHAR(500)  NOT NULL,
      created_at  DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      CONSTRAINT FK_qpeu_plan FOREIGN KEY (plan_key)
        REFERENCES intel.query_plan(plan_key)
    )
  `);
  console.log('  intel.query_plan_example_utterance OK');

  // ── 10. semantic_policy ────────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'semantic_policy'
    )
    CREATE TABLE intel.semantic_policy (
      policy_id    INT IDENTITY(1,1) PRIMARY KEY,
      policy_key   NVARCHAR(128)  NOT NULL UNIQUE,
      description  NVARCHAR(2000) NULL,
      policy_json  NVARCHAR(MAX)  NOT NULL,
      is_active    BIT            NOT NULL DEFAULT 1,
      created_at   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      updated_at   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    )
  `);
  console.log('  intel.semantic_policy OK');

  // ── 11. user_preference ────────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'user_preference'
    )
    CREATE TABLE intel.user_preference (
      user_email      NVARCHAR(256) NOT NULL PRIMARY KEY,
      preference_json NVARCHAR(MAX) NOT NULL DEFAULT '{}',
      updated_at      DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
    )
  `);
  console.log('  intel.user_preference OK');

  // ── 12. person_score ───────────────────────────────────────────────────
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'intel' AND t.name = 'person_score'
    )
    BEGIN
      CREATE TABLE intel.person_score (
        score_id       BIGINT IDENTITY(1,1) PRIMARY KEY,
        person_id      INT           NOT NULL,
        score_type     NVARCHAR(64)  NOT NULL,
        score_value    DECIMAL(10,4) NOT NULL,
        score_label    NVARCHAR(64)  NULL,
        model_version  NVARCHAR(64)  NULL,
        as_of_date     DATE          NOT NULL,
        drivers_json   NVARCHAR(MAX) NULL,
        created_at     DATETIME2     NOT NULL DEFAULT SYSUTCDATETIME()
      );
      CREATE INDEX IX_person_score_lookup
        ON intel.person_score(score_type, as_of_date)
        INCLUDE(person_id, score_value, score_label);
      CREATE INDEX IX_person_score_person
        ON intel.person_score(person_id, score_type);
    END
  `);
  console.log('  intel.person_score OK');

  console.log('\nIntel schema creation complete.');
  await pool.close();
}

main().catch(err => { console.error(err); process.exit(1); });
