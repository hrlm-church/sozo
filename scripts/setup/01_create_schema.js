/**
 * Step 1.1 — Create Fresh Database Schema
 *
 * Drops ALL existing schemas/tables and creates the clean Sozo domain model.
 * Run: node scripts/setup/01_create_schema.js
 */

const fs = require('fs');
const path = require('path');
const sql = require('mssql');

// ── env ─────────────────────────────────────────────────────────────────────
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(p)) { console.error('ERROR: .env.local not found'); process.exit(1); }
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

function getConfig() {
  return {
    server: process.env.SOZO_SQL_HOST,
    database: process.env.SOZO_SQL_DB,
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 300000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 5, min: 0, idleTimeoutMillis: 5000 },
  };
}

async function exec(pool, text) {
  await pool.request().batch(text);
}

// ── schema DDL ──────────────────────────────────────────────────────────────
async function main() {
  loadEnv();
  console.log('Step 1.1 — Create Fresh Database Schema');
  console.log('='.repeat(60));

  const pool = await sql.connect(getConfig());

  try {
    // ── Drop all tables in target schemas ──
    console.log('\n[1] Dropping existing tables...');
    const schemas = [
      'serving', 'intel', 'engagement', 'event', 'commerce', 'giving',
      'household', 'person', 'raw', 'staging', 'meta',
      // Old pipeline schemas
      'gold_intel', 'gold', 'silver', 'bronze',
    ];

    for (const s of schemas) {
      const res = await pool.request().query(`
        SELECT t.name
        FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = '${s}'
      `);
      for (const row of res.recordset) {
        await exec(pool, `DROP TABLE IF EXISTS [${s}].[${row.name}]`);
      }
    }
    // Also drop views in serving/old schemas
    for (const s of ['serving', 'gold', 'gold_intel']) {
      const vres = await pool.request().query(`
        SELECT v.name
        FROM sys.views v JOIN sys.schemas s ON v.schema_id = s.schema_id
        WHERE s.name = '${s}'
      `);
      for (const row of vres.recordset) {
        await exec(pool, `DROP VIEW IF EXISTS [${s}].[${row.name}]`);
      }
    }
    console.log('  Tables dropped.');

    // ── Create schemas ──
    console.log('\n[2] Creating schemas...');
    const newSchemas = [
      'meta', 'raw', 'staging', 'person', 'household',
      'giving', 'commerce', 'event', 'engagement', 'intel', 'serving',
    ];
    for (const s of newSchemas) {
      await exec(pool, `IF SCHEMA_ID('${s}') IS NULL EXEC('CREATE SCHEMA [${s}]')`);
    }
    console.log('  Schemas: ' + newSchemas.join(', '));

    // ── META ──
    console.log('\n[3] Creating meta tables...');

    await exec(pool, `
      CREATE TABLE meta.source_system (
        source_id   INT IDENTITY(1,1) PRIMARY KEY,
        name        VARCHAR(64) NOT NULL UNIQUE,
        display_name NVARCHAR(128),
        is_active   BIT NOT NULL DEFAULT 1
      )
    `);

    await exec(pool, `
      CREATE TABLE meta.file_lineage (
        lineage_id  UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        batch_id    UNIQUEIDENTIFIER NOT NULL,
        source_id   INT NOT NULL REFERENCES meta.source_system(source_id),
        blob_path   NVARCHAR(512) NOT NULL,
        file_hash   VARCHAR(64) NOT NULL,
        row_count   INT NOT NULL DEFAULT 0,
        status      VARCHAR(32) NOT NULL DEFAULT 'pending',
        ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `
      CREATE INDEX IX_file_lineage_source ON meta.file_lineage(source_id, blob_path)
    `);

    // ── RAW ──
    console.log('[4] Creating raw tables...');

    await exec(pool, `
      CREATE TABLE raw.record (
        id          BIGINT IDENTITY(1,1) PRIMARY KEY,
        lineage_id  UNIQUEIDENTIFIER NOT NULL,
        source_id   INT NOT NULL,
        row_num     INT NOT NULL,
        record_hash VARCHAR(64) NOT NULL,
        data        NVARCHAR(MAX) NOT NULL,
        ingested_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `
      CREATE INDEX IX_raw_record_source ON raw.record(source_id, lineage_id)
    `);

    // ── STAGING (temp for identity resolution) ──
    console.log('[5] Creating staging tables...');

    await exec(pool, `
      CREATE TABLE staging.person_extract (
        id            BIGINT IDENTITY(1,1) PRIMARY KEY,
        source_id     INT NOT NULL,
        source_ref    VARCHAR(256) NOT NULL,
        blob_path     NVARCHAR(512),
        first_name    NVARCHAR(128),
        last_name     NVARCHAR(128),
        display_name  NVARCHAR(256),
        email         VARCHAR(256),
        email2        VARCHAR(256),
        email3        VARCHAR(256),
        phone         VARCHAR(32),
        phone2        VARCHAR(32),
        phone3        VARCHAR(32),
        address_line1 NVARCHAR(256),
        address_line2 NVARCHAR(256),
        city          NVARCHAR(128),
        state         VARCHAR(64),
        zip           VARCHAR(20),
        country       VARCHAR(64),
        company       NVARCHAR(256),
        raw_record_id BIGINT,
        resolved_person_id UNIQUEIDENTIFIER NULL
      )
    `);
    await exec(pool, `
      CREATE INDEX IX_staging_person_email ON staging.person_extract(email) WHERE email IS NOT NULL
    `);
    await exec(pool, `
      CREATE INDEX IX_staging_person_phone ON staging.person_extract(phone) WHERE phone IS NOT NULL
    `);
    await exec(pool, `
      CREATE INDEX IX_staging_person_source ON staging.person_extract(source_id, source_ref)
    `);

    // ── PERSON ──
    console.log('[6] Creating person tables...');

    await exec(pool, `
      CREATE TABLE person.profile (
        id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        display_name  NVARCHAR(256),
        first_name    NVARCHAR(128),
        last_name     NVARCHAR(128),
        confidence    DECIMAL(5,2) NOT NULL DEFAULT 0.80,
        created_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);

    await exec(pool, `
      CREATE TABLE person.email (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id   UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        email       VARCHAR(256) NOT NULL,
        is_primary  BIT NOT NULL DEFAULT 0,
        source_id   INT,
        created_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_person_email UNIQUE (email)
      )
    `);
    await exec(pool, `CREATE INDEX IX_person_email_pid ON person.email(person_id)`);

    await exec(pool, `
      CREATE TABLE person.phone (
        id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id       UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        phone_normalized VARCHAR(20) NOT NULL,
        phone_display   VARCHAR(32),
        is_primary      BIT NOT NULL DEFAULT 0,
        source_id       INT,
        created_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_person_phone UNIQUE (phone_normalized)
      )
    `);
    await exec(pool, `CREATE INDEX IX_person_phone_pid ON person.phone(person_id)`);

    await exec(pool, `
      CREATE TABLE person.address (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id   UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        line1       NVARCHAR(256),
        line2       NVARCHAR(256),
        city        NVARCHAR(128),
        state       VARCHAR(64),
        zip         VARCHAR(20),
        country     VARCHAR(64) DEFAULT 'US',
        is_primary  BIT NOT NULL DEFAULT 0,
        source_id   INT,
        created_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_person_address_pid ON person.address(person_id)`);

    await exec(pool, `
      CREATE TABLE person.source_link (
        id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id       UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        source_id       INT NOT NULL REFERENCES meta.source_system(source_id),
        source_record_id VARCHAR(256) NOT NULL,
        match_method    VARCHAR(64) NOT NULL DEFAULT 'source_record',
        confidence      DECIMAL(5,2) NOT NULL DEFAULT 0.80,
        linked_at       DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_source_link UNIQUE (source_id, source_record_id)
      )
    `);
    await exec(pool, `CREATE INDEX IX_source_link_pid ON person.source_link(person_id)`);

    // ── HOUSEHOLD ──
    console.log('[7] Creating household tables...');

    await exec(pool, `
      CREATE TABLE household.unit (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        name        NVARCHAR(256),
        status      VARCHAR(32) NOT NULL DEFAULT 'active',
        created_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);

    await exec(pool, `
      CREATE TABLE household.member (
        id           UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        household_id UNIQUEIDENTIFIER NOT NULL REFERENCES household.unit(id),
        person_id    UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        role         VARCHAR(32) DEFAULT 'member',
        joined_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_household_member UNIQUE (household_id, person_id)
      )
    `);
    await exec(pool, `CREATE INDEX IX_hh_member_pid ON household.member(person_id)`);

    // ── GIVING ──
    console.log('[8] Creating giving tables...');

    await exec(pool, `
      CREATE TABLE giving.donation (
        id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id       UNIQUEIDENTIFIER REFERENCES person.profile(id),
        amount          DECIMAL(12,2),
        currency        VARCHAR(3) DEFAULT 'USD',
        donated_at      DATETIME2,
        source_id       INT REFERENCES meta.source_system(source_id),
        source_ref      VARCHAR(256),
        payment_method  VARCHAR(64),
        fund            NVARCHAR(256),
        appeal          NVARCHAR(256),
        designation     NVARCHAR(256),
        created_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_donation_pid ON giving.donation(person_id)`);
    await exec(pool, `CREATE INDEX IX_donation_date ON giving.donation(donated_at)`);

    await exec(pool, `
      CREATE TABLE giving.recurring_plan (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id   UNIQUEIDENTIFIER REFERENCES person.profile(id),
        amount      DECIMAL(12,2),
        cadence     VARCHAR(32),
        status      VARCHAR(32),
        start_date  DATE,
        end_date    DATE,
        source_id   INT REFERENCES meta.source_system(source_id),
        source_ref  VARCHAR(256),
        created_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);

    await exec(pool, `
      CREATE TABLE giving.pledge (
        id                UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id         UNIQUEIDENTIFIER REFERENCES person.profile(id),
        pledged_amount    DECIMAL(12,2),
        fulfilled_amount  DECIMAL(12,2) DEFAULT 0,
        pledge_date       DATE,
        due_date          DATE,
        status            VARCHAR(32),
        source_id         INT REFERENCES meta.source_system(source_id),
        created_at        DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);

    // ── COMMERCE ──
    console.log('[9] Creating commerce tables...');

    await exec(pool, `
      CREATE TABLE commerce.[order] (
        id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id     UNIQUEIDENTIFIER REFERENCES person.profile(id),
        order_number  VARCHAR(64),
        total_amount  DECIMAL(12,2),
        order_date    DATETIME2,
        status        VARCHAR(32),
        source_id     INT REFERENCES meta.source_system(source_id),
        source_ref    VARCHAR(256),
        created_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_order_pid ON commerce.[order](person_id)`);

    await exec(pool, `
      CREATE TABLE commerce.order_line (
        id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        order_id      UNIQUEIDENTIFIER NOT NULL REFERENCES commerce.[order](id),
        product_name  NVARCHAR(256),
        quantity      INT DEFAULT 1,
        unit_price    DECIMAL(12,2),
        total         DECIMAL(12,2),
        created_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);

    await exec(pool, `
      CREATE TABLE commerce.subscription (
        id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id     UNIQUEIDENTIFIER REFERENCES person.profile(id),
        product_name  NVARCHAR(256),
        amount        DECIMAL(12,2),
        cadence       VARCHAR(32),
        status        VARCHAR(32),
        start_date    DATE,
        next_renewal  DATE,
        is_gift       BIT DEFAULT 0,
        source_id     INT REFERENCES meta.source_system(source_id),
        source_ref    VARCHAR(256),
        created_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_subscription_pid ON commerce.subscription(person_id)`);

    await exec(pool, `
      CREATE TABLE commerce.invoice (
        id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id       UNIQUEIDENTIFIER REFERENCES person.profile(id),
        invoice_number  VARCHAR(64),
        total           DECIMAL(12,2),
        status          VARCHAR(32),
        issued_at       DATETIME2,
        paid_at         DATETIME2,
        source_id       INT REFERENCES meta.source_system(source_id),
        source_ref      VARCHAR(256),
        created_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_invoice_pid ON commerce.invoice(person_id)`);

    await exec(pool, `
      CREATE TABLE commerce.payment (
        id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id       UNIQUEIDENTIFIER REFERENCES person.profile(id),
        amount          DECIMAL(12,2),
        payment_date    DATETIME2,
        method          VARCHAR(64),
        status          VARCHAR(32),
        source_id       INT REFERENCES meta.source_system(source_id),
        source_ref      VARCHAR(256),
        donation_id     UNIQUEIDENTIFIER REFERENCES giving.donation(id),
        order_id        UNIQUEIDENTIFIER REFERENCES commerce.[order](id),
        invoice_id      UNIQUEIDENTIFIER REFERENCES commerce.invoice(id),
        created_at      DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_payment_pid ON commerce.payment(person_id)`);

    // ── EVENT ──
    console.log('[10] Creating event tables...');

    await exec(pool, `
      CREATE TABLE event.event (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        name        NVARCHAR(256),
        event_date  DATE,
        location    NVARCHAR(256),
        venue       NVARCHAR(256),
        event_type  VARCHAR(64),
        created_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);

    await exec(pool, `
      CREATE TABLE event.ticket (
        id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        event_id      UNIQUEIDENTIFIER REFERENCES event.event(id),
        person_id     UNIQUEIDENTIFIER REFERENCES person.profile(id),
        tickets_qty   INT DEFAULT 1,
        amount        DECIMAL(12,2),
        coupon_code   VARCHAR(64),
        purchased_at  DATETIME2,
        source_id     INT REFERENCES meta.source_system(source_id),
        source_ref    VARCHAR(256),
        created_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_ticket_pid ON event.ticket(person_id)`);
    await exec(pool, `CREATE INDEX IX_ticket_event ON event.ticket(event_id)`);

    // ── ENGAGEMENT ──
    console.log('[11] Creating engagement tables...');

    await exec(pool, `
      CREATE TABLE engagement.activity (
        id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id     UNIQUEIDENTIFIER REFERENCES person.profile(id),
        activity_type VARCHAR(64),
        subject       NVARCHAR(512),
        body          NVARCHAR(MAX),
        occurred_at   DATETIME2,
        source_id     INT REFERENCES meta.source_system(source_id),
        created_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_activity_pid ON engagement.activity(person_id)`);

    await exec(pool, `
      CREATE TABLE engagement.communication (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id   UNIQUEIDENTIFIER REFERENCES person.profile(id),
        channel     VARCHAR(32),
        direction   VARCHAR(16),
        subject     NVARCHAR(512),
        sent_at     DATETIME2,
        source_id   INT REFERENCES meta.source_system(source_id),
        created_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_comm_pid ON engagement.communication(person_id)`);

    await exec(pool, `
      CREATE TABLE engagement.note (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id   UNIQUEIDENTIFIER REFERENCES person.profile(id),
        note_text   NVARCHAR(MAX),
        author      NVARCHAR(128),
        created_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        source_id   INT REFERENCES meta.source_system(source_id)
      )
    `);
    await exec(pool, `CREATE INDEX IX_note_pid ON engagement.note(person_id)`);

    await exec(pool, `
      CREATE TABLE engagement.tag (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id   UNIQUEIDENTIFIER REFERENCES person.profile(id),
        tag_value   NVARCHAR(512) NOT NULL,
        tag_group   VARCHAR(64),
        source_id   INT REFERENCES meta.source_system(source_id),
        applied_at  DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_tag_pid ON engagement.tag(person_id)`);
    await exec(pool, `CREATE INDEX IX_tag_group ON engagement.tag(tag_group)`);

    // ── INTEL ──
    console.log('[12] Creating intel tables...');

    await exec(pool, `
      CREATE TABLE intel.segment (
        id            INT IDENTITY(1,1) PRIMARY KEY,
        name          VARCHAR(128) NOT NULL,
        description   NVARCHAR(512),
        segment_type  VARCHAR(64) NOT NULL,
        criteria_json NVARCHAR(MAX),
        created_at    DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);

    await exec(pool, `
      CREATE TABLE intel.segment_member (
        id          BIGINT IDENTITY(1,1) PRIMARY KEY,
        segment_id  INT NOT NULL REFERENCES intel.segment(id),
        person_id   UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        score       DECIMAL(5,2),
        reason      NVARCHAR(512),
        computed_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_segmember_pid ON intel.segment_member(person_id)`);
    await exec(pool, `CREATE INDEX IX_segmember_seg ON intel.segment_member(segment_id)`);

    await exec(pool, `
      CREATE TABLE intel.donor_score (
        id          UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id   UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        model_name  VARCHAR(64) NOT NULL,
        score       DECIMAL(5,2),
        score_band  VARCHAR(32),
        rationale   NVARCHAR(MAX),
        computed_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_dscore_pid ON intel.donor_score(person_id)`);

    await exec(pool, `
      CREATE TABLE intel.forecast (
        id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id       UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        metric          VARCHAR(64) NOT NULL,
        predicted_value DECIMAL(12,2),
        confidence      DECIMAL(5,2),
        horizon_months  INT DEFAULT 12,
        computed_at     DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_forecast_pid ON intel.forecast(person_id)`);

    await exec(pool, `
      CREATE TABLE intel.next_best_action (
        id            UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id     UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        action        VARCHAR(128) NOT NULL,
        channel       VARCHAR(64),
        rationale     NVARCHAR(MAX),
        expected_lift DECIMAL(5,2),
        confidence    DECIMAL(5,2),
        computed_at   DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_nba_pid ON intel.next_best_action(person_id)`);

    await exec(pool, `
      CREATE TABLE intel.sentiment (
        id                  UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
        person_id           UNIQUEIDENTIFIER NOT NULL REFERENCES person.profile(id),
        source_activity_id  UNIQUEIDENTIFIER,
        sentiment           VARCHAR(32) NOT NULL,
        topic               NVARCHAR(256),
        confidence          DECIMAL(5,2),
        computed_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_sentiment_pid ON intel.sentiment(person_id)`);

    // ── SERVING (materialized tables) ──
    console.log('[13] Creating serving tables...');

    await exec(pool, `
      CREATE TABLE serving.person_360 (
        person_id          UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        display_name       NVARCHAR(256),
        first_name         NVARCHAR(128),
        last_name          NVARCHAR(128),
        email              VARCHAR(256),
        phone              VARCHAR(32),
        household_id       UNIQUEIDENTIFIER,
        household_name     NVARCHAR(256),
        source_systems     VARCHAR(256),
        lifetime_giving    DECIMAL(12,2) DEFAULT 0,
        donation_count     INT DEFAULT 0,
        avg_gift           DECIMAL(12,2) DEFAULT 0,
        first_gift_date    DATETIME2,
        last_gift_date     DATETIME2,
        largest_gift       DECIMAL(12,2) DEFAULT 0,
        recency_days       INT,
        frequency_annual   DECIMAL(8,2) DEFAULT 0,
        monetary_annual    DECIMAL(12,2) DEFAULT 0,
        active_subscriptions INT DEFAULT 0,
        subscription_months INT DEFAULT 0,
        last_event_date    DATETIME2,
        events_attended    INT DEFAULT 0,
        tickets_total      INT DEFAULT 0,
        engagement_count   INT DEFAULT 0,
        last_engagement    DATETIME2,
        tag_count          INT DEFAULT 0,
        lifecycle_stage    VARCHAR(32),
        churn_risk         DECIMAL(5,2),
        ltv_estimate       DECIMAL(12,2),
        top_segments_json  NVARCHAR(MAX),
        next_action_json   NVARCHAR(MAX),
        updated_at         DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    await exec(pool, `CREATE INDEX IX_p360_lifecycle ON serving.person_360(lifecycle_stage)`);
    await exec(pool, `CREATE INDEX IX_p360_household ON serving.person_360(household_id)`);
    await exec(pool, `CREATE INDEX IX_p360_lastgift ON serving.person_360(last_gift_date)`);

    await exec(pool, `
      CREATE TABLE serving.household_360 (
        household_id          UNIQUEIDENTIFIER NOT NULL PRIMARY KEY,
        name                  NVARCHAR(256),
        member_count          INT DEFAULT 0,
        members_json          NVARCHAR(MAX),
        household_giving_total DECIMAL(12,2) DEFAULT 0,
        household_annual_giving DECIMAL(12,2) DEFAULT 0,
        giving_trend          VARCHAR(16),
        active_subs           INT DEFAULT 0,
        events_attended       INT DEFAULT 0,
        health_score          DECIMAL(5,2),
        best_contact_method   VARCHAR(64),
        updated_at            DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);

    // ── Seed source systems ──
    console.log('\n[14] Seeding source systems...');
    const sources = [
      ['bloomerang', 'Bloomerang'],
      ['donor_direct', 'Donor Direct'],
      ['givebutter', 'Givebutter'],
      ['keap', 'Keap (Infusionsoft)'],
      ['kindful', 'Kindful'],
      ['stripe', 'Stripe'],
      ['transactions_imports', 'Transaction Imports'],
    ];
    for (const [name, display] of sources) {
      await pool.request()
        .input('name', sql.VarChar, name)
        .input('display', sql.NVarChar, display)
        .query(`
          IF NOT EXISTS (SELECT 1 FROM meta.source_system WHERE name = @name)
            INSERT INTO meta.source_system (name, display_name) VALUES (@name, @display)
        `);
    }

    // ── Summary ──
    const counts = await pool.request().query(`
      SELECT s.name AS schema_name, COUNT(*) AS table_count
      FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name IN ('meta','raw','staging','person','household','giving','commerce','event','engagement','intel','serving')
      GROUP BY s.name
      ORDER BY s.name
    `);

    console.log('\nSchema created successfully:');
    let total = 0;
    for (const r of counts.recordset) {
      console.log(`  ${r.schema_name}: ${r.table_count} tables`);
      total += r.table_count;
    }
    console.log(`  TOTAL: ${total} tables across ${counts.recordset.length} schemas`);

    const srcCount = await pool.request().query('SELECT COUNT(*) AS cnt FROM meta.source_system');
    console.log(`  Source systems seeded: ${srcCount.recordset[0].cnt}`);

  } finally {
    await pool.close();
  }

  console.log('\nDone.');
}

main().catch(err => {
  console.error('FATAL:', err.message);
  console.error(err.stack);
  process.exit(1);
});
