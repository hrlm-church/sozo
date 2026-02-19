/**
 * Create conversation persistence schema for Sozo chat memory.
 * Tables: sozo.conversation, sozo.conversation_message
 *
 * Usage: node scripts/pipeline/create_conversation_schema.js
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
    requestTimeout: 30000,
  });

  console.log('Creating conversation schema...');

  // Create schema
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sozo')
      EXEC('CREATE SCHEMA sozo')
  `);

  // Create conversation table
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'sozo' AND t.name = 'conversation')
    CREATE TABLE sozo.conversation (
      id           NVARCHAR(36)   NOT NULL PRIMARY KEY,
      title        NVARCHAR(256)  NOT NULL DEFAULT 'New Chat',
      owner_email  NVARCHAR(256)  NOT NULL DEFAULT 'anonymous@sozo.local',
      message_count INT           NOT NULL DEFAULT 0,
      created_at   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      updated_at   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
    )
  `);
  console.log('  sozo.conversation OK');

  // Create message table
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'sozo' AND t.name = 'conversation_message')
    CREATE TABLE sozo.conversation_message (
      id              NVARCHAR(36)   NOT NULL PRIMARY KEY,
      conversation_id NVARCHAR(36)   NOT NULL,
      role            NVARCHAR(20)   NOT NULL,
      content_json    NVARCHAR(MAX)  NOT NULL,
      created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      CONSTRAINT FK_msg_conv FOREIGN KEY (conversation_id) REFERENCES sozo.conversation(id) ON DELETE CASCADE
    )
  `);
  console.log('  sozo.conversation_message OK');

  // Create indexes
  try {
    await pool.request().query(`
      CREATE INDEX IX_conv_owner ON sozo.conversation(owner_email, updated_at DESC)
    `);
  } catch (e) { if (!e.message.includes('already exists')) console.log('  Index warning:', e.message.slice(0,100)); }

  try {
    await pool.request().query(`
      CREATE INDEX IX_msg_conv ON sozo.conversation_message(conversation_id, created_at)
    `);
  } catch (e) { if (!e.message.includes('already exists')) console.log('  Index warning:', e.message.slice(0,100)); }

  console.log('  Indexes OK');

  // Create feedback table (for step 3)
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'sozo' AND t.name = 'feedback')
    CREATE TABLE sozo.feedback (
      id              NVARCHAR(36)   NOT NULL PRIMARY KEY,
      conversation_id NVARCHAR(36),
      message_id      NVARCHAR(36),
      rating          INT            NOT NULL,
      owner_email     NVARCHAR(256),
      created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      CONSTRAINT FK_fb_conv FOREIGN KEY (conversation_id) REFERENCES sozo.conversation(id) ON DELETE SET NULL
    )
  `);
  console.log('  sozo.feedback OK');

  // Create insights table (for step 4)
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'sozo' AND t.name = 'insight')
    CREATE TABLE sozo.insight (
      id              NVARCHAR(36)   NOT NULL PRIMARY KEY,
      insight_text    NVARCHAR(1000) NOT NULL,
      category        NVARCHAR(100),
      confidence      DECIMAL(3,2)   DEFAULT 0.8,
      source_query    NVARCHAR(MAX),
      owner_email     NVARCHAR(256),
      created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      expires_at      DATETIME2
    )
  `);
  console.log('  sozo.insight OK');

  try {
    await pool.request().query(`CREATE INDEX IX_insight_owner ON sozo.insight(owner_email, created_at DESC)`);
  } catch (e) { if (!e.message.includes('already exists')) console.log('  Index warning:', e.message.slice(0,100)); }

  await pool.close();
  console.log('\nDone. All conversation/feedback/insight tables created.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
