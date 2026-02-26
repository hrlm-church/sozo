/**
 * Create memory/learning schema for Sozo continuous learning system.
 * Tables: sozo.conversation_summary, sozo.knowledge
 *
 * Usage: node scripts/pipeline/create_memory_schema.js
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

  console.log('Creating memory/learning schema...\n');

  // Ensure sozo schema exists
  await pool.request().query(`
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'sozo')
      EXEC('CREATE SCHEMA sozo')
  `);

  // 1. conversation_summary — stores AI-extracted summaries of each conversation
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'sozo' AND t.name = 'conversation_summary'
    )
    CREATE TABLE sozo.conversation_summary (
      id              NVARCHAR(36)   NOT NULL PRIMARY KEY,
      owner_email     NVARCHAR(256)  NOT NULL,
      title           NVARCHAR(256)  NOT NULL DEFAULT 'Untitled',
      summary_text    NVARCHAR(MAX)  NOT NULL,
      topics          NVARCHAR(MAX)  NULL,
      query_patterns  NVARCHAR(MAX)  NULL,
      message_count   INT            NOT NULL DEFAULT 0,
      created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      updated_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      CONSTRAINT FK_convsummary_conv FOREIGN KEY (id)
        REFERENCES sozo.conversation(id) ON DELETE CASCADE
    )
  `);
  console.log('  sozo.conversation_summary OK');

  // 2. knowledge — structured, append-only knowledge base
  await pool.request().query(`
    IF NOT EXISTS (
      SELECT 1 FROM sys.tables t
      JOIN sys.schemas s ON t.schema_id = s.schema_id
      WHERE s.name = 'sozo' AND t.name = 'knowledge'
    )
    CREATE TABLE sozo.knowledge (
      id              NVARCHAR(36)   NOT NULL PRIMARY KEY,
      owner_email     NVARCHAR(256)  NOT NULL,
      category        NVARCHAR(50)   NOT NULL,
      content         NVARCHAR(2000) NOT NULL,
      source_conv_id  NVARCHAR(36)   NULL,
      confidence      DECIMAL(3,2)   NOT NULL DEFAULT 0.80,
      supersedes_id   NVARCHAR(36)   NULL,
      is_active       BIT            NOT NULL DEFAULT 1,
      created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      updated_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
      CONSTRAINT FK_knowledge_conv FOREIGN KEY (source_conv_id)
        REFERENCES sozo.conversation(id) ON DELETE SET NULL
    )
  `);
  console.log('  sozo.knowledge OK');

  // Create indexes
  const indexes = [
    { name: 'IX_convsummary_owner', sql: 'CREATE INDEX IX_convsummary_owner ON sozo.conversation_summary(owner_email, updated_at DESC)' },
    { name: 'IX_knowledge_owner', sql: 'CREATE INDEX IX_knowledge_owner ON sozo.knowledge(owner_email, is_active, category)' },
    { name: 'IX_knowledge_supersedes', sql: 'CREATE INDEX IX_knowledge_supersedes ON sozo.knowledge(supersedes_id) WHERE supersedes_id IS NOT NULL' },
  ];

  for (const idx of indexes) {
    try {
      await pool.request().query(idx.sql);
      console.log(`  ${idx.name} OK`);
    } catch (e) {
      if (e.message.includes('already exists')) {
        console.log(`  ${idx.name} (already exists)`);
      } else {
        console.log(`  ${idx.name} WARNING: ${e.message.slice(0, 100)}`);
      }
    }
  }

  // Verify
  const counts = await pool.request().query(`
    SELECT
      (SELECT COUNT(*) FROM sozo.conversation_summary) AS summaries,
      (SELECT COUNT(*) FROM sozo.knowledge) AS knowledge_items
  `);
  console.log('\nVerification:', counts.recordset[0]);

  await pool.close();
  console.log('\nDone. Memory tables created.');
}

main().catch(err => { console.error('FATAL:', err.message); process.exit(1); });
