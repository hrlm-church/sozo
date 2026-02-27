/**
 * Create audit.api_log table for request-level audit trail.
 * Run once: node scripts/pipeline/create_audit_schema.js
 */
const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    // Create audit schema if not exists
    await pool.request().batch(`
      IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'audit')
        EXEC('CREATE SCHEMA audit')
    `);
    console.log('Schema [audit] ensured.');

    // Create api_log table
    await pool.request().batch(`
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'api_log' AND schema_id = SCHEMA_ID('audit'))
      CREATE TABLE audit.api_log (
        id              BIGINT IDENTITY(1,1) PRIMARY KEY,
        request_id      NVARCHAR(36)   NOT NULL,
        timestamp       DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        method          NVARCHAR(10)   NOT NULL,
        path            NVARCHAR(500)  NOT NULL,
        status_code     INT            NOT NULL,
        duration_ms     INT            NOT NULL,
        user_email      NVARCHAR(256),
        ip_address      NVARCHAR(45),
        user_agent      NVARCHAR(500),
        error_message   NVARCHAR(1000),

        INDEX ix_api_log_timestamp (timestamp DESC),
        INDEX ix_api_log_user (user_email, timestamp DESC),
        INDEX ix_api_log_path (path, timestamp DESC)
      )
    `);
    console.log('Table [audit.api_log] ensured.');

    console.log('\nTo set up auto-cleanup, create an Azure SQL Agent job:');
    console.log("  DELETE FROM audit.api_log WHERE timestamp < DATEADD(day, -90, SYSUTCDATETIME())");
  });

  console.log('\nDone.');
}

main().catch((err) => {
  console.error('Failed:', err.message);
  process.exit(1);
});
