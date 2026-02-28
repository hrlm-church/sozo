/**
 * Create token usage tracking table for per-org budget enforcement.
 *
 * Usage: node scripts/pipeline/create_token_budget_schema.js
 */
const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    // Token usage log — one row per chat request
    await pool.request().query(`
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'token_usage' AND schema_id = SCHEMA_ID('sozo'))
      BEGIN
        CREATE TABLE sozo.token_usage (
          id              NVARCHAR(36)   PRIMARY KEY DEFAULT NEWID(),
          org_id          NVARCHAR(36)   NOT NULL,
          user_email      NVARCHAR(256)  NOT NULL,
          input_tokens    INT            NOT NULL DEFAULT 0,
          output_tokens   INT            NOT NULL DEFAULT 0,
          model_name      NVARCHAR(100),
          request_id      NVARCHAR(36),
          created_at      DATETIME2      DEFAULT SYSUTCDATETIME()
        );

        CREATE INDEX IX_token_usage_org_date
          ON sozo.token_usage (org_id, created_at);
        CREATE INDEX IX_token_usage_user_date
          ON sozo.token_usage (user_email, created_at);

        PRINT 'Created sozo.token_usage table';
      END
      ELSE
        PRINT 'sozo.token_usage already exists';
    `);

    // Org budget config — one row per org with monthly limits
    await pool.request().query(`
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'org_budget' AND schema_id = SCHEMA_ID('sozo'))
      BEGIN
        CREATE TABLE sozo.org_budget (
          org_id            NVARCHAR(36)   PRIMARY KEY,
          monthly_token_limit BIGINT       NOT NULL DEFAULT 5000000,
          alert_threshold   DECIMAL(3,2)   NOT NULL DEFAULT 0.80,
          is_enforced       BIT            NOT NULL DEFAULT 1,
          updated_at        DATETIME2      DEFAULT SYSUTCDATETIME(),
          FOREIGN KEY (org_id) REFERENCES sozo.organization(id)
        );

        PRINT 'Created sozo.org_budget table';
      END
      ELSE
        PRINT 'sozo.org_budget already exists';
    `);

    // Seed default budget for existing orgs
    await pool.request().query(`
      INSERT INTO sozo.org_budget (org_id, monthly_token_limit, alert_threshold)
      SELECT id, 5000000, 0.80
      FROM sozo.organization o
      WHERE NOT EXISTS (SELECT 1 FROM sozo.org_budget b WHERE b.org_id = o.id);
    `);

    console.log('Token budget schema created successfully.');
  });
}

main().catch((err) => {
  console.error('Failed:', err.message);
  process.exit(1);
});
