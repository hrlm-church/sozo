/**
 * Create RBAC tables: sozo.organization + sozo.org_member
 * Run once: node scripts/pipeline/create_rbac_schema.js
 */
const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    // 1. Organization table
    await pool.request().batch(`
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'organization' AND schema_id = SCHEMA_ID('sozo'))
      CREATE TABLE sozo.organization (
        id              NVARCHAR(36)   PRIMARY KEY DEFAULT NEWID(),
        name            NVARCHAR(256)  NOT NULL,
        slug            NVARCHAR(100)  NOT NULL UNIQUE,
        is_active       BIT            NOT NULL DEFAULT 1,
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
      )
    `);
    console.log('Table [sozo.organization] ensured.');

    // 2. Org member table (links users to orgs with roles)
    await pool.request().batch(`
      IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'org_member' AND schema_id = SCHEMA_ID('sozo'))
      CREATE TABLE sozo.org_member (
        id              NVARCHAR(36)   PRIMARY KEY DEFAULT NEWID(),
        org_id          NVARCHAR(36)   NOT NULL,
        email           NVARCHAR(256)  NOT NULL,
        role            NVARCHAR(20)   NOT NULL DEFAULT 'viewer'
                        CHECK (role IN ('admin', 'analyst', 'viewer')),
        invited_by      NVARCHAR(256),
        is_active       BIT            NOT NULL DEFAULT 1,
        created_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at      DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

        FOREIGN KEY (org_id) REFERENCES sozo.organization(id),
        CONSTRAINT UQ_org_member_email UNIQUE (org_id, email),
        INDEX IX_org_member_email (email),
        INDEX IX_org_member_org (org_id, is_active)
      )
    `);
    console.log('Table [sozo.org_member] ensured.');

    // 3. Seed default organization for Pure Freedom Ministries
    const existing = await pool.request().query(`
      SELECT id FROM sozo.organization WHERE slug = 'pure-freedom'
    `);

    let orgId;
    if (existing.recordset.length === 0) {
      const result = await pool.request().query(`
        INSERT INTO sozo.organization (id, name, slug)
        OUTPUT INSERTED.id
        VALUES (NEWID(), 'Pure Freedom Ministries', 'pure-freedom')
      `);
      orgId = result.recordset[0].id;
      console.log('Created default org: Pure Freedom Ministries (id:', orgId, ')');
    } else {
      orgId = existing.recordset[0].id;
      console.log('Default org already exists (id:', orgId, ')');
    }

    // 4. Seed current users as admins (find distinct owner_emails from conversations)
    const users = await pool.request().query(`
      SELECT DISTINCT owner_email FROM sozo.conversation
      WHERE owner_email IS NOT NULL AND owner_email != ''
    `);

    let seeded = 0;
    for (const row of users.recordset) {
      const email = row.owner_email;
      const memberExists = await pool.request().query(`
        SELECT 1 FROM sozo.org_member WHERE org_id = '${orgId}' AND email = '${email}'
      `);
      if (memberExists.recordset.length === 0) {
        await pool.request().query(`
          INSERT INTO sozo.org_member (id, org_id, email, role)
          VALUES (NEWID(), '${orgId}', '${email}', 'admin')
        `);
        seeded++;
        console.log('  Seeded admin:', email);
      }
    }
    console.log(`Seeded ${seeded} admin users.`);
  });

  console.log('\nDone.');
}

main().catch((err) => {
  console.error('Failed:', err.message);
  process.exit(1);
});
