/**
 * Create the dashboard persistence schema.
 * Run: node scripts/setup/02_dashboard_schema.js
 */
const fs = require('fs');
const path = require('path');
const sql = require('mssql');

function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(p)) { console.error('ERROR: .env.local not found'); process.exit(1); }
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}

function getDbConfig() {
  return {
    server: process.env.SOZO_SQL_HOST,
    database: process.env.SOZO_SQL_DB,
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 120000,
    options: { encrypt: true, trustServerCertificate: false },
    pool: { max: 2, min: 0, idleTimeoutMillis: 10000 },
  };
}

async function main() {
  loadEnv();
  console.log('Creating dashboard schema...');

  const ddl = fs.readFileSync(
    path.join(__dirname, '..', '..', 'sql', '002_dashboard_schema.sql'),
    'utf8',
  );

  const pool = await sql.connect(getDbConfig());
  try {
    // Split on GO and execute each batch
    const batches = ddl.split(/^\s*GO\s*$/im).filter(b => b.trim());
    for (const batch of batches) {
      await pool.request().batch(batch);
    }
    console.log('Dashboard schema created successfully.');

    // Verify tables exist
    const result = await pool.request().query(`
      SELECT TABLE_SCHEMA, TABLE_NAME
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = 'dashboard'
      ORDER BY TABLE_NAME
    `);
    console.log('Tables:');
    for (const r of result.recordset) {
      console.log(`  ${r.TABLE_SCHEMA}.${r.TABLE_NAME}`);
    }
  } finally {
    await pool.close();
  }

  console.log('Done.');
}

main().catch(err => {
  console.error('FATAL:', err.message);
  process.exit(1);
});
