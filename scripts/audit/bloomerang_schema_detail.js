const fs = require('fs');
const path = require('path');
const sql = require('mssql');

function loadEnv() {
  const envPath = path.join(process.cwd(), '.env.local');
  const lines = fs.readFileSync(envPath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
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
    options: { encrypt: true, trustServerCertificate: false, requestTimeout: 120000 },
    pool: { max: 3 },
  });

  console.log('=== BLOOMERANG SCHEMA TABLES ===');
  const tables = await pool.request().query(
    "SELECT t.name FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'bloomerang' ORDER BY t.name");
  console.log('Tables:', tables.recordset.map(r => r.name));

  for (const tbl of tables.recordset) {
    const safeName = tbl.name;
    console.log('\n--- bloomerang.[' + safeName + '] ---');

    // columns
    const cols = await pool.request().query(
      "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE " +
      "FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'bloomerang' AND TABLE_NAME = '" + safeName + "' ORDER BY ORDINAL_POSITION");
    console.table(cols.recordset);

    // row count
    const cnt = await pool.request().query("SELECT COUNT(*) AS row_count FROM bloomerang.[" + safeName + "]");
    console.log('Row count:', cnt.recordset[0].row_count);

    // sample
    const sample = await pool.request().query("SELECT TOP 3 * FROM bloomerang.[" + safeName + "]");
    if (sample.recordset.length > 0) {
      console.log('Sample (top 3):');
      for (const row of sample.recordset) {
        // Truncate long values for display
        const display = {};
        for (const [k, v] of Object.entries(row)) {
          if (typeof v === 'string' && v.length > 80) display[k] = v.substring(0, 80) + '...';
          else display[k] = v;
        }
        console.log(JSON.stringify(display, null, 2));
      }
    }
  }

  await pool.close();
  process.exit(0);
}
main().catch(e => { console.error('FATAL:', e.message); process.exit(1); });
