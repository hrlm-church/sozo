const fs = require('fs'), path = require('path'), sql = require('mssql');
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}
loadEnv();
async function main() {
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST, database: 'sozov2',
    user: process.env.SOZO_SQL_USER, password: process.env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
    requestTimeout: 120000
  });
  const q = await pool.request().query(
    "SELECT category_name, group_name, group_description FROM silver.tag ORDER BY category_name, group_name"
  );

  const byCat = {};
  for (const r of q.recordset) {
    const cat = r.category_name || '(none)';
    if (!byCat[cat]) byCat[cat] = [];
    byCat[cat].push({ name: r.group_name, desc: r.group_description });
  }

  const sorted = Object.entries(byCat).sort((a, b) => b[1].length - a[1].length);
  for (const [cat, tags] of sorted) {
    console.log(`\n=== ${cat} (${tags.length} tags) ===`);
    for (const t of tags) {
      const desc = (t.desc && t.desc !== 'NULL' && t.desc !== t.name) ? ` -- ${t.desc}` : '';
      console.log(`  ${t.name}${desc}`);
    }
  }
  await pool.close();
}
main().catch(e => { console.error('FAIL:', e.message); process.exit(1); });
