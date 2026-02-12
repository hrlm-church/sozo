const fs = require('fs');
const sql = require('mssql');

const env = Object.fromEntries(
  fs.readFileSync('.env.local', 'utf8')
    .split(/\r?\n/)
    .filter((l) => l && !l.startsWith('#') && l.includes('='))
    .map((l) => {
      const i = l.indexOf('=');
      return [l.slice(0, i), l.slice(i + 1)];
    })
);

(async () => {
  const pool = await sql.connect({
    server: env.SOZO_SQL_HOST,
    database: env.SOZO_SQL_DB,
    user: env.SOZO_SQL_USER,
    password: env.SOZO_SQL_PASSWORD,
    options: { encrypt: true, trustServerCertificate: false },
  });

  const q = await pool.request().query(`
    SELECT COUNT(1) AS person_360 FROM serving.v_person_overview;
    SELECT COUNT(1) AS household_360 FROM serving.v_household_overview;
    SELECT COUNT(1) AS signal_rows FROM serving.v_signal_explorer;
    SELECT COUNT(1) AS unresolved_person_signals
    FROM gold.signal_fact
    WHERE canonical_type='person' AND canonical_id IS NULL;
  `);

  console.log(JSON.stringify(q.recordsets, null, 2));
  await pool.close();
})().catch((e) => {
  console.error(e.message);
  process.exit(1);
});
