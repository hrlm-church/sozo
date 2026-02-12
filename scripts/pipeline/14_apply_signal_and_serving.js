const fs = require('fs');
const path = require('path');
const { withDb } = require('./_db');

async function main() {
  const sqlPath = path.join(process.cwd(), 'sql/pipeline/004_signal_and_serving.sql');
  const sqlText = fs.readFileSync(sqlPath, 'utf8');
  await withDb(async (pool) => {
    await pool.request().batch(sqlText);
  });
  console.log('OK: signal + serving SQL objects created/verified.');
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
