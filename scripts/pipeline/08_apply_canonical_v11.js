const { runSqlFile } = require('./_db');

async function main() {
  await runSqlFile('sql/pipeline/002_canonical_v11.sql');
  console.log('OK: canonical v1.1 and intelligence schemas/tables created/verified.');
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
