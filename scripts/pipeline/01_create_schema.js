const { runSqlFile } = require('./_db');

async function main() {
  await runSqlFile('sql/pipeline/001_init_pipeline.sql');
  console.log('OK: schemas and core tables created/verified.');
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
