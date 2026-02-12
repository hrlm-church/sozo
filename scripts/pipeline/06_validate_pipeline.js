const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    const rowCounts = await pool.request().query(`
SELECT source_system, COUNT(1) AS bronze_rows
FROM bronze.raw_record
GROUP BY source_system
ORDER BY source_system;
`);

    const nullChecks = await pool.request().query(`
SELECT 'silver.person_source.missing_source_record_id' AS check_name, COUNT(1) AS issue_count
FROM silver.person_source WHERE source_record_id IS NULL OR LTRIM(RTRIM(source_record_id))=''
UNION ALL
SELECT 'silver.transaction_source.missing_source_record_id', COUNT(1)
FROM silver.transaction_source WHERE source_record_id IS NULL OR LTRIM(RTRIM(source_record_id))=''
UNION ALL
SELECT 'gold.crosswalk.missing_canonical_id', COUNT(1)
FROM gold.crosswalk WHERE canonical_id IS NULL;
`);

    const dupChecks = await pool.request().query(`
SELECT 'gold.crosswalk.duplicate_source_key' AS check_name, COUNT(1) AS duplicate_groups
FROM (
  SELECT source_system, source_record_id, COUNT(1) c
  FROM gold.crosswalk
  GROUP BY source_system, source_record_id
  HAVING COUNT(1) > 1
) d;
`);

    const unmatched = await pool.request().query(`
SELECT
  source_system,
  source_record_id,
  match_method,
  match_confidence,
  possible_match
FROM gold.crosswalk
WHERE canonical_type='person' AND possible_match=1
ORDER BY created_at DESC;
`);

    console.log('ROW_COUNTS_BY_SOURCE');
    console.table(rowCounts.recordset);

    console.log('NULL_KEY_CHECKS');
    console.table(nullChecks.recordset);

    console.log('DUPLICATE_KEY_CHECKS');
    console.table(dupChecks.recordset);

    console.log('UNMATCHED_IDENTITY_CLUSTERS');
    console.table(unmatched.recordset);
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
