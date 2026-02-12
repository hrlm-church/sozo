const { withDb } = require('./_db');

async function main() {
  await withDb(async (pool) => {
    const rs = await pool.request().query(`
SELECT 'silver.person_tag_signal.total' AS check_name, COUNT(1) AS check_value FROM silver.person_tag_signal
UNION ALL
SELECT 'silver.person_tag_signal.needs_review', COUNT(1) FROM silver.person_tag_signal WHERE needs_review=1
UNION ALL
SELECT 'gold.signal_fact.total', COUNT(1) FROM gold.signal_fact
UNION ALL
SELECT 'gold.signal_fact.person_missing', COUNT(1) FROM gold.signal_fact WHERE canonical_type='person' AND canonical_id IS NULL
UNION ALL
SELECT 'serving.v_person_overview.total', COUNT(1) FROM serving.v_person_overview
UNION ALL
SELECT 'serving.v_household_overview.total', COUNT(1) FROM serving.v_household_overview
UNION ALL
SELECT 'serving.v_signal_explorer.total', COUNT(1) FROM serving.v_signal_explorer;
`);

    console.table(rs.recordset);
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
