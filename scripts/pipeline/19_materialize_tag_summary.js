/**
 * 19_materialize_tag_summary.js
 *
 * Materializes serving.tag_summary from serving.tag_detail for fast
 * tag aggregation queries. Run after tag data is loaded.
 *
 * Usage: node scripts/pipeline/19_materialize_tag_summary.js
 */

const { getPool, closePool } = require('./_db');

async function main() {
  const pool = await getPool();
  console.log('[tag_summary] Starting materialization...');

  // Truncate and rebuild
  await pool.request().query(`
    TRUNCATE TABLE serving.tag_summary;

    INSERT INTO serving.tag_summary (person_id, display_name, tag_group, tag_count, distinct_tags, most_recent_tag, most_recent_at)
    SELECT
      td.person_id,
      MAX(td.display_name) AS display_name,
      td.tag_group,
      COUNT(*) AS tag_count,
      COUNT(DISTINCT td.tag_value) AS distinct_tags,
      (SELECT TOP 1 t2.tag_value FROM serving.tag_detail t2
       WHERE t2.person_id = td.person_id AND t2.tag_group = td.tag_group
       ORDER BY t2.applied_at DESC) AS most_recent_tag,
      MAX(td.applied_at) AS most_recent_at
    FROM serving.tag_detail td
    WHERE td.display_name <> 'Unknown'
    GROUP BY td.person_id, td.tag_group;
  `);

  const result = await pool.request().query('SELECT COUNT(*) AS cnt FROM serving.tag_summary');
  console.log(`[tag_summary] Materialized ${result.recordset[0].cnt} rows.`);

  await closePool();
}

main().catch((err) => {
  console.error('[tag_summary] Error:', err);
  process.exit(1);
});
