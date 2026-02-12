const fs = require('fs');
const { withDb } = require('./_db');

const mappingPath = process.argv[2] || 'reports/catalog/TAG_SIGNAL_GROUP_MAPPING.json';

function sqlSafe(value) {
  return String(value || '').replace(/'/g, "''");
}

async function main() {
  if (!fs.existsSync(mappingPath)) {
    throw new Error(`Mapping file not found: ${mappingPath}`);
  }

  const payload = JSON.parse(fs.readFileSync(mappingPath, 'utf8'));
  const sourceRows = payload.mapping || [];
  if (!sourceRows.length) {
    throw new Error(`No mapping rows found in: ${mappingPath}`);
  }

  // Defensive dedupe by canonical key because some tags can repeat in generated files.
  const deduped = new Map();
  for (const row of sourceRows) {
    const key = `donor_direct|${String(row.tag || '').trim().toLowerCase()}`;
    const existing = deduped.get(key);
    if (!existing || Number(row.confidence || 0) > Number(existing.confidence || 0)) {
      deduped.set(key, row);
    }
  }
  const rows = [...deduped.values()];

  await withDb(async (pool) => {
    await pool.request().batch(`
IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON t.schema_id=s.schema_id WHERE s.name='meta' AND t.name='tag_signal_map')
BEGIN
  CREATE TABLE meta.tag_signal_map (
    tag_signal_map_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    source_system VARCHAR(64) NOT NULL,
    tag_value NVARCHAR(512) NOT NULL,
    tag_prefix VARCHAR(128) NULL,
    signal_group VARCHAR(64) NOT NULL,
    confidence DECIMAL(5,2) NOT NULL,
    needs_review BIT NOT NULL DEFAULT 0,
    rule_code VARCHAR(128) NULL,
    is_active BIT NOT NULL DEFAULT 1,
    created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT uq_tag_signal_map UNIQUE(source_system, tag_value)
  );
END;
`);

    await pool.request().batch(`
DELETE FROM meta.tag_signal_map
WHERE source_system='donor_direct';
`);

    const chunkSize = 200;
    for (let i = 0; i < rows.length; i += chunkSize) {
      const chunk = rows.slice(i, i + chunkSize);
      const values = chunk.map((r) => {
        const tagValue = sqlSafe(r.tag);
        const tagPrefix = sqlSafe(r.tag_prefix || 'Unscoped');
        const signalGroup = sqlSafe(r.signal_group);
        const confidence = Number(r.confidence || 0).toFixed(2);
        const needsReview = r.needs_review ? 1 : 0;
        const ruleCode = sqlSafe(r.rule || 'n/a');
        return `('donor_direct',N'${tagValue}','${tagPrefix}','${signalGroup}',${confidence},${needsReview},'${ruleCode}',1)`;
      }).join(',\n');

      await pool.request().batch(`
INSERT INTO meta.tag_signal_map(source_system,tag_value,tag_prefix,signal_group,confidence,needs_review,rule_code,is_active)
VALUES
${values};
`);
    }

    const check = await pool.request().query(`
SELECT
  COUNT(1) AS total_rows,
  SUM(CASE WHEN needs_review=1 THEN 1 ELSE 0 END) AS review_rows
FROM meta.tag_signal_map
WHERE source_system='donor_direct';
`);

    console.log(JSON.stringify({
      loaded_rows: Number(check.recordset[0].total_rows),
      review_rows: Number(check.recordset[0].review_rows),
      input_rows: sourceRows.length,
      deduped_rows: rows.length,
      table: 'meta.tag_signal_map',
      source_system: 'donor_direct',
    }, null, 2));
  });
}

main().catch((err) => {
  console.error(`ERROR: ${err.message}`);
  process.exit(1);
});
