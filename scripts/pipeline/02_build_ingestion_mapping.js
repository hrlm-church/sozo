const { withDb } = require('./_db');

const SOURCES = [
  'bloomerang',
  'donor_direct',
  'givebutter',
  'keap',
  'kindful',
  'stripe',
  'transactions_imports',
];

const MAPPINGS = [
  { entity: 'person', pattern: '%contact%.csv', parser: 'csv_generic', target: 'silver.person_source' },
  { entity: 'organization', pattern: '%company%.csv', parser: 'csv_generic', target: 'silver.person_source' },
  { entity: 'transaction', pattern: '%invoice%.csv', parser: 'csv_generic', target: 'silver.transaction_source' },
  { entity: 'transaction', pattern: '%payment%.csv', parser: 'csv_generic', target: 'silver.transaction_source' },
  { entity: 'transaction', pattern: '%order%.csv', parser: 'csv_generic', target: 'silver.transaction_source' },
  { entity: 'engagement', pattern: '%note%.csv', parser: 'csv_generic', target: 'silver.engagement_source' },
  { entity: 'engagement', pattern: '%activity%.csv', parser: 'csv_generic', target: 'silver.engagement_source' },
  { entity: 'fallback', pattern: '%.csv', parser: 'csv_generic', target: 'silver.engagement_source' },
];

async function main() {
  await withDb(async (pool) => {
    for (const source of SOURCES) {
      await pool
        .request()
        .input('source_system', source)
        .input('display_name', source.replace(/_/g, ' '))
        .batch(`
IF NOT EXISTS (SELECT 1 FROM meta.source_system WHERE source_system = @source_system)
  INSERT INTO meta.source_system (source_system, display_name) VALUES (@source_system, @display_name);
`);

      for (const m of MAPPINGS) {
        await pool
          .request()
          .input('source_system', source)
          .input('entity_name', m.entity)
          .input('file_pattern', m.pattern)
          .input('parser_name', m.parser)
          .input('target_table', m.target)
          .batch(`
IF NOT EXISTS (
  SELECT 1 FROM meta.ingestion_mapping
  WHERE source_system=@source_system AND entity_name=@entity_name AND file_pattern=@file_pattern
)
INSERT INTO meta.ingestion_mapping(source_system, entity_name, file_pattern, parser_name, target_table, notes)
VALUES(@source_system,@entity_name,@file_pattern,@parser_name,@target_table,'Generated for multi-source Bronze/Silver ingestion');
`);
      }
    }

    const count = await pool.request().query('SELECT COUNT(1) AS c FROM meta.ingestion_mapping;');
    console.log(`OK: ingestion mappings ready (${count.recordset[0].c} rows).`);
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
