/**
 * Bloomerang Data Audit — sozov2 database
 *
 * Connects to Azure SQL (sozov2) and investigates all Bloomerang data
 * across bronze, silver, meta, and serving layers.
 */

const fs = require('fs');
const path = require('path');
const sql = require('mssql');

// ── env ─────────────────────────────────────────────────────────────────────
function loadEnv() {
  const envPath = path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(envPath)) {
    console.error('ERROR: .env.local not found');
    process.exit(1);
  }
  const lines = fs.readFileSync(envPath, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) {
      const val = m[2].replace(/^["']|["']$/g, '');
      process.env[m[1]] = val;
    }
  }
}

// ── helpers ─────────────────────────────────────────────────────────────────
function hr(title) {
  console.log('\n' + '='.repeat(70));
  console.log('  ' + title);
  console.log('='.repeat(70));
}

function printTable(rows) {
  if (!rows || rows.length === 0) {
    console.log('  (no rows)');
    return;
  }
  console.table(rows);
}

async function safeQuery(pool, label, queryText) {
  try {
    const result = await pool.request().query(queryText);
    console.log('\n  [' + label + ']');
    printTable(result.recordset);
    return result.recordset;
  } catch (err) {
    console.log('\n  [' + label + '] ERROR: ' + err.message);
    return [];
  }
}

// ── main ────────────────────────────────────────────────────────────────────
async function main() {
  loadEnv();

  const config = {
    server: process.env.SOZO_SQL_HOST,
    database: 'sozov2',
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    options: {
      encrypt: true,
      trustServerCertificate: false,
      requestTimeout: 120000,
      connectTimeout: 30000,
    },
    pool: { max: 3, min: 0, idleTimeoutMillis: 30000 },
  };

  console.log('Connecting to ' + config.server + ' / ' + config.database + ' ...');
  const pool = await sql.connect(config);
  console.log('Connected.\n');

  // 1. List ALL schemas
  hr('1. ALL SCHEMAS IN sozov2');
  await safeQuery(pool, 'schemas',
    "SELECT s.name AS schema_name, COUNT(t.name) AS table_count " +
    "FROM sys.schemas s " +
    "LEFT JOIN sys.tables t ON t.schema_id = s.schema_id " +
    "GROUP BY s.name " +
    "HAVING COUNT(t.name) > 0 " +
    "ORDER BY table_count DESC");

  // 2. Tables with 'bloom' in the name
  hr('2. TABLES WITH bloom IN THE NAME');
  await safeQuery(pool, 'bloom tables',
    "SELECT TABLE_SCHEMA, TABLE_NAME " +
    "FROM INFORMATION_SCHEMA.TABLES " +
    "WHERE TABLE_NAME LIKE '%bloom%' OR TABLE_NAME LIKE '%Bloom%' " +
    "ORDER BY TABLE_SCHEMA, TABLE_NAME");

  // 3. All bronze tables
  hr('3. ALL BRONZE TABLES');
  const bronzeTables = await safeQuery(pool, 'bronze tables',
    "SELECT t.name AS table_name " +
    "FROM sys.tables t " +
    "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
    "WHERE s.name = 'bronze' " +
    "ORDER BY t.name");

  // 4. Row counts for all bronze tables
  hr('4. BRONZE TABLE ROW COUNTS');
  if (bronzeTables.length > 0) {
    const countQueries = bronzeTables.map(function(r) {
      return "SELECT '" + r.table_name + "' AS table_name, COUNT(*) AS row_count FROM bronze.[" + r.table_name + "]";
    });
    for (let i = 0; i < countQueries.length; i += 5) {
      const batch = countQueries.slice(i, i + 5).join(' UNION ALL ');
      await safeQuery(pool, 'bronze counts batch ' + (Math.floor(i/5)+1), batch);
    }
  }

  // 5. Check bronze tables for source columns and bloom data
  hr('5. BRONZE TABLES — DISTINCT SOURCE VALUES');
  for (const tbl of bronzeTables) {
    const colCheck = await pool.request().query(
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
      "WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = '" + tbl.table_name + "' " +
      "AND COLUMN_NAME IN ('source_system', 'source', 'data_source', 'file_name', 'file_path', 'table_name')");

    if (colCheck.recordset.length > 0) {
      for (const col of colCheck.recordset) {
        await safeQuery(pool, 'bronze.' + tbl.table_name + '.' + col.COLUMN_NAME + ' distinct values',
          "SELECT TOP 20 [" + col.COLUMN_NAME + "], COUNT(*) AS cnt " +
          "FROM bronze.[" + tbl.table_name + "] " +
          "GROUP BY [" + col.COLUMN_NAME + "] " +
          "ORDER BY cnt DESC");
      }
    }
  }

  // 6. All silver tables
  hr('6. ALL SILVER TABLES');
  const silverTables = await safeQuery(pool, 'silver tables',
    "SELECT t.name AS table_name " +
    "FROM sys.tables t " +
    "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
    "WHERE s.name = 'silver' " +
    "ORDER BY t.name");

  // 7. Silver tables — bloomerang source_system check
  hr('7. SILVER TABLES — BLOOMERANG source_system CHECK');
  for (const tbl of silverTables) {
    const colCheck = await pool.request().query(
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
      "WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = '" + tbl.table_name + "' " +
      "AND COLUMN_NAME = 'source_system'");

    if (colCheck.recordset.length > 0) {
      await safeQuery(pool, 'silver.' + tbl.table_name + ' bloom',
        "SELECT source_system, COUNT(*) AS cnt " +
        "FROM silver.[" + tbl.table_name + "] " +
        "WHERE source_system = 'bloomerang' " +
        "GROUP BY source_system");
    }
  }

  // 7b. Silver — all source_system values per table
  hr('7b. SILVER — ALL source_system VALUES PER TABLE');
  for (const tbl of silverTables) {
    const colCheck = await pool.request().query(
      "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
      "WHERE TABLE_SCHEMA = 'silver' AND TABLE_NAME = '" + tbl.table_name + "' " +
      "AND COLUMN_NAME = 'source_system'");

    if (colCheck.recordset.length > 0) {
      await safeQuery(pool, 'silver.' + tbl.table_name + ' all sources',
        "SELECT source_system, COUNT(*) AS cnt " +
        "FROM silver.[" + tbl.table_name + "] " +
        "GROUP BY source_system " +
        "ORDER BY cnt DESC");
    }
  }

  // 8. Meta / file_lineage — bloomerang files
  hr('8. META LAYER — BLOOMERANG');
  await safeQuery(pool, 'meta tables',
    "SELECT t.name AS table_name " +
    "FROM sys.tables t " +
    "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
    "WHERE s.name = 'meta' " +
    "ORDER BY t.name");

  await safeQuery(pool, 'meta file_lineage bloom',
    "SELECT * FROM meta.file_lineage " +
    "WHERE file_path LIKE '%bloom%' OR file_path LIKE '%Bloom%' " +
    "ORDER BY file_path");

  await safeQuery(pool, 'meta source_system bloom',
    "SELECT * FROM meta.source_system WHERE name LIKE '%bloom%' OR name LIKE '%Bloom%'");

  // 9. Identity map
  hr('9. IDENTITY_MAP — BLOOMERANG');
  await safeQuery(pool, 'identity_map bloom',
    "SELECT source_system, COUNT(*) AS cnt " +
    "FROM silver.identity_map " +
    "WHERE source_system = 'bloomerang' " +
    "GROUP BY source_system");

  // 10. Serving / gold layer
  hr('10. SERVING / GOLD LAYER');
  await safeQuery(pool, 'serving/gold tables',
    "SELECT s.name AS schema_name, t.name AS table_name " +
    "FROM sys.tables t " +
    "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
    "WHERE s.name IN ('serving', 'gold') " +
    "ORDER BY s.name, t.name");

  // 11. Bronze bloomerang table details
  hr('11. BRONZE BLOOMERANG TABLE DETAILS');
  const bloomBronze = bronzeTables.filter(function(t) {
    return t.table_name.toLowerCase().includes('bloom');
  });

  if (bloomBronze.length > 0) {
    for (const tbl of bloomBronze) {
      console.log('\n  --- bronze.' + tbl.table_name + ' COLUMNS ---');
      await safeQuery(pool, 'columns of bronze.' + tbl.table_name,
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE " +
        "FROM INFORMATION_SCHEMA.COLUMNS " +
        "WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = '" + tbl.table_name + "' " +
        "ORDER BY ORDINAL_POSITION");

      console.log('\n  --- bronze.' + tbl.table_name + ' SAMPLE (TOP 5) ---');
      await safeQuery(pool, 'sample of bronze.' + tbl.table_name,
        "SELECT TOP 5 * FROM bronze.[" + tbl.table_name + "]");
    }
  } else {
    console.log('  No bronze tables with "bloom" in the name.');
    console.log('  Checking generic bronze tables for bloomerang data...');

    for (const tbl of bronzeTables) {
      const colCheck = await pool.request().query(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS " +
        "WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = '" + tbl.table_name + "' " +
        "AND COLUMN_NAME IN ('source_system', 'source', 'file_name', 'file_path', 'table_name')");

      for (const col of colCheck.recordset) {
        try {
          const bloomCheck = await pool.request().query(
            "SELECT TOP 1 1 AS found FROM bronze.[" + tbl.table_name + "] " +
            "WHERE [" + col.COLUMN_NAME + "] LIKE '%bloom%' OR [" + col.COLUMN_NAME + "] LIKE '%Bloom%'");

          if (bloomCheck.recordset.length > 0) {
            console.log('\n  FOUND bloomerang data in bronze.' + tbl.table_name + ' via ' + col.COLUMN_NAME);

            await safeQuery(pool, 'columns of bronze.' + tbl.table_name,
              "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH " +
              "FROM INFORMATION_SCHEMA.COLUMNS " +
              "WHERE TABLE_SCHEMA = 'bronze' AND TABLE_NAME = '" + tbl.table_name + "' " +
              "ORDER BY ORDINAL_POSITION");

            await safeQuery(pool, 'bloom sample from bronze.' + tbl.table_name,
              "SELECT TOP 5 * FROM bronze.[" + tbl.table_name + "] " +
              "WHERE [" + col.COLUMN_NAME + "] LIKE '%bloom%' OR [" + col.COLUMN_NAME + "] LIKE '%Bloom%'");

            await safeQuery(pool, 'bloom count in bronze.' + tbl.table_name,
              "SELECT COUNT(*) AS bloom_rows FROM bronze.[" + tbl.table_name + "] " +
              "WHERE [" + col.COLUMN_NAME + "] LIKE '%bloom%' OR [" + col.COLUMN_NAME + "] LIKE '%Bloom%'");
          }
        } catch (e) { /* skip */ }
      }
    }
  }

  // 12. Raw schema check
  hr('12. RAW SCHEMA CHECK');
  const rawTables = await safeQuery(pool, 'raw tables',
    "SELECT t.name AS table_name " +
    "FROM sys.tables t " +
    "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
    "WHERE s.name = 'raw' " +
    "ORDER BY t.name");

  // If raw.record exists, check for bloomerang
  if (rawTables.some(function(t) { return t.table_name === 'record'; })) {
    await safeQuery(pool, 'raw.record bloom check',
      "SELECT TOP 5 * FROM raw.record WHERE source_system = 'bloomerang'");
    await safeQuery(pool, 'raw.record bloom count',
      "SELECT source_system, COUNT(*) AS cnt FROM raw.record WHERE source_system = 'bloomerang' GROUP BY source_system");
  }

  // 13. Search all varchar columns for 'bloomerang'
  hr('13. FULL TEXT SEARCH FOR "bloomerang" ACROSS ALL TABLES');
  const allTables = await pool.request().query(
    "SELECT s.name AS schema_name, t.name AS table_name, c.name AS column_name " +
    "FROM sys.columns c " +
    "JOIN sys.tables t ON c.object_id = t.object_id " +
    "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
    "JOIN sys.types tp ON c.user_type_id = tp.user_type_id " +
    "WHERE tp.name IN ('varchar', 'nvarchar') " +
    "AND s.name NOT IN ('sys', 'INFORMATION_SCHEMA') " +
    "ORDER BY s.name, t.name, c.name");

  const tableGroups = {};
  for (const r of allTables.recordset) {
    const key = r.schema_name + '.' + r.table_name;
    if (!tableGroups[key]) tableGroups[key] = { schema: r.schema_name, table: r.table_name, cols: [] };
    tableGroups[key].cols.push(r.column_name);
  }

  let foundAny = false;
  for (const key of Object.keys(tableGroups)) {
    const info = tableGroups[key];
    const conditions = info.cols.map(function(c) { return '[' + c + '] LIKE \'%bloomerang%\''; }).join(' OR ');
    try {
      const result = await pool.request().query(
        "SELECT TOP 1 1 AS found FROM [" + info.schema + "].[" + info.table + "] WHERE " + conditions);
      if (result.recordset.length > 0) {
        foundAny = true;
        const countResult = await pool.request().query(
          "SELECT COUNT(*) AS cnt FROM [" + info.schema + "].[" + info.table + "] WHERE " + conditions);
        console.log('  FOUND in ' + key + ': ' + countResult.recordset[0].cnt + ' rows');
      }
    } catch (e) { /* skip errors */ }
  }
  if (!foundAny) {
    console.log('  No tables contain the string "bloomerang" in any varchar column.');
  }

  // SUMMARY
  hr('AUDIT COMPLETE');
  console.log('  Review the output above for a complete picture of Bloomerang data in sozov2.');

  await pool.close();
  process.exit(0);
}

main().catch(function(err) {
  console.error('FATAL:', err.message);
  process.exit(1);
});
