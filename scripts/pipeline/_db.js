const fs = require('fs');
const path = require('path');
const sql = require('mssql');

function loadEnvFile(envPath) {
  const resolved = envPath || path.join(process.cwd(), '.env.local');
  if (!fs.existsSync(resolved)) return;
  const lines = fs.readFileSync(resolved, 'utf8').split(/\r?\n/);
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eq = trimmed.indexOf('=');
    if (eq <= 0) continue;
    const key = trimmed.slice(0, eq).trim();
    let value = trimmed.slice(eq + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (!(key in process.env)) process.env[key] = value;
  }
}

function getConfig() {
  loadEnvFile();
  const required = ['SOZO_SQL_HOST', 'SOZO_SQL_DB', 'SOZO_SQL_USER', 'SOZO_SQL_PASSWORD'];
  const missing = required.filter((k) => !process.env[k]);
  if (missing.length) {
    throw new Error(`Missing SQL env vars: ${missing.join(', ')}`);
  }

  return {
    server: process.env.SOZO_SQL_HOST,
    database: process.env.SOZO_SQL_DB,
    user: process.env.SOZO_SQL_USER,
    password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000,
    requestTimeout: 300000,
    options: {
      encrypt: true,
      trustServerCertificate: false,
    },
    pool: {
      max: 5,
      min: 0,
      idleTimeoutMillis: 5000,
    },
  };
}

async function withDb(fn) {
  const pool = await sql.connect(getConfig());
  try {
    return await fn(pool);
  } finally {
    await pool.close();
  }
}

async function runSqlFile(filePath) {
  const full = path.isAbsolute(filePath) ? filePath : path.join(process.cwd(), filePath);
  const text = fs.readFileSync(full, 'utf8');
  return withDb(async (pool) => {
    const statements = text
      .split(/;\s*(?:\r?\n|$)/)
      .map((s) => s.trim())
      .filter(Boolean);

    for (const stmt of statements) {
      await pool.request().batch(stmt);
    }
  });
}

module.exports = {
  withDb,
  runSqlFile,
  loadEnvFile,
};
