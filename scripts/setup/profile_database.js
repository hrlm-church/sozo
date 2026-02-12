#!/usr/bin/env node
const path = require('path'), fs = require('fs'), sql = require('mssql');
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}
loadEnv();
(async () => {
  const pool = await sql.connect({
    server: process.env.SOZO_SQL_HOST, database: process.env.SOZO_SQL_DB,
    user: process.env.SOZO_SQL_USER, password: process.env.SOZO_SQL_PASSWORD,
    connectionTimeout: 30000, requestTimeout: 120000,
    options: { encrypt: true, trustServerCertificate: false }
  });
  const q = async (s) => (await pool.request().query(s)).recordset;

  // ALL table schemas
  const allCols = await q(`
    SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA IN ('serving','giving','commerce','engagement','event','person','household','meta','intel')
    ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
  `);
  const byTable = {};
  for (const c of allCols) {
    const key = c.TABLE_SCHEMA + '.' + c.TABLE_NAME;
    if (!byTable[key]) byTable[key] = [];
    byTable[key].push(c.COLUMN_NAME + ' ' + c.DATA_TYPE);
  }
  console.log('=== COMPLETE SCHEMA ===');
  for (const [table, cols] of Object.entries(byTable)) {
    console.log(table + ': ' + cols.join(', '));
  }

  // Funds
  const funds = await q('SELECT TOP (10) fund, COUNT(*) AS cnt, SUM(amount) AS total FROM giving.donation GROUP BY fund ORDER BY total DESC');
  console.log('\nTOP FUNDS:');
  for (const r of funds) console.log(' ', r.fund || '(null)', ':', r.cnt, '/ $' + Math.round(r.total || 0));

  // Subscription product_names
  const plans = await q('SELECT TOP (10) product_name, COUNT(*) AS cnt FROM commerce.subscription GROUP BY product_name ORDER BY cnt DESC');
  console.log('\nTOP SUBSCRIPTION PRODUCTS:');
  for (const r of plans) console.log(' ', r.product_name, ':', r.cnt);

  // Communication channels
  const ch = await q('SELECT channel, COUNT(*) AS cnt FROM engagement.communication GROUP BY channel ORDER BY cnt DESC');
  console.log('\nCOMMUNICATION CHANNELS:');
  for (const r of ch) console.log(' ', r.channel, ':', r.cnt);

  // Household giving trends
  const gt = await q('SELECT giving_trend, COUNT(*) AS cnt FROM serving.household_360 GROUP BY giving_trend ORDER BY cnt DESC');
  console.log('\nHOUSEHOLD GIVING TRENDS:');
  for (const r of gt) console.log(' ', r.giving_trend || '(null)', ':', r.cnt);

  // Payment methods
  const pm = await q('SELECT TOP (8) payment_method, COUNT(*) AS cnt FROM giving.donation WHERE payment_method IS NOT NULL GROUP BY payment_method ORDER BY cnt DESC');
  console.log('\nPAYMENT METHODS:');
  for (const r of pm) console.log(' ', r.payment_method, ':', r.cnt);

  // Giving stats
  const gs = await q('SELECT COUNT(*) AS donors, SUM(lifetime_giving) AS total, AVG(lifetime_giving) AS avg_gift, MAX(lifetime_giving) AS max_giving FROM serving.person_360 WHERE donation_count > 0');
  console.log('\nGIVING STATS:', JSON.stringify(gs[0]));

  // Activity types
  const at2 = await q('SELECT TOP (10) activity_type, COUNT(*) AS cnt FROM engagement.activity GROUP BY activity_type ORDER BY cnt DESC');
  console.log('\nACTIVITY TYPES:');
  for (const r of at2) console.log(' ', r.activity_type, ':', r.cnt);

  // Appeals
  const ap = await q('SELECT TOP (8) appeal, COUNT(*) AS cnt FROM giving.donation WHERE appeal IS NOT NULL GROUP BY appeal ORDER BY cnt DESC');
  console.log('\nTOP APPEALS:');
  for (const r of ap) console.log(' ', r.appeal, ':', r.cnt);

  // Tag groups
  const tg = await q('SELECT TOP (10) tag_group, COUNT(*) AS cnt FROM engagement.tag GROUP BY tag_group ORDER BY cnt DESC');
  console.log('\nTOP TAG GROUPS:');
  for (const r of tg) console.log(' ', r.tag_group || '(null)', ':', r.cnt);

  // Subscription statuses
  const ss = await q('SELECT status, COUNT(*) AS cnt FROM commerce.subscription GROUP BY status ORDER BY cnt DESC');
  console.log('\nSUBSCRIPTION STATUSES:');
  for (const r of ss) console.log(' ', r.status, ':', r.cnt);

  // Order statuses
  const os = await q('SELECT TOP (5) status, COUNT(*) AS cnt FROM commerce.[order] GROUP BY status ORDER BY cnt DESC');
  console.log('\nORDER STATUSES:');
  for (const r of os) console.log(' ', r.status, ':', r.cnt);

  await pool.close();
})();
