const { withDb } = require('./_db');

const pick = (obj, keys) => {
  for (const key of keys) {
    const found = Object.keys(obj).find((k) => k.toLowerCase() === key.toLowerCase());
    if (found && obj[found] !== undefined && obj[found] !== null && String(obj[found]).trim() !== '') {
      return String(obj[found]).trim();
    }
  }
  return null;
};

const parseAmount = (v) => {
  if (!v) return null;
  const n = Number(String(v).replace(/[$,]/g, ''));
  return Number.isFinite(n) ? n : null;
};

const parseDate = (v) => {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
};

const clip = (value, max) => {
  if (value == null) return value;
  const text = String(value);
  return text.length > max ? text.slice(0, max) : text;
};

async function main() {
  await withDb(async (pool) => {
    const rs = await pool.request().query(`
SELECT batch_id, source_system, source_record_id, file_path, record_json
FROM bronze.raw_record;
`);

    let personCount = 0;
    let txCount = 0;
    let engCount = 0;

    for (const row of rs.recordset) {
      const payload = JSON.parse(row.record_json);
      const file = String(row.file_path).toLowerCase();

      const fullName = pick(payload, ['full_name', 'name', 'contactname', 'first_name']);
      const email = pick(payload, ['email', 'emailaddress', 'contact_email']);
      const phone = pick(payload, ['phone', 'mobile', 'phone_number']);
      const address = pick(payload, ['address', 'address1', 'street']);
      const city = pick(payload, ['city']);
      const state = pick(payload, ['state', 'province']);
      const postal = pick(payload, ['postal', 'postal_code', 'zip', 'zipcode']);

      const amount = parseAmount(pick(payload, ['amount', 'total', 'value', 'payment_amount']));
      const transactionRef = pick(payload, ['invoiceid', 'transactionid', 'orderid', 'id']);
      const status = pick(payload, ['status', 'payment_status']);
      const eventTs = parseDate(pick(payload, ['date', 'created_at', 'updated_at', 'payment_date']));

      const engagementType = pick(payload, ['type', 'activity_type', 'note_type']);
      const subject = clip(pick(payload, ['subject', 'title']), 250);
      const notes = clip(pick(payload, ['notes', 'description', 'content']), 1900);

      const flags = [];
      if (!fullName && !email && !phone) flags.push('missing_person_identity');
      if (/(invoice|payment|order|transaction|stripe)/.test(file) && amount == null) flags.push('missing_amount');

      if (fullName || email || phone) {
        // eslint-disable-next-line no-await-in-loop
        await pool.request()
          .input('batch_id', row.batch_id)
          .input('source_system', row.source_system)
          .input('source_record_id', row.source_record_id)
          .input('file_path', row.file_path)
          .input('full_name', fullName)
          .input('email', email)
          .input('phone', phone)
          .input('address_line1', address)
          .input('city', city)
          .input('state', state)
          .input('postal_code', postal)
          .input('quality_flags', JSON.stringify(flags))
          .batch(`
INSERT INTO silver.person_source(batch_id,source_system,source_record_id,file_path,full_name,email,phone,address_line1,city,state,postal_code,quality_flags)
VALUES(@batch_id,@source_system,@source_record_id,@file_path,@full_name,@email,@phone,@address_line1,@city,@state,@postal_code,@quality_flags);
`);
        personCount += 1;
      }

      if (/(invoice|payment|order|transaction|stripe|donor)/.test(file) || amount != null) {
        // eslint-disable-next-line no-await-in-loop
        await pool.request()
          .input('batch_id', row.batch_id)
          .input('source_system', row.source_system)
          .input('source_record_id', row.source_record_id)
          .input('file_path', row.file_path)
          .input('person_ref', pick(payload, ['contactid', 'personid', 'customerid', 'email']))
          .input('transaction_ref', transactionRef)
          .input('amount', amount)
          .input('currency', pick(payload, ['currency']) || 'USD')
          .input('status', status)
          .input('transaction_ts', eventTs)
          .input('quality_flags', JSON.stringify(flags))
          .batch(`
INSERT INTO silver.transaction_source(batch_id,source_system,source_record_id,file_path,person_ref,transaction_ref,amount,currency,status,transaction_ts,quality_flags)
VALUES(@batch_id,@source_system,@source_record_id,@file_path,@person_ref,@transaction_ref,@amount,@currency,@status,@transaction_ts,@quality_flags);
`);
        txCount += 1;
      }

      if (/(note|engage|activity|campaign|message)/.test(file) || engagementType || subject || notes) {
        // eslint-disable-next-line no-await-in-loop
        await pool.request()
          .input('batch_id', row.batch_id)
          .input('source_system', row.source_system)
          .input('source_record_id', row.source_record_id)
          .input('file_path', row.file_path)
          .input('person_ref', pick(payload, ['contactid', 'personid', 'email']))
          .input('engagement_type', engagementType)
          .input('subject', subject)
          .input('occurred_at', eventTs)
          .input('notes', notes)
          .input('quality_flags', JSON.stringify(flags))
          .batch(`
INSERT INTO silver.engagement_source(batch_id,source_system,source_record_id,file_path,person_ref,engagement_type,subject,occurred_at,notes,quality_flags)
VALUES(@batch_id,@source_system,@source_record_id,@file_path,@person_ref,@engagement_type,@subject,@occurred_at,@notes,@quality_flags);
`);
        engCount += 1;
      }
    }

    console.log(`OK: silver transforms complete (person=${personCount}, transaction=${txCount}, engagement=${engCount})`);
  });
}

main().catch((err) => {
  console.error('ERROR:', err.message);
  process.exit(1);
});
