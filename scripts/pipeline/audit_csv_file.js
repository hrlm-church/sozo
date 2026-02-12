const fs = require('fs');
const path = require('path');
const readline = require('readline');

const filePath = process.argv[2];
const sourceSystem = process.argv[3] || '';
const blobName = process.argv[4] || path.basename(filePath || '');

if (!filePath || !fs.existsSync(filePath)) {
  console.error('Missing CSV file path');
  process.exit(1);
}

const splitCsvLine = (line) => {
  const out = [];
  let cur = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === ',' && !inQuotes) {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
};

const toNum = (v) => {
  if (v == null) return null;
  const n = Number(String(v).replace(/[$,\s]/g, ''));
  return Number.isFinite(n) ? n : null;
};

(async () => {
  const stat = fs.statSync(filePath);
  const stream = fs.createReadStream(filePath);
  const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

  let headers = null;
  let rowCount = 0;
  let emptyRowCount = 0;
  let totalCells = 0;
  let emptyCells = 0;
  const nullByCol = [];

  let idCol = -1;
  const seenIds = new Set();
  let duplicateIdRows = 0;

  const eventColIdx = [];
  let eventRows = 0;

  let amountCol = -1;
  let amountRows = 0;
  let amountSum = 0;

  const dateColIdx = [];
  let minDate = null;
  let maxDate = null;

  for await (const rawLine of rl) {
    const line = String(rawLine || '').replace(/^\uFEFF/, '');
    if (headers == null) {
      headers = splitCsvLine(line).map((h, i) => (h || `col_${i + 1}`).trim().replace(/^"|"$/g, ''));

      for (let i = 0; i < headers.length; i += 1) {
        nullByCol.push(0);
        const h = headers[i].toLowerCase();
        if (idCol < 0 && (h === 'id' || h.endsWith('id') || h.includes('contactid') || h.includes('customerid'))) {
          idCol = i;
        }
        if (amountCol < 0 && (h.includes('amount') || h.includes('total') || h.includes('value'))) {
          amountCol = i;
        }
        if (/date|time|created|updated/.test(h)) {
          dateColIdx.push(i);
        }
        if (/event|ticket|attend|registration|venue|promoter|coupon/.test(h)) {
          eventColIdx.push(i);
        }
      }
      continue;
    }

    const cells = splitCsvLine(line);
    rowCount += 1;

    let rowNonEmpty = 0;
    let rowEventHit = false;

    for (let i = 0; i < headers.length; i += 1) {
      const v = (cells[i] ?? '').trim();
      totalCells += 1;
      if (!v || v.toLowerCase() === 'null' || v.toLowerCase() === 'n/a') {
        emptyCells += 1;
        nullByCol[i] += 1;
      } else {
        rowNonEmpty += 1;
      }

      if (eventColIdx.includes(i) && v) {
        rowEventHit = true;
      }

      if (dateColIdx.includes(i) && v) {
        const d = new Date(v);
        if (!Number.isNaN(d.getTime())) {
          const iso = d.toISOString();
          if (!minDate || iso < minDate) minDate = iso;
          if (!maxDate || iso > maxDate) maxDate = iso;
        }
      }
    }

    if (rowNonEmpty === 0) emptyRowCount += 1;
    if (rowEventHit) eventRows += 1;

    if (idCol >= 0) {
      const id = (cells[idCol] ?? '').trim();
      if (id) {
        if (seenIds.has(id)) duplicateIdRows += 1;
        seenIds.add(id);
      }
    }

    if (amountCol >= 0) {
      const n = toNum(cells[amountCol]);
      if (n != null) {
        amountRows += 1;
        amountSum += n;
      }
    }
  }

  if (!headers) headers = [];

  const topNullColumns = headers
    .map((h, i) => ({
      column: h,
      null_count: nullByCol[i] || 0,
      null_pct: rowCount > 0 ? Number((((nullByCol[i] || 0) / rowCount) * 100).toFixed(2)) : 0,
    }))
    .sort((a, b) => b.null_pct - a.null_pct)
    .slice(0, 10);

  const out = {
    source_system: sourceSystem,
    blob_name: blobName,
    file_size_bytes: stat.size,
    header_count: headers.length,
    row_count: rowCount,
    empty_row_count: emptyRowCount,
    completeness_pct: totalCells > 0 ? Number((((totalCells - emptyCells) / totalCells) * 100).toFixed(2)) : 0,
    duplicate_id_rows: duplicateIdRows,
    event_signal_rows: eventRows,
    amount_rows: amountRows,
    amount_sum: Number(amountSum.toFixed(2)),
    min_date: minDate,
    max_date: maxDate,
    top_null_columns: topNullColumns,
  };

  process.stdout.write(JSON.stringify(out));
})();
