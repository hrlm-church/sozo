const fs = require('fs');
const readline = require('readline');

const filePath = process.argv[2] || '/tmp/sozo-tag-check/keap_contact_full.csv';
const outJson = process.argv[3] || 'reports/catalog/keap_tag_analysis.json';
const outMd = process.argv[4] || 'reports/catalog/KEAP_TAG_ANALYSIS.md';

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
  return out.map((v) => v.replace(/^\uFEFF/, '').trim());
};

const normalizeKey = (s) => String(s || '').toLowerCase().replace(/[^a-z0-9]/g, '');
const splitMulti = (s) => String(s || '')
  .split(/[,;|]/)
  .map((x) => x.trim())
  .filter(Boolean);

(async () => {
  if (!fs.existsSync(filePath)) {
    throw new Error(`File not found: ${filePath}`);
  }

  const rl = readline.createInterface({ input: fs.createReadStream(filePath), crlfDelay: Infinity });

  let header = null;
  let idx = { tagIds: -1, tags: -1, tagCategoryIds: -1, tagCategories: -1 };
  let totalRows = 0;
  let rowsWithAnyTag = 0;
  let rowsWithTags = 0;
  let rowsWithTagIds = 0;
  let rowsWithTagCategories = 0;
  let rowsWithTagCategoryIds = 0;

  const tagFreq = new Map();
  const tagCategoryFreq = new Map();
  const sampleRows = [];

  for await (const raw of rl) {
    const line = String(raw || '');
    if (!header) {
      header = splitCsvLine(line);
      const norm = header.map(normalizeKey);
      idx.tagIds = norm.indexOf('tagids');
      idx.tags = norm.indexOf('tags');
      idx.tagCategoryIds = norm.indexOf('tagcategoryids');
      idx.tagCategories = norm.indexOf('tagcategories');
      continue;
    }

    const cells = splitCsvLine(line);
    totalRows += 1;

    const tagIds = idx.tagIds >= 0 ? (cells[idx.tagIds] || '').trim() : '';
    const tags = idx.tags >= 0 ? (cells[idx.tags] || '').trim() : '';
    const tagCategoryIds = idx.tagCategoryIds >= 0 ? (cells[idx.tagCategoryIds] || '').trim() : '';
    const tagCategories = idx.tagCategories >= 0 ? (cells[idx.tagCategories] || '').trim() : '';

    const hasAny = Boolean(tagIds || tags || tagCategoryIds || tagCategories);
    if (hasAny) rowsWithAnyTag += 1;
    if (tags) rowsWithTags += 1;
    if (tagIds) rowsWithTagIds += 1;
    if (tagCategoryIds) rowsWithTagCategoryIds += 1;
    if (tagCategories) rowsWithTagCategories += 1;

    if (tags) {
      for (const t of splitMulti(tags)) {
        tagFreq.set(t, (tagFreq.get(t) || 0) + 1);
      }
    }
    if (tagCategories) {
      for (const c of splitMulti(tagCategories)) {
        tagCategoryFreq.set(c, (tagCategoryFreq.get(c) || 0) + 1);
      }
    }

    if (hasAny && sampleRows.length < 25) {
      sampleRows.push({ tagIds, tags, tagCategoryIds, tagCategories });
    }
  }

  const topTags = [...tagFreq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 50).map(([value, count]) => ({ value, count }));
  const topCategories = [...tagCategoryFreq.entries()].sort((a, b) => b[1] - a[1]).slice(0, 50).map(([value, count]) => ({ value, count }));

  const result = {
    filePath,
    totalRows,
    columnsDetected: idx,
    coverage: {
      rowsWithAnyTag,
      rowsWithTags,
      rowsWithTagIds,
      rowsWithTagCategories,
      rowsWithTagCategoryIds,
      pctAnyTag: totalRows ? Number(((rowsWithAnyTag / totalRows) * 100).toFixed(2)) : 0,
      pctTags: totalRows ? Number(((rowsWithTags / totalRows) * 100).toFixed(2)) : 0,
      pctTagCategories: totalRows ? Number(((rowsWithTagCategories / totalRows) * 100).toFixed(2)) : 0,
    },
    distinct: {
      tags: tagFreq.size,
      tagCategories: tagCategoryFreq.size,
    },
    topTags,
    topCategories,
    sampleRows,
  };

  fs.mkdirSync(require('path').dirname(outJson), { recursive: true });
  fs.writeFileSync(outJson, JSON.stringify(result, null, 2));

  let md = '# Keap Tag Analysis\n\n';
  md += `File: \`${filePath}\`\n\n`;
  md += `Rows analyzed: **${totalRows}**\n\n`;
  md += '## Coverage\n';
  md += `- Rows with any tag field populated: **${rowsWithAnyTag}** (${result.coverage.pctAnyTag}%)\n`;
  md += `- Rows with Tags populated: **${rowsWithTags}** (${result.coverage.pctTags}%)\n`;
  md += `- Rows with Tag Categories populated: **${rowsWithTagCategories}** (${result.coverage.pctTagCategories}%)\n`;
  md += `- Rows with Tag Ids populated: **${rowsWithTagIds}**\n`;
  md += `- Rows with Tag Category Ids populated: **${rowsWithTagCategoryIds}**\n\n`;

  md += '## Distinct values\n';
  md += `- Distinct Tags: **${tagFreq.size}**\n`;
  md += `- Distinct Tag Categories: **${tagCategoryFreq.size}**\n\n`;

  md += '## Top Tags\n';
  if (topTags.length === 0) {
    md += 'No populated tag values found.\n\n';
  } else {
    md += '| Tag | Count |\n|---|---:|\n';
    for (const t of topTags.slice(0, 25)) md += `| ${t.value.replace(/\|/g, '/')} | ${t.count} |\n`;
    md += '\n';
  }

  md += '## Top Tag Categories\n';
  if (topCategories.length === 0) {
    md += 'No populated tag category values found.\n';
  } else {
    md += '| Category | Count |\n|---|---:|\n';
    for (const c of topCategories.slice(0, 25)) md += `| ${c.value.replace(/\|/g, '/')} | ${c.count} |\n`;
  }

  fs.writeFileSync(outMd, md);
  console.log(JSON.stringify({ outJson, outMd, totalRows, rowsWithAnyTag, distinctTags: tagFreq.size, distinctCategories: tagCategoryFreq.size }, null, 2));
})();
