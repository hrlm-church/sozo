#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
source .env.local

mkdir -p reports/catalog /tmp/sozo-audit

# Build a deterministic CSV blob list from raw inventory
node - <<'NODE'
const fs = require('fs');
const blobs = JSON.parse(fs.readFileSync('reports/catalog/raw_blobs_full.json','utf8'));
const csv = blobs
  .filter(b => /\.csv$/i.test(b.name || ''))
  .map(b => ({
    name: b.name,
    source_system: (b.name || '').split('/')[0] || '(root)',
    size_bytes: b.properties?.contentLength || 0,
    last_modified: b.properties?.lastModified || null,
    etag: b.properties?.etag || null,
  }))
  .sort((a,b)=>a.name.localeCompare(b.name));
fs.writeFileSync('reports/catalog/raw_csv_inventory.json', JSON.stringify(csv,null,2));
NODE

# Header/profile output
out="reports/catalog/raw_csv_profile.tsv"
echo -e "source_system\tblob_name\tsize_bytes\tlast_modified\theader_columns\tdomain_tags" > "$out"

node - <<'NODE' > /tmp/sozo-audit/csv_rows.jsonl
const fs = require('fs');
const rows = JSON.parse(fs.readFileSync('reports/catalog/raw_csv_inventory.json','utf8'));
for (const r of rows) {
  console.log(JSON.stringify(r));
}
NODE

while IFS= read -r line; do
  source_system=$(echo "$line" | node -p "const o=JSON.parse(require('fs').readFileSync(0,'utf8')); o.source_system")
  blob_name=$(echo "$line" | node -p "const o=JSON.parse(require('fs').readFileSync(0,'utf8')); o.name")
  size_bytes=$(echo "$line" | node -p "const o=JSON.parse(require('fs').readFileSync(0,'utf8')); o.size_bytes")
  last_modified=$(echo "$line" | node -p "const o=JSON.parse(require('fs').readFileSync(0,'utf8')); o.last_modified || ''")

  tmp_file="/tmp/sozo-audit/header.csv"
  az storage blob download \
    --account-name "$SOZO_STORAGE_ACCOUNT" \
    --account-key "$SOZO_STORAGE_ACCOUNT_KEY" \
    --container-name "$SOZO_STORAGE_RAW_CONTAINER" \
    --name "$blob_name" \
    --file "$tmp_file" \
    --overwrite \
    --start-range 0 \
    --end-range 262143 >/dev/null

  header=$(head -n 1 "$tmp_file" | tr '\r\n' ' ' | sed 's/\t/ /g' | sed 's/""/"/g')
  fname_lc=$(echo "$blob_name" | tr '[:upper:]' '[:lower:]')
  header_lc=$(echo "$header" | tr '[:upper:]' '[:lower:]')

  tags=()
  [[ "$fname_lc" == *"event"* || "$fname_lc" == *"ticket"* || "$fname_lc" == *"attendee"* || "$fname_lc" == *"registration"* ]] && tags+=("event_ticket")
  [[ "$fname_lc" == *"donat"* || "$fname_lc" == *"gift"* || "$header_lc" == *"donation"* ]] && tags+=("donation")
  [[ "$fname_lc" == *"invoice"* || "$fname_lc" == *"payment"* || "$fname_lc" == *"order"* || "$fname_lc" == *"stripe"* ]] && tags+=("commerce_payment")
  [[ "$fname_lc" == *"contact"* || "$fname_lc" == *"company"* || "$header_lc" == *"email"* ]] && tags+=("crm_identity")
  [[ "$fname_lc" == *"note"* || "$fname_lc" == *"activity"* || "$fname_lc" == *"engage"* ]] && tags+=("engagement")

  domain_tags=$(IFS=','; echo "${tags[*]:-uncategorized}")

  # Escape tabs in header for TSV safety
  header_safe=$(echo "$header" | tr '\t' ' ')

  echo -e "${source_system}\t${blob_name}\t${size_bytes}\t${last_modified}\t${header_safe}\t${domain_tags}" >> "$out"
done < /tmp/sozo-audit/csv_rows.jsonl

# Summary markdown
node - <<'NODE'
const fs = require('fs');
const blobs = JSON.parse(fs.readFileSync('reports/catalog/raw_blobs_full.json','utf8'));
const csvRows = fs.readFileSync('reports/catalog/raw_csv_profile.tsv','utf8').trim().split(/\r?\n/).slice(1).map(line=>{
  const parts = line.split('\t');
  return {
    source_system: parts[0],
    blob_name: parts[1],
    size_bytes: Number(parts[2]||0),
    last_modified: parts[3],
    header_columns: parts[4]||'',
    domain_tags: (parts[5]||'').split(',').filter(Boolean),
  };
});

const byTop = {};
for (const b of blobs) {
  const top = (b.name||'').split('/')[0] || '(root)';
  byTop[top] = byTop[top] || { files: 0, bytes: 0 };
  byTop[top].files += 1;
  byTop[top].bytes += Number(b.properties?.contentLength || 0);
}

const bySourceCsv = {};
for (const r of csvRows) {
  bySourceCsv[r.source_system] = bySourceCsv[r.source_system] || { csv_files: 0, bytes: 0, tags: {} };
  bySourceCsv[r.source_system].csv_files += 1;
  bySourceCsv[r.source_system].bytes += r.size_bytes;
  for (const t of r.domain_tags) {
    bySourceCsv[r.source_system].tags[t] = (bySourceCsv[r.source_system].tags[t] || 0) + 1;
  }
}

const eventRows = csvRows.filter(r => r.domain_tags.includes('event_ticket'));

let md = '';
md += '# Raw Container Full Data Catalog\n\n';
md += `Generated: ${new Date().toISOString()}\n\n`;
md += `Total blobs in raw: **${blobs.length}**\n\n`;
md += '## Top-level folder inventory\n';
md += '| Folder | Files | Size (MB) |\n|---|---:|---:|\n';
for (const [k,v] of Object.entries(byTop).sort((a,b)=>a[0].localeCompare(b[0]))) {
  md += `| ${k} | ${v.files} | ${(v.bytes/1024/1024).toFixed(2)} |\n`;
}

md += '\n## Business CSV catalog summary\n';
md += `CSV files profiled: **${csvRows.length}** (header-level profiling)\n\n`;
md += '| Source | CSV files | Size (MB) | Domain tags |\n|---|---:|---:|---|\n';
for (const [k,v] of Object.entries(bySourceCsv).sort((a,b)=>a[0].localeCompare(b[0]))) {
  const tags = Object.entries(v.tags).sort((a,b)=>b[1]-a[1]).map(([t,c])=>`${t}:${c}`).join(', ');
  md += `| ${k} | ${v.csv_files} | ${(v.bytes/1024/1024).toFixed(2)} | ${tags || 'n/a'} |\n`;
}

md += '\n## Event/Ticket candidate files\n';
if (eventRows.length === 0) {
  md += 'No event/ticket files detected by filename/header heuristics.\n';
} else {
  md += '| Source | File | Size (MB) | Header preview |\n|---|---|---:|---|\n';
  for (const r of eventRows) {
    md += `| ${r.source_system} | ${r.blob_name} | ${(r.size_bytes/1024/1024).toFixed(2)} | ${r.header_columns.replace(/\|/g,'/').slice(0,140)} |\n`;
  }
}

md += '\n## Output files\n';
md += '- `reports/catalog/raw_blobs_full.json` (full blob inventory)\n';
md += '- `reports/catalog/raw_csv_inventory.json` (all CSV blob metadata)\n';
md += '- `reports/catalog/raw_csv_profile.tsv` (file-level profile with header + domain tags)\n';
md += '- `reports/catalog/RAW_DATA_CATALOG.md` (this summary)\n';

fs.writeFileSync('reports/catalog/RAW_DATA_CATALOG.md', md);
console.log('catalog markdown created');
NODE

echo "Audit complete: reports/catalog/RAW_DATA_CATALOG.md"
