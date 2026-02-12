#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
source .env.local

mkdir -p reports/catalog /tmp/sozo-audit/full

# Ensure blob inventory exists
if [[ ! -f reports/catalog/raw_blobs_full.json ]]; then
  az storage blob list \
    --account-name "$SOZO_STORAGE_ACCOUNT" \
    --account-key "$SOZO_STORAGE_ACCOUNT_KEY" \
    --container-name "$SOZO_STORAGE_RAW_CONTAINER" \
    --num-results "*" -o json > reports/catalog/raw_blobs_full.json
fi

# Prepare CSV list
node - <<'NODE'
const fs=require('fs');
const blobs=JSON.parse(fs.readFileSync('reports/catalog/raw_blobs_full.json','utf8'));
const csv=blobs.filter(b=>/\.csv$/i.test(b.name||'')).map(b=>([
  (b.name||'').split('/')[0]||'(root)',
  b.name,
  String(b.properties?.contentLength||0),
  b.properties?.lastModified||''
].join('\t')));
fs.writeFileSync('/tmp/sozo-audit/csv_list.tsv', csv.join('\n')+'\n');
NODE

# Audit all file metadata
node - <<'NODE'
const fs=require('fs');
const blobs=JSON.parse(fs.readFileSync('reports/catalog/raw_blobs_full.json','utf8'));
const out=blobs.map(b=>({
  blob_name:b.name,
  source_system:(b.name||'').split('/')[0]||'(root)',
  size_bytes:Number(b.properties?.contentLength||0),
  last_modified:b.properties?.lastModified||null,
  content_type:b.properties?.contentSettings?.contentType||null,
  extension:(b.name||'').includes('.')?'.'+(b.name||'').split('.').pop().toLowerCase():'',
}));
fs.writeFileSync('reports/catalog/raw_all_files_audit.json', JSON.stringify(out,null,2));
NODE

# Row-level audit for every CSV
: > reports/catalog/raw_csv_row_audit.jsonl

while IFS=$'\t' read -r source_system blob_name size_bytes last_modified; do
  [[ -z "${blob_name:-}" ]] && continue

  safe_name=$(echo "$blob_name" | sed 's#[/ ]#_#g')
  local_file="/tmp/sozo-audit/full/${safe_name}"

  az storage blob download \
    --account-name "$SOZO_STORAGE_ACCOUNT" \
    --account-key "$SOZO_STORAGE_ACCOUNT_KEY" \
    --container-name "$SOZO_STORAGE_RAW_CONTAINER" \
    --name "$blob_name" \
    --file "$local_file" \
    --overwrite >/dev/null

  node scripts/pipeline/audit_csv_file.js "$local_file" "$source_system" "$blob_name" >> reports/catalog/raw_csv_row_audit.jsonl
  echo >> reports/catalog/raw_csv_row_audit.jsonl
done < /tmp/sozo-audit/csv_list.tsv

# Build markdown summary
node - <<'NODE'
const fs=require('fs');
const allFiles=JSON.parse(fs.readFileSync('reports/catalog/raw_all_files_audit.json','utf8'));
const csvRows=fs.readFileSync('reports/catalog/raw_csv_row_audit.jsonl','utf8').trim().split(/\r?\n/).filter(Boolean).map(l=>JSON.parse(l));

const bySource={};
for(const f of allFiles){
  const s=f.source_system;
  bySource[s]=bySource[s]||{files:0,bytes:0,csv_files:0,csv_rows:0,event_rows:0,amount_sum:0};
  bySource[s].files+=1;
  bySource[s].bytes+=f.size_bytes;
}
for(const r of csvRows){
  const s=r.source_system;
  bySource[s]=bySource[s]||{files:0,bytes:0,csv_files:0,csv_rows:0,event_rows:0,amount_sum:0};
  bySource[s].csv_files+=1;
  bySource[s].csv_rows+=Number(r.row_count||0);
  bySource[s].event_rows+=Number(r.event_signal_rows||0);
  bySource[s].amount_sum+=Number(r.amount_sum||0);
}

const topLargeCsv=[...csvRows].sort((a,b)=>b.file_size_bytes-a.file_size_bytes).slice(0,15);
const highNull=[...csvRows]
  .map(r=>({source_system:r.source_system,blob_name:r.blob_name,worst_null_pct:r.top_null_columns?.[0]?.null_pct||0,worst_null_col:r.top_null_columns?.[0]?.column||''}))
  .sort((a,b)=>b.worst_null_pct-a.worst_null_pct)
  .slice(0,15);
const eventFiles=csvRows.filter(r=>Number(r.event_signal_rows||0)>0);

let md='';
md+='# Raw Container Full File Audit\n\n';
md+=`Generated: ${new Date().toISOString()}\n\n`;
md+=`Total blobs audited: **${allFiles.length}**\n\n`;
md+=`Total CSVs row-profiled: **${csvRows.length}**\n\n`;

md+='## Source summary\n';
md+='| Source | Files | Size (MB) | CSV files | CSV rows | Event-signal rows | Amount sum (raw) |\n|---|---:|---:|---:|---:|---:|---:|\n';
for(const [s,v] of Object.entries(bySource).sort((a,b)=>a[0].localeCompare(b[0]))){
  md+=`| ${s} | ${v.files} | ${(v.bytes/1024/1024).toFixed(2)} | ${v.csv_files} | ${v.csv_rows} | ${v.event_rows} | ${v.amount_sum.toFixed(2)} |\n`;
}

md+='\n## Largest CSV files\n';
md+='| Source | File | Size (MB) | Rows | Completeness % | Duplicate ID rows |\n|---|---|---:|---:|---:|---:|\n';
for(const r of topLargeCsv){
  md+=`| ${r.source_system} | ${r.blob_name} | ${(r.file_size_bytes/1024/1024).toFixed(2)} | ${r.row_count} | ${r.completeness_pct} | ${r.duplicate_id_rows} |\n`;
}

md+='\n## Event/ticket active files (row-level)\n';
if(eventFiles.length===0){
  md+='No files had row-level event/ticket signal data populated.\n';
}else{
  md+='| Source | File | Event-signal rows | Date min | Date max |\n|---|---|---:|---|---|\n';
  for(const r of eventFiles.sort((a,b)=>b.event_signal_rows-a.event_signal_rows)){
    md+=`| ${r.source_system} | ${r.blob_name} | ${r.event_signal_rows} | ${r.min_date||''} | ${r.max_date||''} |\n`;
  }
}

md+='\n## Highest-nullness files (top column)\n';
md+='| Source | File | Top null column | Null % |\n|---|---|---|---:|\n';
for(const r of highNull){
  md+=`| ${r.source_system} | ${r.blob_name} | ${r.worst_null_col} | ${r.worst_null_pct} |\n`;
}

md+='\n## Output files\n';
md+='- `reports/catalog/raw_all_files_audit.json` (all blobs metadata)\n';
md+='- `reports/catalog/raw_csv_row_audit.jsonl` (row-level CSV audit records)\n';
md+='- `reports/catalog/RAW_FULL_AUDIT.md` (this report)\n';

fs.writeFileSync('reports/catalog/RAW_FULL_AUDIT.md', md);
console.log('full audit markdown created');
NODE

echo "Full audit complete: reports/catalog/RAW_FULL_AUDIT.md"
