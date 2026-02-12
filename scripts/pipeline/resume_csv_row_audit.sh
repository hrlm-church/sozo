#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
source .env.local
mkdir -p /tmp/sozo-audit/full reports/catalog

node - <<'NODE'
const fs=require('fs');
const blobs=JSON.parse(fs.readFileSync('reports/catalog/raw_blobs_full.json','utf8'));
const csv=blobs.filter(b=>/\.csv$/i.test(b.name||'')).map(b=>({
  source:(b.name||'').split('/')[0]||'(root)',
  name:b.name,
  size:Number(b.properties?.contentLength||0),
  modified:b.properties?.lastModified||''
}));
let done=new Set();
if(fs.existsSync('reports/catalog/raw_csv_row_audit.jsonl')){
  const lines=fs.readFileSync('reports/catalog/raw_csv_row_audit.jsonl','utf8').trim().split(/\r?\n/).filter(Boolean);
  for(const l of lines){
    try{const o=JSON.parse(l); if(o.blob_name) done.add(o.blob_name);}catch{}
  }
}
const missing=csv.filter(r=>!done.has(r.name));
fs.writeFileSync('/tmp/sozo-audit/missing_csv.tsv',missing.map(r=>[r.source,r.name,r.size,r.modified].join('\t')).join('\n')+(missing.length?'\n':''));
console.log(`missing_csv=${missing.length}`);
NODE

if [[ ! -s /tmp/sozo-audit/missing_csv.tsv ]]; then
  echo "No missing CSV audits."
  exit 0
fi

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
  echo "audited: $blob_name"
done < /tmp/sozo-audit/missing_csv.tsv
