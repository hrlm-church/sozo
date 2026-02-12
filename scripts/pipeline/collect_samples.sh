#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."

if [[ ! -f .env.local ]]; then
  echo "ERROR: .env.local not found"
  exit 1
fi

# shellcheck disable=SC1091
source .env.local

mkdir -p data/samples

sources=(bloomerang donor_direct givebutter keap kindful stripe transactions_imports)

echo "Collecting one sample CSV per source from raw container..."

for source in "${sources[@]}"; do
  blob_name=$(az storage blob list \
    --account-name "$SOZO_STORAGE_ACCOUNT" \
    --account-key "$SOZO_STORAGE_ACCOUNT_KEY" \
    --container-name "$SOZO_STORAGE_RAW_CONTAINER" \
    --prefix "$source/" \
    --query "[?contains(name, '.csv')].name | [0]" \
    -o tsv)

  if [[ -z "${blob_name:-}" ]]; then
    echo "WARN: no csv found for $source"
    continue
  fi

  az storage blob download \
    --account-name "$SOZO_STORAGE_ACCOUNT" \
    --account-key "$SOZO_STORAGE_ACCOUNT_KEY" \
    --container-name "$SOZO_STORAGE_RAW_CONTAINER" \
    --name "$blob_name" \
    --file "data/samples/${source}.csv" \
    --overwrite >/dev/null

  echo "OK: $source -> $blob_name"
done

echo "Done. Samples in data/samples/*.csv"
