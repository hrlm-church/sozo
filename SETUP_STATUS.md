# Sozo Azure Setup Status

## Current state
- Subscription: 994cae15-7d28-4fa8-b98d-5d135ab93be3
- Resource group: pf-data-platform
- Location (data): eastus
- SQL location: eastus2
- Storage account: pfpuredatalake
- Containers: raw, clean, mart
- Raw control paths: _manifests/.keep, _quarantine/.keep
- Service Bus namespace: sozoingest99722
- Queues: ingestion-jobs, insight-jobs, dead-letter-review
- Key Vault: sozokv00502
- SQL Server: sozosql01729
- SQL DB: sozoapp

## Next command block
export AZ_SQL_SERVER="sozosql01729"
export AZ_SQL_DB="sozoapp"
export AZ_SQL_HOST="sozosql01729.database.windows.net"

az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-sql-server" --value "$AZ_SQL_SERVER"
az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-sql-db" --value "$AZ_SQL_DB"
az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-sql-user" --value "$AZ_SQL_ADMIN"
az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-sql-password" --value "$AZ_SQL_PASSWORD"
az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-sql-host" --value "$AZ_SQL_HOST"
az keyvault secret list --vault-name "$AZ_KV_NAME" --query "[?starts_with(name,'sozo-sql')].name" --output table

## Azure OpenAI provisioning (MVP priority 1)
export AZ_OAI_NAME="sozoaoai$RANDOM"
export AZ_OAI_DEPLOYMENT="sozo-gpt4o-mini"
export AZ_OAI_MODEL="gpt-4o-mini"
export AZ_OAI_VERSION="2024-07-18"

az cognitiveservices account create \
  --name "$AZ_OAI_NAME" \
  --resource-group "$AZ_RG" \
  --location "$AZ_LOCATION" \
  --kind OpenAI \
  --sku S0 \
  --yes

az cognitiveservices account deployment create \
  --name "$AZ_OAI_NAME" \
  --resource-group "$AZ_RG" \
  --deployment-name "$AZ_OAI_DEPLOYMENT" \
  --model-name "$AZ_OAI_MODEL" \
  --model-version "$AZ_OAI_VERSION" \
  --model-format OpenAI \
  --sku-capacity 10 \
  --sku-name Standard

AZ_OAI_ENDPOINT="$(az cognitiveservices account show --name "$AZ_OAI_NAME" --resource-group "$AZ_RG" --query properties.endpoint -o tsv)"
AZ_OAI_KEY="$(az cognitiveservices account keys list --name "$AZ_OAI_NAME" --resource-group "$AZ_RG" --query key1 -o tsv)"

az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-openai-endpoint" --value "$AZ_OAI_ENDPOINT"
az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-openai-api-key" --value "$AZ_OAI_KEY"
az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-openai-chat-deployment" --value "$AZ_OAI_DEPLOYMENT"
az keyvault secret set --vault-name "$AZ_KV_NAME" --name "sozo-openai-api-version" --value "2024-10-21"
