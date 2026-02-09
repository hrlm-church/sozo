# Sozo Handoff

## Goal
Backend-first MVP: chat + Azure-connected answers + dashboard cards.

## Current Azure state (done)
- Subscription: 994cae15-7d28-4fa8-b98d-5d135ab93be3
- Resource group: pf-data-platform
- Storage: pfpuredatalake (raw, clean, mart)
- Raw control paths: _manifests/.keep, _quarantine/.keep
- Service Bus namespace: sozoingest99722
- Queues: ingestion-jobs, insight-jobs, dead-letter-review
- Key Vault: sozokv00502
- Azure SQL Server: sozosql01729
- Azure SQL DB: sozoapp
- Azure AI Search: sozosearch602572
- Key Vault has sozo-* secrets for storage/servicebus/sql/search

## Local app state
- Repo path: /Users/eddiemenezes/Documents/New project/sozo
- GitHub: https://github.com/hrlm-church/sozo
- Vercel linked
- .env.local already generated from Key Vault
- Current UI is style-prototype dashboard; not final product

## Next technical priority
1) Add Azure OpenAI resource + model deployments
2) Build backend APIs:
   - /api/health (check SQL, Search, Storage, ServiceBus)
   - /api/chat (Azure OpenAI + SQL/Search tool routing skeleton)
   - /api/dashboard/summary (gold-view metrics)
3) Build minimal chat-first UI that can render:
   - answer text
   - citations
   - chart/table payloads

## Constraints
- Keep secrets server-side only
- Backend-first; UI polish later
- Person/household-centric data model
