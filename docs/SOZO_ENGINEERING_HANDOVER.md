# Sozo Engineering Handover

## 0) Operator Environment Status
The local workstation is already configured and authenticated for required platforms:

- Azure CLI: authenticated to subscription `994cae15-7d28-4fa8-b98d-5d135ab93be3` (`Pure Freedom Subscription`)
- GitHub: repository connected and push working (`https://github.com/hrlm-church/sozo`)
- Vercel CLI: authenticated and project linked (`hrlm-churchs-projects/sozo`)
- Local project installed at:
  - `/Users/eddiemenezes/Documents/New project/sozo`

Local prerequisites already present:
- Homebrew
- Node.js / npm
- Git
- Azure CLI (`az`)
- Vercel CLI (`vercel`)
- Project dependencies installed (`node_modules` present)

## 1) Product and System Goal
Sozo is a person/household-centric data intelligence platform.

Primary outcomes:
- Ingest all raw files from multiple business systems without data loss.
- Normalize data into Silver.
- Resolve identities into canonical Gold entities.
- Generate AI-ready signal facts and 360 views.
- Power chat + dashboards through Azure SQL + Azure AI Search + Azure OpenAI.

## 2) Current Azure Environment (working baseline)
Subscription and tenant:
- Subscription ID: `994cae15-7d28-4fa8-b98d-5d135ab93be3`
- Resource group: `pf-data-platform`

Storage:
- Account: `pfpuredatalake`
- Containers: `raw`, `clean`, `mart`
- Raw source folders:
  - `bloomerang`
  - `donor_direct`
  - `givebutter`
  - `keap`
  - `kindful`
  - `stripe`
  - `transactions_imports`

Messaging:
- Service Bus namespace: `sozoingest99722`
- Queues:
  - `ingestion-jobs`
  - `insight-jobs`
  - `dead-letter-review`

Secrets:
- Key Vault: `sozokv00502`

Database:
- Azure SQL server: `sozosql01729.database.windows.net`
- Database: `sozoapp`

Search:
- Azure AI Search service: `sozosearch602572`
- Active index (current script fallback): `household-insights-v1-v2`

## 3) Repo Layout (important paths)
- App/UI/API:
  - `src/app`
  - `src/components`
  - `src/lib/server`
- Pipeline scripts:
  - `scripts/pipeline`
- SQL DDL:
  - `sql/pipeline`
- Data maps and catalogs:
  - `data/mappings`
  - `reports/catalog`
- Operational status:
  - `SETUP_STATUS.md`

## 4) Runtime Architecture
Data + AI execution stack:
1. Blob raw files are ingested into `bronze.raw_record` with lineage and hashes.
2. Silver parsing creates typed source tables.
3. Gold build creates canonical entities and relationships.
4. Signal pipeline builds `silver.person_tag_signal` and `gold.signal_fact`.
5. Intelligence pipeline writes scores/segments in `gold_intel.*`.
6. Search sync pushes person/household docs to Azure AI Search.
7. API routes use SQL/Search/OpenAI to answer chat and dashboard requests.

High-level service flow:
- `POST /api/chat` route selection:
  - dashboard summary route
  - search route
  - sql route
- `GET /api/health` checks SQL, Search, Storage, Service Bus, OpenAI.
- `GET /api/dashboard/summary` serves KPI artifacts.

## 5) Environment Variables
Canonical env template is `.env.example`.

Required for server startup:
- `SOZO_AZURE_RESOURCE_GROUP`
- `SOZO_AZURE_LOCATION`
- `SOZO_STORAGE_ACCOUNT`
- `SOZO_SERVICEBUS_NAMESPACE`
- `SOZO_SQL_HOST`
- `SOZO_SQL_DB`
- `SOZO_SEARCH_SERVICE_NAME`

Required for full capability:
- Storage auth:
  - `SOZO_STORAGE_ACCOUNT_KEY`
- SQL query execution:
  - `SOZO_SQL_USER`
  - `SOZO_SQL_PASSWORD`
- Search sync/query:
  - `SOZO_SEARCH_ADMIN_KEY`
  - `SOZO_SEARCH_INDEX_NAME` (optional, script can auto-select)
- OpenAI chat:
  - `SOZO_OPENAI_ENDPOINT`
  - `SOZO_OPENAI_API_KEY`
  - `SOZO_OPENAI_CHAT_DEPLOYMENT`
  - `SOZO_OPENAI_API_VERSION`

## 6) Pipeline Scripts and Responsibilities
Core schema and mapping:
- `01_create_schema.js`: initialize baseline schemas/tables.
- `02_build_ingestion_mapping.js`: source->entity mapping seed.

Ingestion:
- `11_ingest_keyfiles_v11.js`:
  - Now full-mode by default.
  - Scans all 7 source prefixes for `*.csv`.
  - No row cap by default.
  - Supports optional cap with `SOZO_MAX_ROWS_PER_FILE`.
  - Has file-hash dedupe (skip already loaded).
  - Retry + continue-on-error behavior for unstable blob operations.

Transforms:
- `04_transform_silver.js`: Bronze -> Silver.
- `05_build_gold.js`: Silver -> Gold canonical + crosswalk.
- `17_backfill_person_links.js`: backfills missing `person_id` in key Gold fact tables.

Signal and serving:
- `12_build_tag_signal_map.js`: generate tag->canonical signal map.
- `13_load_tag_signal_map.js`: upsert map into `meta.tag_signal_map`.
- `14_apply_signal_and_serving.js`: create signal tables/views.
- `15_build_signal_facts.js`: rebuild `gold.signal_fact` from canonical facts + tag signals.
- `16_validate_serving.js`: serving-level row count checks.

Intelligence and search:
- `10_build_intelligence_v1.js`: feature snapshots + scoring + microsegments + NBA.
- `07_sync_search.js`: index person/household docs to Azure Search.

Maintenance:
- `18_reset_derived.js`: reset Silver/Gold/Intel (keeps Bronze immutable).
- `check_counts.js`: quick 360 + signal sanity counts.

## 7) SQL Artifacts
Base schema:
- `sql/pipeline/001_init_pipeline.sql`

Canonical v1.1 and intel tables:
- `sql/pipeline/002_canonical_v11.sql`

Tag map seed SQL (generated):
- `sql/pipeline/003_seed_tag_signal_map.sql`

Signal + serving views:
- `sql/pipeline/004_signal_and_serving.sql`

## 8) Canonical Model Coverage
Core entities in use:
- `gold.person`
- `gold.household`
- `gold.organization`
- `gold.crosswalk`
- `gold.identity_resolution`
- `gold.relationship`

Commerce/fundraising/events/subscription:
- `gold.payment_transaction`
- `gold.donation_transaction`
- `gold.ticket_sale`
- `gold.subscription_contract`
- `gold.pledge_commitment`
- `gold.refund_chargeback`
- `gold.invoice`, `gold.invoice_line`
- `gold.order`, `gold.order_line`

Signals and serving:
- `silver.person_tag_signal`
- `gold.signal_fact`
- `gold.person_360` (view)
- `gold.household_360` (view)
- `gold.organization_360` (view)
- `serving.v_person_overview`
- `serving.v_household_overview`
- `serving.v_organization_overview`
- `serving.v_signal_explorer`

Intelligence:
- `gold_intel.feature_snapshot`
- `gold_intel.model_score`
- `gold_intel.microsegment_membership`
- `gold_intel.next_best_action`
- `gold_intel.sentiment_signal`

## 9) Signal Taxonomy
Signal group registry file:
- `data/mappings/canonical_signal_groups_v1.json`

Current configured groups include:
- `fundraising_giving`
- `subscription_box_lifecycle`
- `event_ticketing_attendance`
- `campaign_marketing`
- `engagement_behavior`
- `journey_automation`
- `content_interest_topic`
- `system_ingestion_lineage`
- `manual_curated_flags`
- and the rest of the canonical list in the mapping file.

## 10) API Surface
Health:
- `GET /api/health`

Dashboard summary:
- `GET /api/dashboard/summary`

Chat:
- `POST /api/chat`
  - accepts `messages[]`, optional `personId`, optional `householdId`
  - tool-route selection by prompt heuristics
  - returns `answer`, `citations`, `artifacts`

Known route behavior:
- SQL route currently uses fixed query templates and `TOP (25)` in `src/lib/server/sql-query.ts`.
- Dashboard summary currently reads `dbo.household_risk_daily` and `dbo.profile_linkage_daily`.

## 11) Full Backfill Runbook (recommended)
Run one command at a time:

```bash
cd "/Users/eddiemenezes/Documents/New project/sozo"
npm run pipeline:reset-derived
npm run pipeline:canonical-v11
npm run pipeline:signal-schema
npm run pipeline:ingest-keyfiles-v11
npm run pipeline:silver
npm run pipeline:gold
npm run pipeline:person-backfill
npm run pipeline:signal-map
npm run pipeline:signal-map-load
npm run pipeline:signal-build
npm run pipeline:intel-v1
npm run pipeline:search
npm run pipeline:signal-validate
node scripts/pipeline/check_counts.js
```

Operational note:
- `pipeline:signal-build` is a rebuild step; if interrupted, `gold.signal_fact` may be temporarily empty until rerun.

## 12) Current Observed State Snapshot
Recent successful state (from terminal runs):
- `serving.v_person_overview`: 1511
- `serving.v_household_overview`: 189
- `serving.v_signal_explorer`: 86051
- unresolved person-linked signal facts: 3278

Interpretation:
- System is functioning.
- Identity linking improved significantly but still incomplete.
- Next gains come from complete ingestion across all raw files and stronger deterministic linking coverage.

## 13) Known Gaps and Risks
Technical gaps:
- `05_build_gold.js` is still partially row-by-row and can be slow at higher volume.
- SQL and dashboard query routes still use limited template queries (`TOP` in API-level query templates).
- Search sync index schema is minimal and not yet vectorized.
- Some unresolved person links remain in `gold.signal_fact`.

Data quality risks:
- Source heterogeneity and inconsistent identifiers across the 7 systems.
- Email coverage quality varies by source/file.
- Historical duplicate and manual tags require curation (`needs_review` rows).

Operational risks:
- Long running commands can be interrupted, leaving derived tables partially rebuilt.
- CLI shell errors from pasted multi-line commands can derail execution.

## 14) Engineering Priorities (next)
Priority 1:
- Convert Gold build to set-based SQL stored procedures for scale and reliability.
- Make `signal-build` transactional for atomic rebuild safety.

Priority 2:
- Full identity strategy improvements:
  - stronger deterministic key extraction (email/phone/customer IDs)
  - confidence-scored fallback matching
  - explicit unmatched queue table

Priority 3:
- Improve search documents:
  - include richer 360 summary fields
  - include signal aggregates and lineage pointers
  - add chunking by profile facets for better retrieval precision

Priority 4:
- Harden API query layer:
  - replace static `TOP (25)` templates with governed query planner on serving views.
  - add guardrails and query audit log.

## 15) Handover Checklist for Incoming Engineer
1. Verify local `.env.local` and `npm run dev` startup.
2. Run `GET /api/health` and confirm all configured services are healthy.
3. Run full backfill runbook once in non-interrupted shell.
4. Confirm final counts with `node scripts/pipeline/check_counts.js`.
5. Inspect `reports/catalog/TAG_SIGNAL_GROUP_REVIEW_QUEUE.tsv` and triage tags.
6. Review and optimize `05_build_gold.js` to set-based operations.
7. Add CI checks for pipeline script syntax and key row-count invariants.

## 16) Quick Commands
Development:
```bash
npm run dev
npm run build
npm run lint
```

Health and diagnostics:
```bash
curl -sS http://127.0.0.1:3000/api/health
npm run pipeline:signal-validate
node scripts/pipeline/check_counts.js
```

---
This handover is meant to be implementation-ready. If behavior diverges from this document, treat the script files and SQL files in this repo as source of truth and update this document in the same commit.
