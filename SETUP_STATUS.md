# Sozo Azure Setup Status

## Current state (validated)
- Subscription: `994cae15-7d28-4fa8-b98d-5d135ab93be3`
- Resource group: `pf-data-platform`
- Storage account: `pfpuredatalake`
- Containers: `raw`, `clean`, `mart`
- Raw source folders: `bloomerang`, `donor_direct`, `givebutter`, `keap`, `kindful`, `stripe`, `transactions_imports`
- Service Bus namespace: `sozoingest99722`
- Queues: `ingestion-jobs`, `insight-jobs`, `dead-letter-review`
- Key Vault: `sozokv00502`
- Azure SQL Server: `sozosql01729.database.windows.net`
- Azure SQL DB: `sozoapp`
- Azure Search service: `sozosearch602572`

## What was created in this run

### Repo artifacts
- `sql/pipeline/001_init_pipeline.sql`
- `scripts/pipeline/_db.js`
- `scripts/pipeline/01_create_schema.js`
- `scripts/pipeline/02_build_ingestion_mapping.js`
- `scripts/pipeline/collect_samples.sh`
- `scripts/pipeline/03_load_bronze_samples.js`
- `scripts/pipeline/04_transform_silver.js`
- `scripts/pipeline/05_build_gold.js`
- `scripts/pipeline/06_validate_pipeline.js`
- `scripts/pipeline/07_sync_search.js`
- `package.json` scripts updated (`pipeline:*` commands)

### SQL schemas/tables
Created/verified schemas:
- `meta`, `bronze`, `silver`, `gold`

Created/verified key tables:
- `meta.source_system`
- `meta.ingestion_mapping`
- `meta.source_file_lineage`
- `bronze.raw_record`
- `silver.person_source`
- `silver.transaction_source`
- `silver.engagement_source`
- Gold canonical tables:
  - `gold.person`, `gold.household`, `gold.organization`, `gold.address`, `gold.contact_point`
  - `gold.identity_resolution`, `gold.relationship`, `gold.crosswalk`, `gold.source_file_lineage`
  - `gold.donation_transaction`, `gold.payment_transaction`, `gold.recurring_plan`
  - `gold.invoice`, `gold.invoice_line`, `gold.[order]`, `gold.order_line`
  - `gold.campaign`, `gold.fund`, `gold.engagement_activity`, `gold.consent_preferences`, `gold.communication_touch`

### Ingestion mapping
- `meta.ingestion_mapping`: `56` rows (7 sources x 8 patterns)

### Bronze sample batch loaded (one file per source)
- `bloomerang`: 25 rows
- `donor_direct`: 25 rows
- `givebutter`: 25 rows
- `keap`: 25 rows
- `kindful`: 25 rows
- `stripe`: 25 rows
- `transactions_imports`: 25 rows

Total Bronze loaded this run: `175` rows.

### Silver transform output
- `silver.person_source`: 89 rows inserted
- `silver.transaction_source`: 100 rows inserted
- `silver.engagement_source`: 0 rows inserted (from sampled files)

### Gold canonical output
- `gold.person`: 62
- `gold.household`: 19
- `gold.crosswalk`: 89
- `gold.identity_resolution`: 89
- `gold.source_file_lineage`: synced from `meta.source_file_lineage`

### Search sync
- Existing configured index had incompatible field definitions.
- Auto-versioned index created/used: `household-insights-v1-v2`
- Synced insight docs: `81` (`person` + `household`)

## Validation query results (this run)
- Row counts by source in Bronze: all 7 sources present with 25 rows each
- Null checks:
  - `silver.person_source.missing_source_record_id`: 0
  - `silver.transaction_source.missing_source_record_id`: 0
  - `gold.crosswalk.missing_canonical_id`: 0
- Duplicate source key check:
  - `gold.crosswalk.duplicate_source_key`: 0
- Unmatched identity clusters (`possible_match=1`): none in current sample batch

## Exact commands run
```bash
npm run pipeline:schema
npm run pipeline:mapping
npm run pipeline:samples
npm run pipeline:bronze
npm run pipeline:silver
npm run pipeline:gold
npm run pipeline:validate
npm run pipeline:search
```

## Next command block
Run this to process larger batches (not just one sample file per source):

```bash
# 1) Pull fresh samples again (or expand collect_samples.sh to pull N files/source)
npm run pipeline:samples

# 2) Reload Bronze/Silver/Gold
npm run pipeline:bronze
npm run pipeline:silver
npm run pipeline:gold

# 3) Validate + re-sync Search
npm run pipeline:validate
npm run pipeline:search
```

## Notes
- This run was intentionally no-data-loss and additive: raw facts were mirrored into Bronze with lineage and hashes.
- Matching is deterministic-first (email/phone), with source-record fallback and support for possible-match flags.
- Next phase should add source-specific parsers and stronger probabilistic household/person matching rules.

---

## Canonical v1.1 + Intelligence v1 (latest run)

### Added schema/tables
- SQL file: `sql/pipeline/002_canonical_v11.sql`
- New schema: `gold_intel`
- New canonical tables:
  - `gold.event`
  - `gold.ticket_sale`
  - `gold.pledge_commitment`
  - `gold.refund_chargeback`
  - `gold.appeal`
  - `gold.designation`
  - `gold.support_case`
  - `gold.subscription_contract`
  - `gold.subscription_status_history`
  - `gold.subscription_shipment`
- New intelligence tables:
  - `gold_intel.feature_snapshot`
  - `gold_intel.model_score`
  - `gold_intel.microsegment_membership`
  - `gold_intel.sentiment_signal`
  - `gold_intel.next_best_action`
- Mapping rules table:
  - `meta.source_to_canonical_rule`

### New scripts
- `scripts/pipeline/08_apply_canonical_v11.js`
- `scripts/pipeline/09_map_sources_canonical_v11.js`
- `scripts/pipeline/10_build_intelligence_v1.js`
- `scripts/pipeline/11_ingest_keyfiles_v11.js`

### New npm scripts
```bash
npm run pipeline:canonical-v11
npm run pipeline:map-v11
npm run pipeline:intel-v1
npm run pipeline:ingest-keyfiles-v11
```

### Execution sequence run
```bash
npm run pipeline:canonical-v11
npm run pipeline:ingest-keyfiles-v11
npm run pipeline:silver
npm run pipeline:gold
npm run pipeline:map-v11
npm run pipeline:intel-v1
npm run pipeline:validate
```

### Key-file ingestion scope (for real v1.1 population)
- `keap/pass_1_foundation/hb840 Contact.csv` (5,000 rows)
- `keap/pass_1_foundation/hb840 Orders known as Jobs.csv` (5,000 rows)
- `keap/pass_1_foundation/hb840 Subscriptions known as JobRecurring.csv` (5,000 rows)
- `donor_direct/Kindful Donors - All Fields from Keap.csv` (782 rows)
- `donor_direct/PFM_Transactions.csv` (5,000 rows)
- `givebutter/Donor Direct Communication Data.csv` (5,000 rows)
- `stripe/Stripe Customers.csv` (344 rows)
- Total key-file rows ingested this run: **26,126**

### Post-run counts (Azure SQL)
- `gold.person`: **515**
- `gold.household`: **64**
- `gold.event`: **1,639**
- `gold.ticket_sale`: **1,639**
- `gold.subscription_contract`: **857**
- `gold.appeal`: **857**
- `gold.designation`: **1,639**
- `gold_intel.feature_snapshot` (today): **4,120**
- `gold_intel.model_score` (today): **1,030**
- `gold_intel.microsegment_membership` (today): **515**
- `gold_intel.next_best_action` (today): **515**

### Validation snapshot
- Bronze row counts now include expanded sources:
  - `keap`: 15,025
  - `donor_direct`: 5,807
  - `givebutter`: 5,025
  - `stripe`: 369
  - plus previous sample rows from other sources
- Null key checks: all zero
- Duplicate source key groups in `gold.crosswalk`: zero

### Operational note
- SQL firewall rule added for this runner IP:
  - `codex-temp-63-65-177-234` on `sozosql01729`
- Remove after work if desired.

### Next command block
```bash
# Refresh intelligence daily (after ingest/silver/gold/map)
npm run pipeline:intel-v1

# Rebuild search docs from current canonical data
npm run pipeline:search

# Validate
npm run pipeline:validate
```

---

## Signal layer + serving 360 (next stage for Sozo self-service)

### Added artifacts
- SQL:
  - `sql/pipeline/004_signal_and_serving.sql`
- Scripts:
  - `scripts/pipeline/12_build_tag_signal_map.js`
  - `scripts/pipeline/13_load_tag_signal_map.js`
  - `scripts/pipeline/14_apply_signal_and_serving.js`
  - `scripts/pipeline/15_build_signal_facts.js`
  - `scripts/pipeline/16_validate_serving.js`
- Mapping registry:
  - `data/mappings/canonical_signal_groups_v1.json`

### New objects created by this stage
- `silver.person_tag_signal`
- `gold.signal_fact`
- `gold.person_360` (view)
- `gold.household_360` (view)
- `gold.organization_360` (view)
- `serving.v_person_overview`
- `serving.v_household_overview`
- `serving.v_organization_overview`
- `serving.v_signal_explorer`

### New npm commands
```bash
npm run pipeline:signal-map
npm run pipeline:signal-map-load
npm run pipeline:signal-schema
npm run pipeline:signal-build
npm run pipeline:signal-validate
```

### Next command block (run in Azure-connected terminal)
```bash
cd "/Users/eddiemenezes/Documents/New project/sozo"

# 1) Build and load tag->signal mapping
npm run pipeline:signal-map
npm run pipeline:signal-map-load

# 2) Create signal + serving SQL objects
npm run pipeline:signal-schema

# 3) Populate person_tag_signal + rebuild signal_fact
npm run pipeline:signal-build

# 4) Validate serving layer
npm run pipeline:signal-validate

# 5) Refresh intelligence + search on top of expanded signals
npm run pipeline:intel-v1
npm run pipeline:search
```
