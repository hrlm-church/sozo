# Sozo Data Warehouse — Architecture Open Questions: Responses

**Pure Freedom Ministries | Internal Engineering | February 2026**

This document provides detailed responses to the 11 architectural questions raised in *sozo_architecture_open_questions.pdf* (v1.0). Each answer describes the current state, identifies gaps, and recommends a path forward.

---

## 1. Source System Ingestion & Automation

**Current State:**
Data collection today is entirely manual across two distinct ingestion patterns:

- **Original 7 sources** (Keap, Donor Direct, Givebutter, Bloomerang, Kindful, Stripe customers, Transaction Imports): One-time CSV exports were uploaded to Azure Blob Storage (`pfpuredatalake/raw`). The blob ingestion script (`load_bronze_to_sql.js`) reads these into the `raw.record` bronze table.
- **6 newer sources** (Mailchimp, Stripe charges, WooCommerce, Tickera, Subbly, Shopify): CSV exports were loaded from an external drive via `load_local_to_bronze.js` into per-source bronze schemas.

There is no automation for downloading files from source systems. Files are not versioned by date or checksum. Re-running the loader on the same file would produce duplicates unless the target table is truncated first.

**Recommended Path Forward:**

| Source | Collection Method | API Available | Recommended Approach |
|--------|------------------|---------------|---------------------|
| Keap CRM | Scheduled CSV export | REST API (v2) | API pull via Azure Function |
| Donor Direct | Scheduled CSV export | SFTP only | SFTP fetch on timer |
| Givebutter | Manual export | REST API | API pull via Azure Function |
| Bloomerang | Manual export | REST API | API pull via Azure Function |
| Kindful | Manual export | None (sunset) | Archive only, no refresh |
| Stripe | — | REST API (full) | API pull or webhook listener |
| Mailchimp | Manual export | REST API (v3) | API pull via Azure Function |
| WooCommerce | Manual export | WP REST API | API pull via Azure Function |
| Tickera | Manual export | WP DB query | WordPress cron export |
| Subbly | Manual export | REST API | API pull via Azure Function |
| Shopify | Manual export | REST Admin API | API pull via Azure Function |

**File versioning:** Each ingested file should be uploaded to Blob Storage under a date-partitioned path (e.g., `raw/keap/2026-02-19/contacts.csv`) with a SHA-256 checksum recorded in `meta.file_lineage` to prevent reprocessing.

---

## 2. Pipeline Orchestration & Scheduling

**Current State:**
The pipeline is fully manual. An operator runs `node scripts/pipeline/<step>.js` in sequence from the command line. There is no orchestration layer, no cron schedule, no failure alerting, and no monitoring dashboard.

Most scripts support `--from=N` for step-level checkpointing, allowing partial reruns without restarting from scratch. However, there is no automatic detection of where a failed run stopped.

**Recommended Approach (Phase 1 — Minimal):**
1. **Azure Function (Timer Trigger)** running nightly at 2:00 AM ET that executes the pipeline steps sequentially via child_process.
2. **`meta.pipeline_run` table** logging each execution:

```sql
CREATE TABLE meta.pipeline_run (
    run_id        INT IDENTITY PRIMARY KEY,
    step_name     VARCHAR(100),
    started_at    DATETIME2 DEFAULT GETDATE(),
    completed_at  DATETIME2,
    status        VARCHAR(20), -- running, completed, failed
    rows_affected INT,
    error_message NVARCHAR(MAX)
);
```

3. **Failure alerting:** Azure Monitor alert on the Function App. If any step writes `status = 'failed'`, send an email or Teams notification via Logic App.
4. **Dashboard:** A simple query against `meta.pipeline_run` showing last run times and status per step, surfaced in the Sozo chat via a new serving view.

**Phase 2 (If Complexity Grows):** Move to Azure Data Factory or GitHub Actions workflows for DAG-based orchestration with retry policies and dependency management.

---

## 3. Concurrency Control & Idempotency

**Current State:**
The documentation warns against concurrent runs, but no enforcement mechanism exists. If two operators accidentally start the same step, duplicate rows will be inserted and lock contention may cause timeouts or deadlocks.

The pipeline is **not idempotent** in its current form. Re-running a transform step without first truncating the target table will produce duplicate rows.

**Recommended Controls:**

### Mutex Lock
Add a `meta.pipeline_lock` table:

```sql
CREATE TABLE meta.pipeline_lock (
    lock_name     VARCHAR(50) PRIMARY KEY,
    acquired_by   VARCHAR(200),
    acquired_at   DATETIME2,
    expires_at    DATETIME2
);
```

Each script calls `MERGE INTO meta.pipeline_lock` at startup. If the lock is held and not expired (e.g., 4-hour TTL), the script exits with a clear error message. The lock is released on completion.

### Idempotency Strategy
Two approaches, depending on the table:

1. **Full reload (recommended for most tables):** `TRUNCATE` + `INSERT`. Each step clears its target before populating. This is already the pattern for most transform steps — it just needs to be enforced consistently.
2. **Upsert for incremental tables:** For tables that may receive partial updates (e.g., daily API pulls), use `MERGE` with a natural key:

```sql
MERGE silver.stripe_charge AS target
USING (VALUES (...)) AS source (stripe_charge_id, ...)
ON target.stripe_charge_id = source.stripe_charge_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

### Unique Constraints
Add unique indexes on natural keys to prevent silent duplication:
- `silver.contact`: UNIQUE on `(source_system, source_id)`
- `silver.donation`: UNIQUE on `(source_system, source_id)`
- `silver.stripe_charge`: UNIQUE on `(stripe_charge_id)`

---

## 4. Bronze - Silver - Serving Layer Definitions

**Current State:**

| Layer | Reload Strategy | Description |
|-------|----------------|-------------|
| Bronze | Append (no dedup) | Raw CSV rows, all NVARCHAR(MAX). One table per file. |
| Silver | Full reload per step | Typed, normalized. Each transform step truncates + inserts. |
| Serving | Full reload | 14 materialized views. `create_serving_views.js` drops and recreates views; `materialize_serving.js` drops and rebuilds indexed tables. |

The architecture deliberately uses "Serving" instead of "Gold" because these tables are purpose-built for LLM consumption, not general BI reporting. They include pre-computed joins, aggregations, and display-ready formatting that a traditional Gold layer would not have.

**BI Tool Compatibility:**
The Serving layer is standard Azure SQL tables with indexes. Any BI tool (Power BI, Tableau, Looker) can connect via ODBC or DirectQuery without modification. Key connection details:
- Server: `sozosql01729.database.windows.net`
- Database: `sozov2`
- Schema: `serving`
- Authentication: SQL auth (sozoadmin) or Azure AD

No additional "Gold" layer is needed unless BI tools require denormalized star schemas, which can be added as additional serving views.

---

## 5. Environment Isolation (Dev / UAT / Prod)

**Current State:**
Single environment. One database (`sozov2`), one search index (`sozo-360-v1`), one App Service (`sozo-app`). Azure Key Vault (`sozokv00502`) exists but is not wired into the application — secrets are stored in `.env.local` (local development) and Azure App Service Application Settings (production).

**Recommended 2-Environment Setup:**

| Resource | Production | Development |
|----------|-----------|-------------|
| SQL Database | `sozov2` | `sozov2-dev` |
| Search Index | `sozo-360-v1` | `sozo-360-dev` |
| App Service | `sozo-app` | `sozo-app-dev` |
| Key Vault | `sozokv00502` | Same vault, `-dev` suffix secrets |

**Promotion process:**
1. Developer works on feature branch, tests against `sozov2-dev`
2. PR reviewed and merged to `main`
3. CI/CD deploys to `sozo-app-dev` automatically
4. Manual promotion: tag release, deploy to `sozo-app` (production)

**Key Vault integration:** Replace hardcoded secrets in App Settings with Key Vault references (`@Microsoft.KeyVault(SecretUri=...)`). Azure App Service natively supports this via Managed Identity.

---

## 6. Code & Schema Versioning

**Current State:**
- **Repository:** Single monorepo (`hrlm-church/sozo`) containing frontend, backend, and pipeline scripts.
- **Branching:** Feature branches merged to `main`. No formal model (no `develop` branch, no release tags).
- **Schema changes:** Applied via pipeline scripts. `create_silver.js` defines all Silver table DDL. `create_serving_views.js` defines all view definitions. Running these scripts is effectively a migration — they use `IF OBJECT_ID(...) IS NOT NULL DROP` + `CREATE` patterns.

**Recommended Additions:**

1. **Schema version tracking:**

```sql
CREATE TABLE meta.schema_version (
    version_id    INT PRIMARY KEY,
    description   NVARCHAR(500),
    applied_at    DATETIME2 DEFAULT GETDATE(),
    applied_by    VARCHAR(100)
);
```

Each DDL script increments the version and logs what changed.

2. **Git branching model:** Adopt GitHub Flow (feature branches + main). No need for `develop` or `release` branches at this scale.

3. **Migration framework:** The current approach (DDL in JS scripts) is pragmatic for this team size. A formal migration tool (Flyway, Liquibase) adds overhead without proportional benefit for a <20 table schema. Revisit if the team grows beyond 3 engineers or schema changes become frequent.

---

## 7. Azure SQL DTU Tier — Temporary or Long-Term?

**Current State:**
The Low DTU tier (Basic/S0, ~5 DTUs) is a cost-saving measure that drives significant architectural workarounds:
- Batch inserts limited to 50-100 rows with 200-300ms delays
- No concurrent pipeline runs
- Client-side JSON parsing (server-side OPENJSON too slow)
- Full pipeline run: ~6-8 hours
- RAG index build: ~3.5 hours

**Cost Analysis:**

| Tier | DTUs | Monthly Cost | Pipeline Time (est.) | Concurrent Runs |
|------|------|-------------|---------------------|-----------------|
| S0 (current) | 10 | ~$15 | 6-8 hours | No |
| S2 | 50 | ~$75 | 1-2 hours | Yes (2) |
| S3 | 100 | ~$150 | 30-60 min | Yes (3+) |
| S4 | 200 | ~$300 | 15-30 min | Yes |

**Recommendation:**
- **Immediate:** Upgrade to S2 ($75/month). This eliminates most workarounds (larger batches, no wait times, server-side JSON becomes viable) while keeping costs nonprofit-friendly.
- **Long-term:** S3 if daily refresh and concurrent BI + pipeline workloads are needed.
- **Not recommended:** Azure Synapse or Databricks. The total data volume (~4M rows, ~50GB) is well within Azure SQL capabilities. These services are designed for petabyte-scale workloads and would add unnecessary complexity and cost.

**SLA expectations:** With S2, end-to-end pipeline (ingest through RAG rebuild) should complete in under 2 hours. With nightly scheduling, data would be fresh by 4:00 AM ET daily.

---

## 8. RAG Layer Currency & Maintenance

**Current State:**
- Full rebuild every run: `build_rag_index.js` processes all 89,143 people (~3.5 hours on current DTU tier)
- No incremental update mechanism
- No automated trigger after serving layer refresh
- No quality evaluation metrics
- Embedding model: OpenAI `text-embedding-3-small` (1,536 dimensions)

**Recommended Improvements:**

### Incremental Rebuild
Add a `last_modified` timestamp to `serving.person_360`. On each RAG rebuild, only process people modified since the last run:

```sql
SELECT person_id FROM serving.person_360
WHERE last_modified > @last_rag_build_time
```

This reduces a typical daily rebuild from 89K documents to ~500-5,000 (only those whose underlying data changed).

### Automated Trigger
The RAG rebuild should be the final step in the nightly pipeline. The orchestration function (see Question 2) runs `build_rag_index.js` after `materialize_serving.js` completes.

### Model Evolution Strategy
If the embedding model changes (e.g., `text-embedding-3-large` or a future model):
1. Create a new search index (`sozo-360-v2`)
2. Rebuild all documents with the new model
3. Update `SOZO_SEARCH_INDEX_NAME` in App Settings
4. Delete the old index after verification

This is a zero-downtime migration — the old index serves queries until the new one is ready.

### Quality Evaluation
Implement a test suite of 20 known queries with expected results:

| Query | Expected Top Result | Validation |
|-------|-------------------|------------|
| "top donors who attend events" | Renee Woods ($89K, 11 events) | Person in top 3 |
| "True Girl subscribers in Nashville" | Active Subbly subs in TN | Results have TN location |
| "why are subscribers canceling" | People with cancellation reasons | Results contain cancel feedback |

Run this suite after each RAG rebuild. Log relevance scores. Alert if any query drops below threshold.

---

## 9. LLM SQL Guardrails & Query Governance

**Current State:**

| Guardrail | Implementation |
|-----------|---------------|
| DDL blocking | `sql-guard.ts` rejects DROP, ALTER, CREATE, INSERT, UPDATE, DELETE, EXEC |
| Read-only enforcement | Requires SELECT or WITH prefix |
| Row limit | Injects `TOP (500)` if missing |
| Table allowlist | Only `serving.*` tables permitted |
| Timeout | 90-second query timeout |
| Result cap | 500 rows maximum returned to LLM |

**Gaps and Recommendations:**

### Cross-Source Joins Not in Serving Views
If the LLM encounters a question that requires data not available in the current 14 serving views, it will either fail gracefully ("I don't have access to that data") or attempt an incorrect query.

**Mitigation:** Add new serving views as cross-source questions are identified. The system prompt already guides the LLM to use specific views for specific question types. The view catalog is the engineering team's primary lever for expanding Sozo's analytical capabilities.

### Query Cost Tracking
Add logging to `sql-guard.ts`:

```typescript
// After query execution
console.log(JSON.stringify({
    event: 'query_executed',
    query_hash: hashQuery(sql),
    execution_time_ms: elapsed,
    rows_returned: results.length,
    timestamp: new Date().toISOString()
}));
```

This enables analysis of which queries are slow, which are most common, and whether the LLM is generating efficient SQL.

### Validated SQL Templates
The system prompt already functions as a soft template library (widget selection guide, formatting rules, SQL rules). A harder template approach — where common queries are pre-written and the LLM selects from them — would improve consistency but reduce flexibility for novel questions. Recommended approach: maintain a library of "reference queries" in the system prompt that the LLM can adapt, rather than rigid templates it must use verbatim.

---

## 10. Identity Resolution — Confidence & Auditability

**Current State:**
The 4-phase Union-Find resolves 212,781 contacts into 89,143 unique people. Merge decisions are not logged, not scored, and not reversible without a full pipeline rerun.

**Merge Phase Breakdown:**

| Phase | Method | Merges | Confidence | % of Total |
|-------|--------|--------|------------|-----------|
| 1 | Cross-reference | 4,740 | Very High | 4% |
| 2 | Email match | 126,471 | High | 53% |
| 3 | Phone match | 49,916 | Medium | 21% |
| 4 | Name + ZIP | 60,334 | Low | 25% |

Phase 4 (name + ZIP) accounts for 25% of all merges and is the most likely to produce false positives.

**Recommended Improvements:**

### 1. Confidence Scoring
Add columns to `silver.identity_map`:

```sql
ALTER TABLE silver.identity_map ADD
    merge_phase     TINYINT,      -- 1=crossref, 2=email, 3=phone, 4=name+zip
    merge_confidence VARCHAR(10),  -- very_high, high, medium, low
    merge_reason    NVARCHAR(200); -- 'email: jane@example.com matched keap:123 + dd:456'
```

### 2. Merge Audit Log

```sql
CREATE TABLE silver.merge_audit (
    audit_id        INT IDENTITY PRIMARY KEY,
    run_date        DATETIME2 DEFAULT GETDATE(),
    master_id       INT,
    contact_id_a    INT,
    contact_id_b    INT,
    merge_phase     TINYINT,
    merge_key       NVARCHAR(500), -- the email, phone, or name+zip that triggered the merge
    source_system_a VARCHAR(20),
    source_system_b VARCHAR(20)
);
```

### 3. Manual Override Table

```sql
CREATE TABLE silver.merge_override (
    override_id     INT IDENTITY PRIMARY KEY,
    action          VARCHAR(10), -- 'split' or 'merge'
    contact_id_a    INT,
    contact_id_b    INT,
    reason          NVARCHAR(500),
    created_by      VARCHAR(100),
    created_at      DATETIME2 DEFAULT GETDATE()
);
```

The identity resolution script reads this table before running and applies overrides: forced splits prevent matching even if email/phone matches; forced merges link records regardless of matching rules.

### 4. Review Queue
Create a serving view that surfaces low-confidence merges for human review:

```sql
CREATE VIEW serving.merge_review_queue AS
SELECT im.master_id, im.contact_id, im.merge_phase, im.merge_reason,
       c.first_name, c.last_name, c.email_primary, c.source_system
FROM silver.identity_map im
JOIN silver.contact c ON c.contact_id = im.contact_id
WHERE im.merge_phase = 4  -- name+zip matches only
ORDER BY im.master_id;
```

---

## 11. Data Generation Frequency & Volume per Source

**Estimated Daily Record Generation:**

| Source | Est. Daily Records | Pattern | Latency Need |
|--------|-------------------|---------|-------------|
| Stripe | 50-100 charges | Event-driven, continuous | Same-day during campaigns |
| WooCommerce | 20-50 orders | Event-driven, business hours | Nightly sufficient |
| Shopify | 5-20 orders | Event-driven, business hours | Nightly sufficient |
| Mailchimp | 100-500 changes | Batch (tag/status changes) | Nightly sufficient |
| Keap CRM | 50-200 updates | Mixed (manual + automated) | Nightly sufficient |
| Subbly | 5-20 events | Event-driven (subscribe/cancel) | Nightly sufficient |
| Givebutter | 5-50 donations | Campaign-driven, spiky | Same-day during campaigns |
| Donor Direct | 5-20 donations | Batch export, weekly | Weekly sufficient |
| Tickera | 0-500 tickets | Event-driven, very spiky (tour days) | Nightly during tour season |
| Bloomerang | ~0 (legacy) | Archive | No refresh needed |
| Kindful | ~0 (sunset) | Archive | No refresh needed |

**Total:** ~250-1,000 new/updated records per day under normal operations. During peak periods (December giving season, tour event days), this can spike to 2,000-5,000/day.

**Peak Periods:**
- **December:** 25% of annual giving. Donation volume 5-10x normal.
- **Tour dates:** 500-1,500 ticket sales in a single day.
- **Campaign launches:** Email sends trigger tag changes for 50K+ contacts.

**Recommended Ingestion Architecture:**

```
TIER 1 — Nightly Batch (Default)
├── All 13 sources pulled via API or SFTP
├── Full pipeline: bronze → silver → identity → serving → RAG
├── Scheduled at 2:00 AM ET via Azure Function
└── Expected completion: <2 hours (on S2 tier)

TIER 2 — On-Demand Refresh (Campaign Mode)
├── Triggered manually or by webhook
├── Stripe + Givebutter + Donation sources only
├── Incremental: only new records since last pull
└── Updates serving layer + RAG for affected people

TIER 3 — Future (Real-Time, If Needed)
├── Stripe webhook → Azure Function → immediate charge insert
├── Event-driven, per-transaction
└── Only justified if same-hour freshness is required
```

**Conclusion:** A nightly batch cycle is sufficient for 95% of operational decisions. The remaining 5% (campaign-day donation tracking, tour-day ticket counts) can be addressed with an on-demand refresh trigger that processes only the transactional sources incrementally. True real-time ingestion (webhooks, streaming) is not justified by the current data volume or operational cadence.

---

## Summary of Immediate Priorities

| Priority | Question | Action | Effort |
|----------|----------|--------|--------|
| 1 | Q7 — DTU Tier | Upgrade Azure SQL to S2 | 15 min (portal change) |
| 2 | Q3 — Concurrency | Add pipeline lock table | 1 day |
| 3 | Q2 — Orchestration | Azure Function timer + pipeline_run logging | 2-3 days |
| 4 | Q10 — Identity Audit | Add merge_phase/confidence to identity_map | 1 day |
| 5 | Q1 — API Ingestion | Connect Stripe + Shopify + Givebutter APIs | 1-2 weeks |
| 6 | Q5 — Dev Environment | Create sozov2-dev + sozo-app-dev | 1 day |
| 7 | Q8 — RAG Maintenance | Incremental rebuild + test suite | 2-3 days |
| 8 | Q6 — Schema Versioning | Add meta.schema_version table | 2 hours |
| 9 | Q9 — Query Logging | Add execution logging to sql-guard | 2 hours |
| 10 | Q4 — BI Connectivity | Document ODBC connection for Power BI | 1 hour |
| 11 | Q11 — Refresh Cadence | Implement nightly batch after API ingestion | Included in Q2 |

---

*Classification: Internal — Engineering | Version 1.0 | Sozo Data Warehouse | Pure Freedom Ministries*
