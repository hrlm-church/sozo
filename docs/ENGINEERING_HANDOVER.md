# Sozo Engineering Handover

**Pure Freedom Ministries — Data Analytics Platform**
**Version**: 2.0 | **Date**: February 2026 | **Author**: AI Engineering (Claude)

---

## Table of Contents

1. [What Is Sozo](#1-what-is-sozo)
2. [Architecture Overview](#2-architecture-overview)
3. [Environments & Access](#3-environments--access)
4. [Data Pipeline — End to End](#4-data-pipeline--end-to-end)
5. [Database Schema](#5-database-schema)
6. [Identity Resolution](#6-identity-resolution)
7. [Serving Layer](#7-serving-layer)
8. [RAG Layer — Semantic Search](#8-rag-layer--semantic-search)
9. [Frontend Application](#9-frontend-application)
10. [LLM Chat System](#10-llm-chat-system)
11. [CI/CD & Branch Workflow](#11-cicd--branch-workflow)
12. [Critical Constraints & Gotchas](#12-critical-constraints--gotchas)
13. [File Reference](#13-file-reference)
14. [What Was Built & Current State](#14-what-was-built--current-state)

---

## 1. What Is Sozo

Sozo is a **donor intelligence platform** for Pure Freedom Ministries (the "True Girl" brand). It merges data from 13 source systems into a unified warehouse, resolves identities across systems, and provides an AI-powered chat interface that answers any question about the ministry's supporters.

**The problem it solves**: The ministry's data was scattered across 13 disconnected systems — Keap CRM, Donor Direct fundraising, Givebutter, Stripe, WooCommerce, Shopify, Subbly, Tickera, Mailchimp, Bloomerang, Kindful, and others. No one could answer "who are our most engaged supporters?" without manually checking 6 different platforms.

**What it does**: Sozo ingests all 13 sources (3.4M+ raw rows), resolves duplicate people across systems (120K records → 89K unique people), builds 360-degree profiles, and lets staff ask natural-language questions like "top 20 donors this year" or "find supporters who attend events AND donate."

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│                        DATA SOURCES (13)                      │
│  Keap · Donor Direct · Givebutter · Bloomerang · Kindful     │
│  Stripe · WooCommerce · Shopify · Subbly · Tickera           │
│  Mailchimp · Transaction Imports · Wealth Screening          │
└───────────────────────┬──────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────┐
│                    AZURE BLOB STORAGE                         │
│  pfpuredatalake/raw — 53 CSV files (original 7 sources)      │
│  Local drive — 15 CSV files (6 newer sources)                │
└───────────────────────┬──────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Raw)                        │
│  18 schemas · 50+ tables · All NVARCHAR(MAX) columns         │
│  Every CSV → one bronze table (exact column names preserved) │
└───────────────────────┬──────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────┐
│                     SILVER LAYER (Typed)                      │
│  silver.contact (120K) · silver.donation (67K)               │
│  silver.stripe_charge (163K) · silver.woo_order (67K)        │
│  silver.event_ticket (21.5K) · silver.shopify_order (5K)     │
│  silver.subbly_subscription (2.4K) · silver.generic_tag      │
│  silver.identity_map (120K → 89K master_ids)                 │
└───────────────────────┬──────────────────────────────────────┘
                        ▼
┌──────────────────────────────────────────────────────────────┐
│                    SERVING LAYER (Views)                      │
│  person_360 · household_360 · donation_detail                │
│  donor_summary · donor_monthly · order_detail                │
│  subscription_detail · tag_detail · communication_detail     │
│  event_detail · stripe_charge_detail · woo_order_detail      │
│  wealth_screening · lost_recurring_donors · stripe_customer  │
│  All materialized as indexed tables for instant queries      │
└────────┬─────────────────────────────────┬───────────────────┘
         ▼                                 ▼
┌─────────────────────┐      ┌─────────────────────────────────┐
│   AZURE AI SEARCH   │      │        NEXT.JS APP              │
│   sozo-360-v1       │      │   Chat (LLM + SQL + Search)     │
│   89,143 person     │      │   Dashboard (drag-drop widgets) │
│   documents with    │◄────►│   Export (CSV / XLS / PDF)      │
│   vector embeddings │      │   Auth (Microsoft Entra ID)     │
└─────────────────────┘      └─────────────────────────────────┘
```

**Tech Stack**:
- **Frontend**: Next.js 16, React 19, Tailwind CSS 4, Recharts, react-grid-layout, Zustand
- **Backend**: Next.js API routes (serverless), AI SDK 6 (Vercel)
- **Database**: Azure SQL (server: `sozosql01729.database.windows.net`)
- **Search**: Azure AI Search (`sozosearch602572`) with vector embeddings
- **AI**: Anthropic Claude (primary), OpenAI (fallback), Azure OpenAI (fallback)
- **Auth**: NextAuth v5 with Microsoft Entra ID (OAuth)
- **Storage**: Azure Blob Storage (`pfpuredatalake`)

---

## 3. Environments & Access

### Three Environments

| | Production | Development | Local |
|---|---|---|---|
| **URL** | `sozo-app.azurewebsites.net` | `sozo-app-dev.azurewebsites.net` | `localhost:3000` |
| **Database** | `sozoapp` (Standard, 20 DTU) | `sozoapp-dev` (Basic, 5 DTU) | Same as env config |
| **Branch** | `main` | `dev` | Any feature branch |
| **Deploy** | Auto on merge to `main` | Auto on merge to `dev` | `npm run dev` |

### Azure Resources (Resource Group: `pf-data-platform`)

| Resource | Name | Purpose |
|----------|------|---------|
| SQL Server | `sozosql01729` | Databases for prod + dev |
| App Service Plan | `sozo-plan` | Hosts both prod + dev apps |
| App Service | `sozo-app` | Production frontend |
| App Service | `sozo-app-dev` | Development frontend |
| Storage Account | `pfpuredatalake` | Raw CSV files in Blob |
| AI Search | `sozosearch602572` | Vector + keyword search |
| OpenAI | East US deployment | GPT-4o-mini + embeddings |

### Credentials

All credentials live in `.env.local` (never committed). Key variables:

```
# Azure SQL
SOZO_SQL_HOST=sozosql01729.database.windows.net
SOZO_SQL_DB=sozoapp          # or sozoapp-dev for dev
SOZO_SQL_USER=sozoadmin
SOZO_SQL_PASSWORD=<secret>

# Azure AI Search
SOZO_SEARCH_SERVICE_NAME=sozosearch602572
SOZO_SEARCH_ADMIN_KEY=<secret>
SOZO_SEARCH_INDEX_NAME=sozo-360-v1

# Azure OpenAI
SOZO_OPENAI_ENDPOINT=https://eastus.api.cognitive.microsoft.com
SOZO_OPENAI_API_KEY=<secret>
SOZO_OPENAI_CHAT_DEPLOYMENT=sozo-gpt4o-mini

# Anthropic (primary LLM)
ANTHROPIC_API_KEY=<secret>

# Auth (Microsoft Entra ID)
AUTH_SECRET=<secret>
AUTH_MICROSOFT_ENTRA_ID_ID=<secret>
AUTH_MICROSOFT_ENTRA_ID_SECRET=<secret>
AUTH_MICROSOFT_ENTRA_ID_ISSUER=<issuer-url>
```

Get `.env.local` from Eddie or the Azure Portal (App Service → Configuration).

---

## 4. Data Pipeline — End to End

The pipeline runs as Node.js scripts. There are two pipeline generations:

### Pipeline v1 (scripts/ingest/) — Original 7 Sources

These scripts process the original 7 data sources (Keap, Donor Direct, Givebutter, Bloomerang, Kindful, Stripe customers, Transaction Imports) stored in Azure Blob.

| Script | Purpose | Runtime | Status |
|--------|---------|---------|--------|
| `02_ingest_raw.js` | Blob CSVs → `raw.record` (JSON rows) | ~2 hours | DONE (3.1M rows) |
| `03_transform.js` | raw → silver typed tables | ~4 hours | DONE (steps 1-4), engagement partial |
| `04_resolve_identities.js` | 3-pass identity merge | ~1 hour | DONE |
| `05_build_serving.js` | Materialize 360 views | ~30 min | DONE |

### Pipeline v2 (scripts/pipeline/) — All 13 Sources + RAG

These scripts add the 6 newer sources (Mailchimp, full Stripe charges, WooCommerce, Tickera, Subbly, Shopify) and build the RAG layer.

| Script | Purpose | Runtime | Status |
|--------|---------|---------|--------|
| `load_local_to_bronze.js` | Local CSVs → bronze tables | ~45 min | DONE (237K rows) |
| `create_silver.js` | DDL for new silver tables | < 1 min | DONE |
| `load_silver.js` | Bronze → silver transform | ~3 hours | DONE (steps 1-36) |
| `resolve_identities_v2.js` | Extended identity resolution | ~1 hour | DONE |
| `create_serving_views.js` | Create/update serving views | < 5 min | DONE |
| `materialize_serving.js` | Views → indexed tables | ~1 hour | DONE |
| `create_search_index.js` | Azure AI Search index schema | < 1 min | DONE |
| `build_rag_index.js` | Person docs → embeddings → index | ~3.5 hours | DONE (89,143 docs) |

### How to Re-Run the Pipeline

If data changes (new CSV uploads, corrections), re-run from the appropriate step:

```bash
# Re-run just the silver transform for new sources (step 25+)
node scripts/pipeline/load_silver.js --from=25

# Re-run identity resolution (processes all contacts)
node scripts/pipeline/resolve_identities_v2.js

# Rebuild serving views + materialize
node scripts/pipeline/create_serving_views.js
node scripts/pipeline/materialize_serving.js

# Rebuild RAG index (generates embeddings — ~3.5 hours, ~$1 cost)
node scripts/pipeline/build_rag_index.js
```

### Resume Flags

Both transform scripts support resume:
- `--from=N` — skip to step N
- `--eng-from=N` — skip engagement sub-steps (v1 only)
- `--skip-tag` — skip tag materialization (largest table, 3M+ rows)
- `--only=name` — materialize only one specific view

---

## 5. Database Schema

### Production Database: `sozoapp`

**18 schemas, 170+ tables/views**:

| Schema | Tables | Purpose |
|--------|--------|---------|
| `keap` | 15+ | Bronze: Keap CRM raw data |
| `donor_direct_*` | 10+ | Bronze: Donor Direct raw data |
| `mailchimp` | 5 | Bronze: Mailchimp audience exports |
| `stripe_charges` | 7 | Bronze: Stripe yearly charge files |
| `woocommerce` | 2 | Bronze: WooCommerce customers + orders |
| `tickera` | 1 | Bronze: Event tickets |
| `subbly` | 2 | Bronze: Subscriptions + customers |
| `shopify` | 2 | Bronze: Shopify customers + orders |
| `silver` | 22 | Typed, cleaned, identity-resolved |
| `serving` | 18 | Pre-joined, materialized 360 views |
| `gold` | 10+ | Canonical entities (v1 pipeline) |
| `gold_intel` | 5 | Intelligence layer (scores, segments) |
| `meta` | 5+ | Lineage, mappings, signal rules |

### Key Silver Tables

| Table | Rows | What It Holds |
|-------|------|---------------|
| `silver.contact` | 120K | All people from all 13 sources |
| `silver.donation` | 67K | Donations (2014-2026) |
| `silver.stripe_charge` | 163K | Stripe payment history (2020-2026) |
| `silver.woo_order` | 67K | WooCommerce orders |
| `silver.event_ticket` | 21.5K | Event/tour tickets |
| `silver.subbly_subscription` | 2.4K | True Girl Box subscriptions |
| `silver.shopify_order` | 5K | Shopify orders |
| `silver.identity_map` | 120K | Maps every contact → master person ID |
| `silver.contact_tag` | 330K | Keap tag assignments |
| `silver.generic_tag` | 80K+ | Mailchimp + Shopify tags |

### Key Serving Views (What the LLM Queries)

| View | Rows | Used For |
|------|------|----------|
| `serving.person_360` | 89K | Master person record — all aggregates |
| `serving.household_360` | 55K | Household-level giving |
| `serving.donation_detail` | 66K | Individual donation records |
| `serving.donor_summary` | 5K | Lifetime donor metrics |
| `serving.donor_monthly` | 62K | Monthly giving trends |
| `serving.tag_detail` | 3M+ | All tags across all sources |
| `serving.order_detail` | 205K | Keap commerce orders |
| `serving.subscription_detail` | 3.5K | Active/cancelled subscriptions |
| `serving.event_detail` | 21K | Event attendance + tickets |
| `serving.stripe_charge_detail` | 163K | Stripe payment detail |
| `serving.woo_order_detail` | 67K | WooCommerce order detail |
| `serving.communication_detail` | 24K | Email/SMS logs |
| `serving.wealth_screening` | 1.1K | Donor capacity estimates |
| `serving.lost_recurring_donors` | 383 | Lost recurring revenue |

---

## 6. Identity Resolution

The identity resolution system merges 120K contact records from 13 sources into 89K unique people.

### Algorithm (4 Phases)

```
Phase 1: Cross-Reference Match (GiveButter has keap_number + dd_number columns)
    → Merges GB contacts with their Keap/DD counterparts

Phase 2: Email Match (confidence 0.99)
    → Groups all contacts sharing the same normalized email

Phase 3: Phone Match (confidence 0.95)
    → For unmatched contacts, groups by normalized phone number

Phase 4: Name + ZIP Match (confidence 0.80)
    → Last resort: first_name + last_name + postal_code
    → Flagged as low-confidence
```

### Output

- `silver.identity_map` — One row per source contact, with `master_id` (unified person ID) and `is_primary` flag
- Primary record selection priority: Keap > Donor Direct > Givebutter > WooCommerce > Shopify > Subbly > Tickera > Mailchimp
- All serving views JOIN through `identity_map` to show unified person data

### Source Priority (For Display Name, Address, etc.)

When the same person exists in multiple systems, the "primary" record determines which name/address is shown:

1. **Keap** (richest CRM data)
2. **Donor Direct** (fundraising detail)
3. **Givebutter** (recent giving)
4. **WooCommerce** (commerce)
5. **Shopify** (commerce)
6. **Subbly** (subscriptions)
7. **Tickera** (events)
8. **Mailchimp** (email only)

---

## 7. Serving Layer

The serving layer is what the LLM and dashboard query. All views are **materialized as indexed tables** for performance (the low-DTU database can't handle complex JOINs at query time).

### Materialization Process

```
View Definition (CREATE VIEW) → SELECT INTO table → CREATE INDEXES
```

Materialization script: `scripts/pipeline/materialize_serving.js`

Order matters (some views depend on others):
1. `donation_detail` (base donations)
2. `donor_summary` (aggregates from donation_detail)
3. `donor_monthly` (monthly rollup from donation_detail)
4. `person_360` (master person with all aggregates)
5. All others in any order

### Adding a New Serving View

1. Add the view SQL to `scripts/pipeline/create_serving_views.js`
2. Add to `VIEWS_IN_ORDER` in `scripts/pipeline/materialize_serving.js`
3. Add to `ALLOWED_TABLES` in `src/lib/server/sql-guard.ts`
4. Add description to `src/lib/server/schema-context.ts`
5. Run: `node scripts/pipeline/create_serving_views.js && node scripts/pipeline/materialize_serving.js --only=your_new_view`

---

## 8. RAG Layer — Semantic Search

The RAG (Retrieval-Augmented Generation) layer enables the LLM to answer semantic questions like "find donors interested in Bible studies who also attend events."

### How It Works

1. For each of 89,143 people, a **rich text document** is generated:
   ```
   PERSON: Jane Smith
   EMAIL: jane@example.com
   LOCATION: Nashville, TN 37209
   LIFECYCLE: Active Donor
   GIVING: $5,432 across 23 gifts. First: Mar 2019, Last: Nov 2024.
   COMMERCE: 8 Keap orders ($456), 3 WooCommerce orders ($89).
   EVENTS: Crazy Hair Tour 2025 (Nashville) — checked in.
   SUBSCRIPTIONS: True Girl Box (Subbly) — Active since Jul 2025.
   ENGAGEMENT: 47 tags: Bible Study, True Woman, Crazy Hair Tour.
   STRIPE: 15 charges ($1,234 total). Card: Visa ending 4242.
   ```

2. Each document is **embedded** using OpenAI `text-embedding-3-small` (1536 dimensions)

3. Documents + embeddings are pushed to **Azure AI Search** index `sozo-360-v1`

4. At query time, the LLM's `search_data` tool does **hybrid search** (keyword + vector):
   - User's question is embedded → vector similarity search
   - Combined with keyword matching
   - Returns top-N matching person profiles

### Index Schema

| Field | Type | Purpose |
|-------|------|---------|
| `id` | String (key) | Unique document ID |
| `person_id` | Int32 | Links to serving.person_360 |
| `display_name` | String | Searchable name |
| `email` | String | Searchable email |
| `location` | String | City, State, ZIP |
| `lifecycle_stage` | String | Filterable lifecycle |
| `content` | String | Full text description |
| `content_vector` | Vector(1536) | Embedding for semantic search |
| `giving_total` | Double | Filterable giving amount |
| `tags_text` | String | Searchable tag list |

### Rebuilding the Index

```bash
# Full rebuild (~3.5 hours, ~$1 in embedding costs)
node scripts/pipeline/build_rag_index.js

# Resume from person ID N
node scripts/pipeline/build_rag_index.js --from=50000
```

---

## 9. Frontend Application

### Layout

```
┌──────────────────────────────────────────────────────────┐
│  TopNav (logo, user menu, theme)                         │
├───────────────┬──────────────────────────────────────────┤
│               │                                          │
│   Chat Panel  │        Dashboard Canvas                  │
│   (440px)     │     (draggable widget grid)              │
│               │                                          │
│  - Messages   │   ┌─────────┐  ┌─────────┐              │
│  - Input      │   │  KPI    │  │  Chart  │              │
│  - Suggested  │   │  Widget │  │  Widget │              │
│    prompts    │   └─────────┘  └─────────┘              │
│               │   ┌──────────────────────┐              │
│  [Export]     │   │   Table Widget       │              │
│  [SQL view]   │   │   (drill-down)       │              │
│  [Pin +]      │   └──────────────────────┘              │
│               │                                          │
├───────────────┴──────────────────────────────────────────┤
│  Input: "Ask anything..."                    [Send]      │
└──────────────────────────────────────────────────────────┘
```

### Key Components

| Component | File | Purpose |
|-----------|------|---------|
| `ChatPanel` | `src/components/chat/ChatPanel.tsx` | Chat UI — messages, input, tool call indicators |
| `DashboardCanvas` | `src/components/dashboard/DashboardCanvas.tsx` | Draggable grid of pinned widgets |
| `WidgetRenderer` | `src/components/widgets/WidgetRenderer.tsx` | Routes widget type → specific renderer |
| `WidgetCard` | `src/components/widgets/WidgetCard.tsx` | Widget wrapper: title, SQL, Export, Pin, Remove |

### Widget Types (10)

| Type | Renderer | Best For |
|------|----------|----------|
| `kpi` | KpiWidget | Single metric with trend |
| `bar_chart` | BarChartWidget | Comparisons (top donors, fund breakdown) |
| `line_chart` | LineChartWidget | Trends over time |
| `area_chart` | AreaChartWidget | Cumulative trends |
| `donut_chart` | DonutChartWidget | Percentage breakdowns |
| `table` | TableWidget | Simple tabular data |
| `drill_down_table` | DrillDownTableWidget | Expandable rows (summary → detail) |
| `funnel` | FunnelWidget | Pipeline/lifecycle stages |
| `stat_grid` | StatGridWidget | Multiple KPIs in a grid |
| `text` | TextWidget | Markdown narrative |

### Export Feature

Every widget with data shows an **Export** dropdown (next to the SQL button):
- **CSV** — Native comma-separated values
- **Excel (.xlsx)** — Formatted workbook via SheetJS
- **PDF** — Visual snapshot via html2canvas + jsPDF

Code: `src/lib/export.ts` + `src/components/widgets/WidgetCard.tsx`

### State Management

- **Zustand** store (`src/lib/stores/dashboard-store.ts`) for dashboard state
- Widgets array + layouts persisted in localStorage
- Dashboard save/load via API routes → Azure SQL

---

## 10. LLM Chat System

### Three Tools

The LLM has three tools it can call:

| Tool | Purpose | When Used |
|------|---------|-----------|
| `query_data` | Execute read-only T-SQL on serving views | Numbers, counts, sums, trends, rankings |
| `search_data` | Hybrid semantic search on person profiles | Find people by behavior, discover patterns |
| `show_widget` | Display interactive chart/table/KPI | Visualize results from either tool |

### Query Flow

```
User: "Top 20 donors this year"
  ↓
LLM decides: This is a NUMBERS question → use query_data
  ↓
query_data: SELECT TOP(20) display_name, lifetime_giving...
  ↓
Result: [{name: "Lampe", amount: 790000}, ...]
  ↓
show_widget: type=drill_down_table, groupKey=display_name
  ↓
User sees: Interactive drill-down table with expand/collapse
```

### SQL Guardrails (`src/lib/server/sql-guard.ts`)

- **Only SELECT** — no INSERT, UPDATE, DELETE, DROP, ALTER
- **Only serving/silver/gold tables** — 60 allowed tables whitelisted
- **Max 500 rows** — auto-injects TOP(500) if missing
- **90-second timeout** — prevents runaway queries
- **No semicolons** — blocks statement chaining
- **No comments** — blocks `--` and `/*` injection

### System Prompt Key Facts

The LLM's system prompt (`src/app/api/chat/route.ts`) includes:
- 89,143 unique people across 13 sources
- 5,037 donors, 362 active (gave last 6 months)
- Top 5 donors: Lampe $790K, Fletcher $383K, Stober $226K
- Dec = 25% of annual giving; Nov-Dec = 34%
- 383 lost recurring donors = $17K/month MRR lost
- 163K Stripe charges, 205K Keap orders, 21.5K event tickets
- Full schema descriptions for all 18 serving views

### Model Priority

```javascript
// Primary → Fallback chain
1. OpenAI (gpt-5.2)
2. Anthropic Claude (claude-sonnet-4-5)
3. Azure OpenAI (sozo-gpt4o-mini)
```

---

## 11. CI/CD & Branch Workflow

### Branch Strategy

```
main ─── protected (production)
 └── dev ─── integration (development)
      ├── feature/your-feature
      ├── fix/bug-description
      └── chore/task-name
```

### Branch Policies on `main` (Azure DevOps)

| Policy | Setting |
|--------|---------|
| Minimum reviewers | 1 required |
| Comment resolution | All must be resolved |
| Merge strategy | Required |
| Direct push | Blocked |

### Developer Workflow

```bash
# 1. Start from latest dev
git checkout dev && git pull

# 2. Create feature branch
git checkout -b feature/my-feature

# 3. Work, commit
git add -A && git commit -m "Add feature"

# 4. Push and open PR → dev
git push origin feature/my-feature
# Open PR in Azure DevOps

# 5. After approval + merge → dev auto-deploys to sozo-app-dev

# 6. When stable, PR from dev → main → auto-deploys to production
```

### CI/CD Pipeline (`azure-pipelines.yml`)

```
Push to main → Build → Deploy to sozo-app (production)
Push to dev  → Build → Deploy to sozo-app-dev (development)
```

Build steps: `npm ci` → `npm run build` → copy static → zip → deploy to Azure App Service.

**One-time setup needed**: Go to Azure DevOps → Pipelines → New Pipeline → select "Existing YAML file" → `/azure-pipelines.yml`. Also needs an Azure service connection named "Azure subscription".

### Git Remotes

| Remote | URL | Purpose |
|--------|-----|---------|
| `origin` | `github.com/hrlm-church/sozo.git` | GitHub (backup) |
| `azure` | `dev.azure.com/purefreedom-devops/Data Analytics/_git/Data Analytics` | Azure DevOps (primary) |

---

## 12. Critical Constraints & Gotchas

### Azure SQL Low DTU — The #1 Constraint

The production database runs on Standard tier (20 DTU). This is extremely limited and drives most architectural decisions:

| What Fails | Why | Workaround |
|------------|-----|------------|
| Server-side JSON parsing (`JSON_VALUE`, `OPENJSON`) | Too CPU-intensive | Parse JSON in Node.js, send typed INSERTs |
| BCP bulk insert | Column mapping fails | Use `INSERT INTO ... VALUES (...)` in 100-row chunks |
| Complex JOINs at query time | Timeout on large tables | Materialize all serving views as indexed tables |
| Concurrent pipeline runs | Lock contention + duplicates | **NEVER** run two instances simultaneously |
| Large batch inserts | DTU exhaustion | `wait(300)` between batches |

### Zombie Process Prevention

Previous sessions left stale Node.js processes running engagement scripts. **ALWAYS**:
```bash
ps aux | grep node    # Check for running processes
kill <pid>            # Kill any stale ones BEFORE starting new work
```

### SQL Gotchas

- `commerce.[order]` needs brackets — `order` is a SQL reserved word
- `TRUNCATE` fails with inbound FK — must clear child tables first
- TRUNCATE timeouts are usually lock contention, not data volume
- `lit()` function handles SQL encoding (NULL, numbers, dates, N'' strings)
- `amt()` clamps to ±9,999,999,999.99 for DECIMAL(12,2) columns
- `dt()` formats dates as `YYYY-MM-DDThh:mm:ss` (no Z), range 1753-9999

### Identity Resolution Gotchas

- Linkage via `source_ref` format: `{source}:{type}:{id}:entity:{entity_id}`
- Use JOIN+SUBSTRING for source_ref matching (NOT CROSS APPLY+LIKE)
- Batched note linkage: `UPDATE TOP(50000)` in a loop for 360K+ rows
- Household grouping: address + 3-char last name prefix overlap

### Data Quality Issues Known

- Some Keap contacts have "Unknown" display names — always filter `WHERE display_name <> 'Unknown'`
- Donor Direct phone field is `TelephoneNumber` (NOT `NumericTelephoneNumber`)
- WooCommerce uses email as source_id (no separate customer ID)
- Subbly has test accounts (filter `test@purefreedom.org`)

---

## 13. File Reference

### Application Code

| Path | Purpose |
|------|---------|
| `src/app/page.tsx` | Main page — two-panel layout (chat + dashboard) |
| `src/app/api/chat/route.ts` | Chat endpoint — LLM system prompt + tool routing |
| `src/app/api/health/route.ts` | Health check (SQL, Search, Storage, OpenAI) |
| `src/app/api/dashboard/*/route.ts` | Dashboard save/load/delete |
| `src/components/chat/ChatPanel.tsx` | Chat UI |
| `src/components/widgets/WidgetCard.tsx` | Widget wrapper (SQL, Export, Pin) |
| `src/components/widgets/WidgetRenderer.tsx` | Widget type → renderer dispatch |
| `src/lib/server/tools.ts` | LLM tool definitions (query_data, search_data, show_widget) |
| `src/lib/server/sql-guard.ts` | SQL injection prevention + table whitelist |
| `src/lib/server/schema-context.ts` | Schema metadata given to LLM |
| `src/lib/server/search-client.ts` | Azure AI Search hybrid search |
| `src/lib/export.ts` | CSV/XLS/PDF export utilities |
| `src/auth.ts` | NextAuth config (Microsoft Entra ID) |
| `middleware.ts` | Auth middleware |

### Pipeline Scripts (Active)

| Path | Purpose |
|------|---------|
| `scripts/pipeline/load_local_to_bronze.js` | Local CSV → bronze tables |
| `scripts/pipeline/load_bronze_to_sql.js` | Blob CSV → bronze tables |
| `scripts/pipeline/create_silver.js` | Silver table DDL |
| `scripts/pipeline/load_silver.js` | Bronze → silver transform (36 steps) |
| `scripts/pipeline/resolve_identities_v2.js` | 4-phase identity resolution |
| `scripts/pipeline/create_serving_views.js` | Serving view definitions |
| `scripts/pipeline/materialize_serving.js` | Materialize views → indexed tables |
| `scripts/pipeline/create_search_index.js` | Azure AI Search index schema |
| `scripts/pipeline/build_rag_index.js` | Person docs → embeddings → search index |
| `scripts/pipeline/_db.js` | Shared DB utilities |

### Configuration

| Path | Purpose |
|------|---------|
| `.env.local` | All credentials (NEVER commit) |
| `next.config.ts` | Next.js config (standalone output, React compiler) |
| `azure-pipelines.yml` | CI/CD pipeline definition |
| `CONTRIBUTING.md` | Developer workflow guide |

---

## 14. What Was Built & Current State

### Completed Work (Chronological)

1. **Schema Design** — 11 schemas, 33+ domain tables with FK relationships
2. **Raw Ingest** — 3.1M rows from 53 CSV files across 7 original sources
3. **Transform Pipeline** — Client-side JSON parsing, 36 transform steps
4. **6 New Sources** — Mailchimp (76K), full Stripe (163K), WooCommerce (90K), Tickera (21.5K), Subbly (4.6K), Shopify (15K)
5. **Identity Resolution** — 4-phase merge: 120K contacts → 89K unique people
6. **Serving Layer** — 18 pre-joined materialized views with indexes
7. **RAG Layer** — 89,143 person documents with vector embeddings in Azure AI Search
8. **Chat Interface** — 3-tool LLM (SQL + Search + Visualization)
9. **Dashboard** — Draggable widget grid with 10 widget types
10. **Export** — CSV/Excel/PDF from any widget
11. **Auth** — Microsoft Entra ID SSO
12. **Deployment** — Azure App Service (prod + dev)
13. **CI/CD** — Azure DevOps pipeline, branch protection, PR workflow
14. **Intelligence Reports** — Strategic 360, behavioral analysis, microsegmentation

### Current Data Statistics

| Metric | Value |
|--------|-------|
| Total raw rows | 3,400,000+ |
| Unique people | 89,143 |
| Source systems | 13 |
| Donors (ever) | 5,037 |
| Active donors (6mo) | 362 |
| Total giving tracked | $6.7M |
| Stripe charges | 163K ($6.75M) |
| Commerce orders | 205K (Keap) + 67K (Woo) + 5K (Shopify) |
| Event tickets | 21,510 |
| Active subscriptions | 1,584 (Subbly) |
| Tags | 5.7M assignments |
| RAG documents | 89,143 |
| Serving views | 18 (all materialized) |

### Known Limitations

- **DTU constraint** — pipeline runs are slow (~8 hours end-to-end); concurrent runs will fail
- **No incremental refresh** — pipeline does full reload (truncate + reinsert); delta/CDC not implemented
- **No automated scheduling** — pipeline runs manually; no cron/ADF/Airflow
- **Single RAG index** — no versioning; rebuild overwrites
- **Auth exemption** — `/api/chat` is currently exempt from auth (TODO: enforce)
- **No test suite** — no unit tests, integration tests, or E2E tests

### Recommended Next Steps

1. **Enforce auth on chat endpoint** — remove exemption in `src/auth.ts`
2. **Add automated tests** — especially for SQL guard and identity resolution
3. **Implement incremental refresh** — use `modified_since` or CDC to avoid full reload
4. **Add pipeline scheduling** — cron job or Azure Data Factory for nightly refresh
5. **Upgrade DTU tier** — Standard 50+ would remove most workarounds
6. **Add monitoring** — Application Insights for error tracking and performance
7. **Implement audit logging** — track which queries users run, for compliance

---

*This document covers the full state of the Sozo platform as of February 19, 2026. For the developer workflow, see `CONTRIBUTING.md`. For architecture Q&A, see `docs/sozo_architecture_answers.md`.*
