# Sozo Engineering Handover

## 1. What Is Sozo?

Sozo is an AI-powered donor intelligence platform for Pure Freedom Ministries (True Girl brand). It merges **13 data sources** into unified person profiles and provides a **conversational AI interface** that can answer any question about the ministry's 100K+ contacts.

The AI has two data tools:
- **SQL** — for quantitative questions (counts, sums, trends, rankings)
- **Semantic search** — for qualitative discovery (find people by behavior, cross-stream patterns)

Plus a **visualization tool** that renders interactive charts, tables, and KPIs inline in the chat.

Sozo is NOT a fixed dashboard. It generates custom SQL and search queries on the fly for whatever you ask. There are no pre-built reports — the LLM reasons about your question and constructs the right approach.

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────┐
│  RAW DATA (13 sources)                                  │
│  Azure Blob Storage + Local CSV files (external drive)  │
└──────────────────────┬──────────────────────────────────┘
                       ↓
┌──────────────────────────────────────────────────────────┐
│  BRONZE LAYER (Azure SQL — per-source schemas)           │
│  All columns NVARCHAR(MAX), zero transformation          │
│  ~3.4M rows (blob) + ~292K rows (local CSV)              │
└──────────────────────┬──────────────────────────────────┘
                       ↓  Node.js client-side parsing
┌──────────────────────────────────────────────────────────┐
│  SILVER LAYER (Azure SQL — typed, normalized tables)     │
│  silver.contact, silver.donation, silver.stripe_charge,  │
│  silver.woo_order, silver.event_ticket, etc.             │
│  ~500K+ rows across 20+ tables                           │
└──────────────────────┬──────────────────────────────────┘
                       ↓  Union-Find identity resolution
┌──────────────────────────────────────────────────────────┐
│  IDENTITY MAP (silver.identity_map)                      │
│  master_id → all source contact records                  │
│  ~200K contacts → ~85K unique people                     │
└──────────────────────┬──────────────────────────────────┘
                       ↓  Pre-joined view creation
┌──────────────────────────────────────────────────────────┐
│  SERVING LAYER (Azure SQL — materialized views)          │
│  serving.person_360, serving.donor_summary,              │
│  serving.donation_detail, serving.tag_detail, etc.       │
│  14 views, 4M+ total rows (materialized as tables)       │
└──────────────┬────────────────┬──────────────────────────┘
               ↓                ↓
┌──────────────────────┐  ┌────────────────────────────────┐
│  Azure AI Search     │  │  AI Chat API                   │
│  sozo-360-v1 index   │  │  POST /api/chat                │
│  100K+ person docs   │  │  3 tools: query_data,          │
│  Vector + keyword    │  │  search_data, show_widget      │
│  HNSW (1536-dim)     │  │  Models: GPT-4o / Claude       │
└──────────────────────┘  └────────────────────────────────┘
```

---

## 3. Data Sources (13 Systems)

### From Azure Blob Storage (original 7)
| System | Rows | Schema | What It Contains |
|--------|------|--------|-----------------|
| Keap (CRM) | 3.2M | `raw.record` | Contacts, orders, invoices, notes, tags, subscriptions, products |
| Donor Direct | 197K | `raw.record` | Donor accounts, transactions, emails, phones, addresses |
| Givebutter | 15K | `raw.record` | Donations, supporters, activity |
| Bloomerang | 8.5K | `raw.record` | Donor data |
| Kindful | misc | `raw.record` | Donation imports |
| Stripe (customers) | 6.8K | `raw.record` | Customer summaries from blob |
| Transaction Imports | misc | `raw.record` | Legacy donation imports |

### From External Drive (6 new sources)
| System | Rows | Schema | What It Contains |
|--------|------|--------|-----------------|
| Mailchimp | 76K | `mailchimp.*` | 5 audience files (subscribed, unsubscribed, cleaned, SMS, non-subscribed) + 1.8M tag assignments |
| Stripe (charges) | 163K | `stripe_charges.*` | Full charge history 2020-2026 (7 yearly files) |
| WooCommerce | 89K | `woocommerce.*` | Customers (21K) + order line items (67K) |
| Tickera | 19K | `tickera.*` | Event tickets with attendee + buyer info |
| Subbly | 4.5K | `subbly.*` | True Girl box customers (2.3K) + subscriptions (2.4K) |
| Shopify | 15K | `shopify.*` | Customers (10K) + order line items (5K) |

---

## 4. Database Schema (sozov2)

Server: `sozosql01729.database.windows.net`
Database: `sozov2`
User: `sozoadmin`

### Bronze Layer (raw data, NVARCHAR(MAX))
- `raw.record` — all blob CSV rows as JSON (3.4M rows)
- `mailchimp.*` — 5 audience tables
- `stripe_charges.*` — 7 yearly charge tables
- `woocommerce.*` — customers, order_lines
- `tickera.*` — tickets
- `subbly.*` — customers, subscriptions
- `shopify.*` — customers, order_lines

### Silver Layer (typed, normalized)
| Table | Rows | Description |
|-------|------|-------------|
| `silver.contact` | 200K+ | All people from all sources (source_system, source_id, name, email, phone, address) |
| `silver.donation` | 67K | All donations (Donor Direct, Givebutter, Kindful, Keap imports) |
| `silver.stripe_charge` | 163K | Full Stripe charge history 2020-2026 |
| `silver.woo_order` | 67K | WooCommerce order line items |
| `silver.event_ticket` | 19K | Tickera event tickets (attendee + buyer) |
| `silver.subbly_subscription` | 2.4K | True Girl subscription box data |
| `silver.shopify_order` | 5K | Shopify order line items |
| `silver.generic_tag` | 1.8M+ | Tags from Mailchimp + Shopify |
| `silver.contact_tag` | 3M+ | Keap tag assignments |
| `silver.tag` | ~600 | Keap tag definitions |
| `silver.note` | 360K+ | Keap + DD notes |
| `silver.communication` | 24K | DD + GB communications |
| `silver.[order]` | 205K | Keap commerce orders (brackets required — reserved word) |
| `silver.order_item` | 560K | Keap order line items |
| `silver.invoice` | 205K | Keap invoices |
| `silver.payment` | 135K | Keap payments |
| `silver.subscription` | 8K | Keap subscriptions |
| `silver.product` | ~700 | Keap products |
| `silver.company` | ~3K | Keap companies |
| `silver.stripe_customer` | 6.8K | Stripe customer summaries |
| `silver.contact_email` | ~120K | DD enrichment emails |
| `silver.contact_phone` | ~50K | DD enrichment phones |
| `silver.contact_address` | ~50K | DD enrichment addresses |
| `silver.identity_map` | 200K+ | master_id → contact_id linkage |

### Serving Layer (pre-joined, materialized)
| View | Rows | Description |
|------|------|-------------|
| `serving.person_360` | 85K+ | Full person profile with aggregates from all sources |
| `serving.household_360` | 55K | Household-level aggregates |
| `serving.donor_summary` | 5K | Donor-level giving aggregates |
| `serving.donor_monthly` | 62K | Monthly giving time series per donor |
| `serving.donation_detail` | 66K | Individual donations with fund, appeal, source |
| `serving.tag_detail` | 4M+ | All tags (Keap + Mailchimp + Shopify) |
| `serving.order_detail` | 205K | Keap commerce orders |
| `serving.payment_detail` | 135K | Keap payments |
| `serving.invoice_detail` | 205K | Keap invoices |
| `serving.subscription_detail` | 8K+ | Keap + Subbly subscriptions |
| `serving.event_detail` | 19K | Tickera event tickets |
| `serving.stripe_charge_detail` | 163K | Full Stripe charge history |
| `serving.woo_order_detail` | 67K | WooCommerce orders |
| `serving.communication_detail` | 24K | Communications |
| `serving.wealth_screening` | 1.1K | Donor capacity analysis |
| `serving.lost_recurring_donors` | 383 | Lost MRR from platform migration |
| `serving.stripe_customer` | 6.8K | Stripe customer summaries |

---

## 5. Identity Resolution

### The Problem
The same person appears in 5+ systems with different IDs. "Jane Smith" might be:
- Keap contact #12345
- Donor Direct account #DD-789
- Mailchimp LEID 987654
- Stripe customer cus_abc123
- WooCommerce email jane@example.com

### The Algorithm (4-phase Union-Find)
1. **Cross-reference match** — Explicit links between systems (e.g., Keap ID stored in DD records)
2. **Email match** — Same email across any two systems → merge into one person
3. **Phone match** — Same phone across any two systems → merge
4. **Name + ZIP match** — Same first+last name + ZIP code → merge (fuzzy fallback)

### Result
- ~200K+ contact records across 13 systems → ~85K unique people
- Each unique person gets a `master_id` in `silver.identity_map`
- Source priority order: Keap (0) > Donor Direct (1) > Givebutter (2) > WooCommerce (3) > Shopify (4) > Subbly (5) > Tickera (6) > Mailchimp (7)
- The highest-priority source's contact record becomes the `is_primary = 1` record (used for display_name, address, etc.)

### How New Sources Connect
Sources without a `contact_id` in `silver.contact` (Stripe charges, WooCommerce orders) connect through **email-based lookup**:
```sql
-- Example: WooCommerce → person via email
OUTER APPLY (
  SELECT TOP 1 im.master_id
  FROM silver.contact c
  JOIN silver.identity_map im ON im.contact_id = c.contact_id
  WHERE c.email_primary = wo.customer_email
) el
```

---

## 6. RAG Layer (Vector Search)

### Purpose
SQL answers quantitative questions. But it cannot answer semantic questions like:
- "Find donors who are interested in Bible studies"
- "Who are our most loyal multi-channel supporters?"
- "Why are subscribers canceling?"

The RAG layer enables these by creating vector embeddings of person profiles.

### Components

**Azure AI Search Index: `sozo-360-v1`**
- 14 fields including `content_vector` (1536 dimensions)
- HNSW algorithm for approximate nearest neighbor search
- Hybrid search: keyword relevance + vector similarity combined

**Document Generation** (`build_rag_index.js`)
For each person, generates a rich text document:
```
PERSON: Jane Smith
EMAIL: jane@example.com
LOCATION: Nashville, TN 37209
LIFECYCLE: Active Donor
GIVING: $5,432 across 23 gifts (avg $236)...
COMMERCE: 8 Keap orders ($456). 3 WooCommerce orders ($89).
EVENTS: Crazy Hair Tour 2025 — checked in.
SUBSCRIPTIONS: True Girl Box — Active.
TAGS (47): Bible Study, True Woman, Crazy Hair Tour...
```

**Embedding Model**: OpenAI `text-embedding-3-small` (1536 dimensions)
- Each document is embedded and stored in Azure AI Search
- Query embedding + keyword search combined for hybrid results

**Search Client** (`src/lib/server/search-client.ts`)
- `hybridSearch(query, top, filter)` — sends both keyword and vector queries
- Falls back to keyword-only if embedding fails
- OData filter support for structured filtering (e.g., `giving_total gt 1000`)

---

## 7. AI Chat Architecture

### API Route: `POST /api/chat`
File: `src/app/api/chat/route.ts`

### 3 Tools Available to the LLM

**1. `query_data`** — Execute read-only T-SQL
- Writes custom SQL on the fly (not pre-built queries)
- SQL validated by `sql-guard.ts`: blocks mutations, enforces TOP limits, allowlists tables
- Up to 500 rows returned per query
- 90-second timeout

**2. `search_data`** — Semantic search across person profiles
- Hybrid keyword + vector search against Azure AI Search
- Returns ranked person profiles with relevance scores
- Supports OData filters for structured constraints

**3. `show_widget`** — Render interactive visualizations
- Types: kpi, bar_chart, line_chart, area_chart, donut_chart, table, drill_down_table, funnel, stat_grid, text
- Data flows from query_data or search_data automatically
- Widget appears inline in chat response

### Model Chain
Configured in `src/lib/server/ai-provider.ts`:
- Primary: OpenAI GPT-4o-mini (via Azure)
- Can swap to Claude or other providers

### System Prompt
The LLM gets:
- Ministry context (org facts, data stats, key metrics)
- Schema documentation (all serving views with column names and row counts)
- SQL rules (no JOINs needed, use serving views, formatting rules)
- Widget selection guide (which chart type for which question)
- Tool usage workflow guidance

---

## 8. Azure Infrastructure

| Resource | Name | Purpose |
|----------|------|---------|
| SQL Server | `sozosql01729.database.windows.net` | Database server (Low DTU tier) |
| Database | `sozov2` | Main data warehouse |
| Blob Storage | `pfpuredatalake` | Raw CSV files from 7 original sources |
| AI Search | `sozosearch602572` | Vector + keyword search index |
| OpenAI | `eastus.api.cognitive.microsoft.com` | Chat model + embeddings |
| Key Vault | `sozokv00502` | Secrets management |

### Critical: Low DTU Constraints
The Azure SQL tier is low-cost with limited compute:
- **Batch inserts**: 50-100 rows per INSERT, 200-300ms wait between batches
- **No server-side JSON**: JSON_VALUE / OPENJSON are too slow — all parsing in Node.js
- **No BCP**: Fails with "Invalid column type" errors
- **Keyset pagination**: `WHERE id > @lastId ORDER BY id` for efficient batched reads
- **Amount clamping**: `amt()` function clamps to ±9,999,999,999.99 for DECIMAL(12,2)
- **Never run two pipeline instances simultaneously**: causes duplicate inserts + lock contention

---

## 9. Pipeline Scripts

All scripts in `scripts/pipeline/`. Run with `node scripts/pipeline/<name>.js`.

### Bronze Load
| Script | Purpose |
|--------|---------|
| `load_bronze_to_sql.js` | Load CSVs from Azure Blob → bronze (raw.record) |
| `load_local_to_bronze.js` | Load CSVs from external drive → bronze schemas |

### Silver Transform
| Script | Purpose |
|--------|---------|
| `create_silver.js` | Create silver table DDL + indexes |
| `load_silver.js` | Transform bronze → silver (36 STEPS) |

### Identity Resolution
| Script | Purpose |
|--------|---------|
| `resolve_identities_v2.js` | 4-phase Union-Find matching, creates identity_map |

### Serving Layer
| Script | Purpose |
|--------|---------|
| `create_serving_views.js` | Create 14+ pre-joined serving views |
| `materialize_serving.js` | Materialize views → indexed tables |

### RAG Layer
| Script | Purpose |
|--------|---------|
| `create_search_index.js` | Create Azure AI Search vector index |
| `build_rag_index.js` | Generate person docs, embed, push to search |

### Resume Flags
Most scripts support `--from=N` to skip completed steps:
```bash
# Resume silver transform from step 25
node scripts/pipeline/load_silver.js --from=25

# Resume bronze load from source 2 (skip source 1)
node scripts/pipeline/load_local_to_bronze.js --from=1

# Resume RAG indexing from person_id 5000
node scripts/pipeline/build_rag_index.js --from=5000
```

---

## 10. Full Pipeline Runbook

Run in order, one at a time. Each step must complete before the next starts.

```bash
cd "/Users/eddiemenezes/Documents/New project/sozo"

# ── Phase 1: Bronze Load ──────────────────────────────
# Load from Azure Blob (original 7 sources — already done)
node scripts/pipeline/load_bronze_to_sql.js

# Load from external drive (6 new sources — ~45min)
node scripts/pipeline/load_local_to_bronze.js

# ── Phase 2: Silver Transform ─────────────────────────
# Create typed tables (< 1min)
node scripts/pipeline/create_silver.js

# Transform all sources (steps 1-36, ~2-3 hours)
node scripts/pipeline/load_silver.js

# ── Phase 3: Identity Resolution ──────────────────────
# Merge contacts across all 13 sources (~30-60min)
node scripts/pipeline/resolve_identities_v2.js

# ── Phase 4: Serving Layer ────────────────────────────
# Create pre-joined views (< 5min)
node scripts/pipeline/create_serving_views.js

# Materialize views to indexed tables (~30-60min)
node scripts/pipeline/materialize_serving.js

# ── Phase 5: RAG Layer ────────────────────────────────
# Create vector search index (< 1min)
node scripts/pipeline/create_search_index.js

# Build person 360 documents + embeddings (~2-3 hours)
node scripts/pipeline/build_rag_index.js

# ── Phase 6: Verify ──────────────────────────────────
npm run dev
# Open http://localhost:3000 and test chat
```

---

## 11. Environment Variables

File: `.env.local` (never committed to git)

```bash
# Azure SQL
SOZO_SQL_HOST=sozosql01729.database.windows.net
SOZO_SQL_USER=sozoadmin
SOZO_SQL_PASSWORD=<password>

# Azure Blob Storage
SOZO_STORAGE_ACCOUNT=pfpuredatalake
SOZO_STORAGE_ACCOUNT_KEY=<key>

# Azure AI Search
SOZO_SEARCH_SERVICE_NAME=sozosearch602572
SOZO_SEARCH_ADMIN_KEY=<key>
SOZO_SEARCH_INDEX_NAME=sozo-360-v1

# Azure OpenAI (chat model)
SOZO_OPENAI_ENDPOINT=https://eastus.api.cognitive.microsoft.com
SOZO_OPENAI_API_KEY=<key>
SOZO_OPENAI_CHAT_DEPLOYMENT=sozo-gpt4o-mini
SOZO_OPENAI_API_VERSION=2024-08-01-preview

# OpenAI (embeddings — uses OpenAI API directly, not Azure)
OPENAI_API_KEY=<key>

# Auth
NEXTAUTH_SECRET=<secret>
NEXTAUTH_URL=http://localhost:3000
```

---

## 12. Tech Stack

| Layer | Technology |
|-------|-----------|
| Frontend | Next.js 16, React 19, Tailwind CSS |
| Backend | Next.js API Routes (serverless) |
| Database | Azure SQL (Low DTU) |
| Search | Azure AI Search (vector + keyword) |
| AI Models | Azure OpenAI (GPT-4o-mini), OpenAI (text-embedding-3-small) |
| Auth | NextAuth.js |
| Deployment | Vercel |
| Pipeline | Node.js scripts (CommonJS) |
| AI SDK | Vercel AI SDK (`ai` package) |

---

## 13. Code Structure

```
src/
├── app/
│   ├── api/
│   │   ├── chat/route.ts          # AI chat endpoint (3 tools)
│   │   ├── dashboard/             # Save/load/delete dashboards
│   │   └── health/route.ts        # Health check
│   ├── page.tsx                   # Main chat UI
│   └── login/page.tsx             # Auth page
├── components/
│   ├── chat/                      # Chat message rendering
│   └── widgets/                   # Chart/table widget components
├── lib/
│   └── server/
│       ├── ai-provider.ts         # Model chain configuration
│       ├── tools.ts               # LLM tools (query_data, search_data, show_widget)
│       ├── sql-client.ts          # Azure SQL connection pool
│       ├── sql-guard.ts           # SQL validation + allowlisted tables
│       ├── schema-context.ts      # Schema documentation for LLM prompt
│       ├── search-client.ts       # Azure AI Search hybrid client
│       └── env.ts                 # Environment variable loading
├── types/
│   └── widget.ts                  # Widget type definitions
scripts/
├── pipeline/
│   ├── load_bronze_to_sql.js      # Blob → bronze
│   ├── load_local_to_bronze.js    # Local CSV → bronze
│   ├── create_silver.js           # Silver DDL
│   ├── load_silver.js             # Bronze → silver (36 steps)
│   ├── resolve_identities_v2.js   # Identity resolution
│   ├── create_serving_views.js    # Serving view definitions
│   ├── materialize_serving.js     # View → indexed table
│   ├── create_search_index.js     # Azure AI Search index
│   └── build_rag_index.js         # Person docs + embeddings
```

---

## 14. Key Patterns and Conventions

### loadEnv()
All pipeline scripts load `.env.local` manually (no dotenv package):
```javascript
function loadEnv() {
  const p = path.join(process.cwd(), '.env.local');
  for (const line of fs.readFileSync(p, 'utf8').split(/\r?\n/)) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) process.env[m[1]] = m[2].replace(/^["']|["']$/g, '');
  }
}
```

### Helper Functions (load_silver.js)
- `lit(val)` — SQL literal encoding (handles NULL, numbers, dates, strings with N'' prefix)
- `amt(val)` — Parse currency string to DECIMAL(12,2), clamped to ±9,999,999,999.99
- `dt(val)` — Parse date to `YYYY-MM-DDThh:mm:ss` format (range 1753-9999)
- `int(val)` — Parse integer
- `bit(val)` — Parse boolean ('Yes'/'True'/1 → 1, else 0)
- `clean(val)` — Trim whitespace, return null for empty
- `trunc(val, len)` — Truncate string to max length

### batchInsert Engine
Reads bronze tables in 1000-row batches via keyset pagination, applies a mapping function, inserts 100 rows at a time with 200ms wait:
```javascript
async function batchInsert(pool, targetTable, columns, mapFn, sourceTable, batchSize = 1000)
```

### SQL Guard (chat API)
- Blocks: DROP, ALTER, CREATE, INSERT, UPDATE, DELETE, EXEC
- Requires: SELECT or WITH (CTE) prefix
- Injects: TOP (500) if missing
- Allowlists: specific table names only
- Timeout: 90 seconds

---

## 15. Ministry Context

### Organization
- **Pure Freedom Ministries** — Christian discipleship nonprofit
- **True Girl** — primary brand (tween girls ministry)
- **Dannah Gresh** — founder, author, speaker

### Revenue/Engagement Streams
1. **Donations** — $6.7M lifetime, 5K donors, 369 active
2. **Commerce** — 205K Keap orders + 67K WooCommerce + 5K Shopify
3. **Subscription Boxes** — True Girl monthly box (Subbly), 6.3K total subscribers
4. **Tours & Events** — Pajama Party Tour, Crazy Hair Tour, B2BB Tour, Pop-Up Parties
5. **Bible Studies & Content** — B2BB (Born to Be Brave), Master Class, BFF Workshop

### Key Metrics
- 85K+ unique people across all systems
- 78 major donors ($10K+) = 68% of total giving
- 29K commerce buyers have never donated
- 383 lost recurring donors = $205K/year lost revenue
- December = 25% of annual giving

---

## 16. Known Constraints and Risks

### Technical
- Azure SQL low DTU → slow batch operations, no concurrent pipeline runs
- Streaming CSV parser needed for files >50K rows (OOM otherwise)
- Identity resolution runs in JS memory (~2GB for 200K contacts)
- TAG expansion can generate 1M+ rows (Mailchimp contacts average ~17 tags each)

### Data Quality
- Email coverage varies by source (Tickera has buyer email but attendee may lack it)
- Name parsing is best-effort (split on space, may fail for complex names)
- Some Keap contacts lack email → phone-only or name+ZIP matching
- Duplicate tag values across sources (Keap "True Girl" vs Mailchimp "True Girl")

### Operational
- NEVER run two pipeline instances simultaneously
- Always check `ps aux | grep node` before starting transforms
- TRUNCATE fails with foreign key constraints — use specific DELETE order
- Firewall rules may need updating when IP changes

---

## 17. Quick Reference

### Development
```bash
npm run dev          # Start dev server (http://localhost:3000)
npm run build        # Production build
npm run lint         # ESLint check
```

### Pipeline (run one at a time)
```bash
node scripts/pipeline/create_silver.js         # Create tables
node scripts/pipeline/load_silver.js --from=25  # Transform from step 25
node scripts/pipeline/resolve_identities_v2.js  # Identity merge
node scripts/pipeline/create_serving_views.js   # Create views
node scripts/pipeline/materialize_serving.js    # Materialize
node scripts/pipeline/create_search_index.js    # Create vector index
node scripts/pipeline/build_rag_index.js        # Build embeddings
```

### Diagnostics
```bash
curl -sS http://localhost:3000/api/health       # Check all services
```
