/**
 * Schema context for the LLM system prompt.
 * SERVING VIEWS are pre-joined — the LLM should NEVER need to write JOINs.
 */

export const SCHEMA_CONTEXT = `
## IMPORTANT: Always Use Serving Views (Pre-Joined — No JOINs Needed)

Every serving view already includes display_name, person_id, email.
NEVER JOIN tables manually — use these views instead.

### serving.person_360 — ALL people aggregates (84,507 rows)
person_id, display_name, first_name, last_name, email, phone,
city, state, postal_code, country, date_of_birth, gender, spouse_name,
household_name, organization_name, primary_source,
source_count, source_systems (CSV of source systems),
donation_count, lifetime_giving, avg_gift, largest_gift,
first_gift_date, last_gift_date, recency_days,
order_count, total_spent,
tag_count, note_count, comm_count,
lifecycle_stage — VALUES: 'prospect' (most), 'active', 'cooling', 'lapsed', 'lost',
created_at

### serving.household_360 — Household aggregates (55,625 rows)
household_id, name, member_count,
household_giving_total,
giving_trend — VALUES: 'none', 'declining', 'growing', 'stable',
state, city

### serving.donation_detail — Every donation with person info (135,888 rows, 2014–2025)
donation_id, person_id, display_name, first_name, last_name, email,
amount, currency, donated_at, donation_month (yyyy-MM), donation_year,
payment_method — VALUES: 'PCC' (57K), 'card' (59K), 'CK' (1.4K), 'CC' (3.7K), 'EFT',
fund — VALUES: various fund codes,
appeal, designation, source_ref, source_system

### serving.donor_summary — Pre-ranked donor totals (one row per donor, 5,042 rows)
person_id, display_name, first_name, last_name, email,
primary_source,
donation_count, total_given, avg_gift, largest_gift,
first_gift_date, last_gift_date, days_since_last,
fund_count, active_months,
lifecycle_stage — VALUES: 'active', 'cooling', 'lapsed', 'lost'
USE THIS for: "top donors", "biggest givers", "donor leaderboard", "who gives the most"
Just: SELECT TOP (20) * FROM serving.donor_summary ORDER BY total_given DESC

### serving.donor_monthly — Donor giving by month (one row per donor-month, 62,414 rows)
person_id, display_name, donation_month (yyyy-MM), donation_year,
gifts (count), amount (sum), primary_fund, primary_method
USE THIS for: "giving by month", "monthly breakdown", "donor trends over time"
For top 20 donors monthly:
  SELECT m.* FROM serving.donor_monthly m
  WHERE m.person_id IN (SELECT TOP (20) person_id FROM serving.donor_summary ORDER BY total_given DESC)
  ORDER BY m.display_name, m.donation_month

### serving.order_detail — Every order with person info (205,182 rows, 2020–2025)
order_id, person_id, display_name, first_name, last_name, email,
order_number, total_amount, order_date,
order_month (yyyy-MM), order_year,
order_status, source_system

### serving.payment_detail — Every payment with person info (135,199 rows)
payment_id, person_id, display_name, first_name, last_name, email,
amount, payment_date, payment_month (yyyy-MM),
payment_method, invoice_id, source_system

### serving.invoice_detail — Every invoice with person info (205,182 rows)
invoice_id, person_id, display_name, first_name, last_name, email,
invoice_number, invoice_total, invoice_status,
issued_at, invoice_month (yyyy-MM), source_system

### serving.subscription_detail — Every subscription with person info (6,337 rows)
subscription_id, person_id, display_name, first_name, last_name, email,
product_name, amount, cadence,
subscription_status — VALUES: 'Inactive' (6.3K), 'Active' (46),
start_date, next_renewal, reason_stopped, source_system

### serving.tag_detail — Every contact-tag assignment (2,973,999 rows) ⭐ KEY FOR SEGMENTATION
tag_id, person_id, display_name, first_name, last_name,
tag_value — the tag name (e.g. "Donor - PF", "True Girl", "2025 FYE Campaign"),
tag_group — the tag category (e.g. "Donor Assignment", "True Girl", "Nurture Tags", "Box Tracking"),
applied_at, source_system
IMPORTANT: This table has ~3 MILLION rows. Use it for segmentation and audience analysis.
Key tag categories (tag_group):
  - "Donor Assignment" — donor tiers and campaigns (18K contacts)
  - "True Girl" — True Girl brand engagement (65K contacts)
  - "B2BB" — Born to Be Brave brand (12K contacts)
  - "Nurture Tags" — email nurture sequences (208K assignments)
  - "True Productions" — tour registrations (134K assignments)
  - "Customer Tags" — purchase behavior (76K assignments)
  - "Box Tracking" — subscription box lifecycle (12K assignments)
  - "Location" — geographic tags (7K assignments)
  - NULL — uncategorized campaign/audience tags (1.8M assignments)
Example queries:
  -- How many contacts are tagged as donors?
  SELECT COUNT(DISTINCT person_id) FROM serving.tag_detail WHERE tag_group = 'Donor Assignment'
  -- Top 20 tags by contact count:
  SELECT TOP (20) tag_value, COUNT(DISTINCT person_id) contacts FROM serving.tag_detail GROUP BY tag_value ORDER BY contacts DESC
  -- Contacts tagged with both "True Girl" and "Donor Assignment":
  SELECT COUNT(DISTINCT tg.person_id) FROM serving.tag_detail tg WHERE tg.tag_group = 'True Girl' AND tg.person_id IN (SELECT person_id FROM serving.tag_detail WHERE tag_group = 'Donor Assignment')

### serving.communication_detail — Emails, calls, messages with person info (24,040 rows)
communication_id, person_id, display_name, first_name, last_name,
channel — VALUES: 'EMAIL' (18K), 'Note' (1.2K), 'CONV' (1.2K), 'Phone Call' (1.1K), 'PHONE' (1K),
direction, subject, sent_at, source_system

### Silver Layer Tables (use when serving views aren't sufficient)
- silver.contact (96,219) — all contacts with source_system, source_id, demographics
- silver.contact_tag (3,001,971) — Keap tag assignments (tag_keap_id, contact_keap_id, date_applied)
- silver.tag (1,826) — tag definitions (keap_id, group_name, category_name)
- silver.note (370,934) — interaction notes
- silver.product (1,119) — product catalog
- silver.order_item (292,923) — line items with item_name, qty, price_per_unit, order_keap_id
- silver.identity_map (96,219) — maps contact_id → master_id for cross-system dedup

### Gold Layer Views (pre-aggregated analytics)
- gold.constituent_360 (84,507) — complete unified profile (all metrics in one row)
- gold.person_giving (84,507) — giving analytics per person
- gold.person_commerce (84,507) — commerce analytics per person
- gold.monthly_trends — donation trends by month and source system

## Key Stats
- 84,507 unified people (96K contacts across 3 source systems)
- 5,042 donors with $14.3M total giving
- 135K donations, 208K orders, 135K payments
- 3M tag assignments across 1,826 tags (richest segmentation data)
- 6,337 subscriptions (46 active, 6.3K inactive)
- 370K notes, 24K communications

## SQL Rules (T-SQL)
- ALWAYS use TOP (N) with parentheses — NEVER use LIMIT or OFFSET/FETCH
- display_name is NOT unique — always GROUP BY person_id + display_name or use MAX(display_name)
- Use DATEADD(YEAR, -2, GETDATE()) for relative dates
- Use FORMAT(date_col, 'yyyy-MM') for monthly grouping
- The views already have donation_month, order_month, payment_month, invoice_month columns
- For silver.[order] use brackets (SQL reserved word)

## CRITICAL: Query Rules
1. ALWAYS query serving.* views — they have display_name built in
2. NEVER JOIN silver tables with person tables — use the serving views instead
3. For donor questions, ALWAYS use serving.donor_summary or serving.donor_monthly — never scan donation_detail with CTEs
4. For tag/segmentation questions, use serving.tag_detail
5. Example — Top 20 donors: SELECT TOP (20) * FROM serving.donor_summary ORDER BY total_given DESC
6. Example — Top 20 donors monthly breakdown:
   SELECT m.* FROM serving.donor_monthly m
   WHERE m.person_id IN (SELECT TOP (20) person_id FROM serving.donor_summary ORDER BY total_given DESC)
   ORDER BY m.display_name, m.donation_month
`.trim();
