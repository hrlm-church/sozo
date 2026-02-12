/**
 * Schema context for the LLM system prompt.
 * SERVING VIEWS are pre-joined — the LLM should NEVER need to write JOINs.
 */

export const SCHEMA_CONTEXT = `
## IMPORTANT: Always Use Serving Views (Pre-Joined — No JOINs Needed)

Every serving view already includes display_name, person_id, email, household info.
NEVER JOIN tables manually — use these views instead.

### serving.person_360 — ALL people aggregates (45,168 rows)
person_id, display_name, first_name, last_name, email, phone,
household_id, household_name, source_systems (CSV),
lifetime_giving, donation_count, avg_gift,
first_gift_date, last_gift_date, largest_gift,
recency_days, frequency_annual, monetary_annual,
active_subscriptions, subscription_months,
last_event_date, events_attended, tickets_total,
engagement_count, last_engagement, tag_count,
lifecycle_stage — VALUES: 'prospect' (40K), 'lost' (4.3K), 'lapsed' (552), 'cooling' (256),
churn_risk (0-1), ltv_estimate,
top_segments_json, next_action_json, updated_at

### serving.household_360 — Household aggregates (39,074 rows)
household_id, name, member_count, members_json,
household_giving_total, household_annual_giving,
giving_trend — VALUES: 'none' (34K), 'declining' (4.6K), 'growing' (299), 'stable' (110),
active_subs, events_attended, health_score (0-1),
best_contact_method, updated_at

### serving.donation_detail — Every donation with person info (67,226 rows, 2014–2025)
donation_id, person_id, display_name, first_name, last_name, email,
household_id, household_name, lifecycle_stage,
amount, currency, donated_at, donation_month (yyyy-MM), donation_year,
payment_method — VALUES: 'PCC' (57K), 'CC' (3.7K), 'PKIND' (2K), 'PKEAP' (1.8K), 'CK' (1.4K), 'EFT',
fund — VALUES: 'PF' ($4.1M), 'TG' ($2.1M), 'BB' ($849K), 'PFP', 'TGP', 'True Girl', etc.,
appeal — VALUES: 'GENERAL' (52K), 'KEAP' (8.4K), 'KINDFUL' (4.2K), 'RECURRING' (1.6K),
designation, source_ref, source_system

### serving.order_detail — Every order with person info (208,159 rows, 2020–2025)
order_id, person_id, display_name, first_name, last_name, email,
household_id, order_number, total_amount, order_date,
order_month (yyyy-MM), order_year,
order_status — VALUES: '0' (121K), '1' (87K),
source_ref, source_system

### serving.subscription_detail — Every subscription with person info (7,368 rows)
subscription_id, person_id, display_name, first_name, last_name, email,
product_name, amount, cadence,
subscription_status — VALUES: 'Inactive' (6.5K), 'Y' (632), 'D' (130), 'Active' (46),
start_date, next_renewal, is_gift, source_ref, source_system

### serving.payment_detail — Every payment with person info (137,114 rows)
payment_id, person_id, display_name, first_name, last_name, email,
amount, payment_date, payment_month (yyyy-MM),
payment_method, payment_status,
donation_id, order_id, invoice_id, source_ref, source_system

### serving.invoice_detail — Every invoice with person info (208,051 rows)
invoice_id, person_id, display_name, first_name, last_name, email,
invoice_number, invoice_total, invoice_status,
issued_at, paid_at, invoice_month (yyyy-MM), source_ref, source_system

### serving.tag_detail — Every tag with person info (83,242 rows)
tag_id, person_id, display_name, first_name, last_name,
tag_value (the tag text), tag_group (currently all NULL),
applied_at, source_ref, source_system

### serving.communication_detail — Emails, calls, messages with person info (25,192 rows)
communication_id, person_id, display_name, first_name, last_name,
channel — VALUES: 'EMAIL' (18K), 'Note' (1.2K), 'CONV' (1.2K), 'Phone Call' (1.1K), 'PHONE' (1K),
direction, subject, sent_at, source_ref, source_system

### engagement.note — Notes (375,623 rows, no view yet — use with person_360 JOIN if needed)
id, person_id, note_text, author, created_at, source_id, source_ref

### engagement.activity — Activities (11,908 rows)
id, person_id, activity_type (mostly 'Note'), subject, body, occurred_at, source_id, source_ref

### meta.source_system — 7 systems
source_id int PK, name varchar, display_name nvarchar
Values: 1=Bloomerang, 2=Donor Direct, 3=Givebutter, 4=Keap, 5=Kindful, 6=Stripe, 7=Transaction Imports

### Tables with NO data yet (tell user):
- event.event, event.ticket — no event data loaded
- giving.recurring_plan — empty
- commerce.order_line — empty

## Key Stats
- 45,168 people total, only 5,110 are donors (11%)
- Total giving: $7.4M, avg per donor: $1,450, max: $820K
- 208K orders, 137K payments, 208K invoices
- 7,368 subscriptions (mostly Inactive)
- 375K notes, 83K tags, 25K communications

## SQL Rules (T-SQL)
- ALWAYS use TOP (N) with parentheses — NEVER use LIMIT or OFFSET/FETCH
- All PKs are uniqueidentifier (GUIDs)
- display_name is NOT unique — always GROUP BY person_id + display_name
- Use DATEADD(YEAR, -2, GETDATE()) for relative dates
- Use FORMAT(date_col, 'yyyy-MM') for monthly grouping
- The views already have donation_month, order_month, payment_month, invoice_month columns

## CRITICAL: Query Rules
1. ALWAYS query serving.* views — they have display_name built in
2. NEVER JOIN giving.donation or commerce.[order] with person tables — use the serving views instead
3. For top-N with detail: use CTE (WITH top AS (...)) then join back to the same serving view
4. Example — Top 20 donors monthly (NO JOINs needed):
   WITH top20 AS (
     SELECT TOP (20) person_id, SUM(amount) AS total
     FROM serving.donation_detail
     WHERE donated_at >= DATEADD(YEAR, -2, GETDATE())
     GROUP BY person_id ORDER BY total DESC
   )
   SELECT d.display_name, d.donation_month AS month, SUM(d.amount) AS amount
   FROM serving.donation_detail d
   JOIN top20 t ON t.person_id = d.person_id
   WHERE d.donated_at >= DATEADD(YEAR, -2, GETDATE())
   GROUP BY d.display_name, d.donation_month
   ORDER BY d.display_name, month
`.trim();
