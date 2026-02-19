/**
 * Comprehensive 360 Profile Builder
 *
 * Server-side function that queries ALL serving views to build enriched
 * person profiles. Called by the build_360 LLM tool.
 *
 * For each person it gathers:
 * - Contact info (person_360 base)
 * - Top tags (from tag_detail)
 * - Events attended (from event_detail)
 * - Active subscriptions (from subscription_detail)
 * - Recent gifts with fund/date (from donation_detail)
 * - Wealth screening data (from wealth_screening)
 */

import { executeSql } from "@/lib/server/sql-client";

const ENRICH_TIMEOUT = 60_000; // 60s for enrichment queries

/** Sanitize a SQL fragment to prevent injection via filter/order params */
function sanitizeFragment(fragment: string): string {
  return fragment
    .replace(/;/g, "")
    .replace(/--/g, "")
    .replace(/\/\*/g, "")
    .replace(/\*\//g, "");
}

/** Check that a fragment has no dangerous SQL keywords */
function isDangerousFragment(fragment: string): boolean {
  return /\b(DROP|ALTER|CREATE|TRUNCATE|INSERT|UPDATE|DELETE|MERGE|EXEC|EXECUTE|GRANT|REVOKE|DENY)\b/i.test(
    fragment,
  );
}

export async function buildComprehensive360(
  filterWhere: string,
  orderBy: string,
  limit: number,
): Promise<
  | { ok: true; data: Record<string, unknown>[]; count: number }
  | { ok: false; error: string }
> {
  // Sanitize inputs
  const safeFilter = sanitizeFragment(filterWhere || "");
  const safeOrder = sanitizeFragment(orderBy || "lifetime_giving DESC");
  const safeLimit = Math.max(1, Math.min(limit || 20, 50));

  if (isDangerousFragment(safeFilter) || isDangerousFragment(safeOrder)) {
    return { ok: false, error: "Filter or order clause contains blocked keywords." };
  }

  // 1. Get matching persons from person_360
  const whereClause = safeFilter
    ? `WHERE display_name <> 'Unknown' AND (${safeFilter})`
    : `WHERE display_name <> 'Unknown'`;

  const personSql = `SELECT TOP (${safeLimit})
    person_id, display_name, first_name, last_name,
    email, phone, city, state, postal_code,
    primary_source, source_count, source_systems,
    donation_count, lifetime_giving, avg_gift, largest_gift,
    first_gift_date, last_gift_date, recency_days,
    order_count, total_spent,
    woo_order_count, woo_total_spent,
    shopify_order_count, shopify_total_spent,
    ticket_count, subbly_sub_count, subbly_active,
    stripe_charge_count, stripe_total,
    tag_count, note_count, comm_count,
    lifecycle_stage
  FROM serving.person_360
  ${whereClause}
  ORDER BY ${safeOrder}`;

  const personResult = await executeSql(personSql, ENRICH_TIMEOUT);
  if (!personResult.ok) {
    return { ok: false, error: personResult.reason || "Failed to query persons" };
  }

  const persons = personResult.rows;
  if (!persons.length) {
    return { ok: true, data: [], count: 0 };
  }

  const ids = persons.map((p) => p.person_id as number);
  const idList = ids.join(",");

  // 2. Gather enrichment data in parallel
  const [tags, events, subs, recentGifts, wealth] = await Promise.all([
    // Tags — limit to ~15 per person to avoid pulling millions
    executeSql(
      `SELECT TOP (${safeLimit * 15}) person_id, tag_value, tag_group
       FROM serving.tag_detail
       WHERE person_id IN (${idList}) AND tag_value IS NOT NULL`,
      ENRICH_TIMEOUT,
    ),
    // Events
    executeSql(
      `SELECT person_id, event_name, payment_date, checked_in
       FROM serving.event_detail
       WHERE person_id IN (${idList})`,
      ENRICH_TIMEOUT,
    ),
    // Subscriptions
    executeSql(
      `SELECT person_id, product_name, subscription_status, source_system
       FROM serving.subscription_detail
       WHERE person_id IN (${idList})`,
      ENRICH_TIMEOUT,
    ),
    // Recent donations — last 5 per person
    executeSql(
      `SELECT TOP (${safeLimit * 5}) person_id, amount, donated_at, fund
       FROM serving.donation_detail
       WHERE person_id IN (${idList})
       ORDER BY donated_at DESC`,
      ENRICH_TIMEOUT,
    ),
    // Wealth screening
    executeSql(
      `SELECT person_id, giving_capacity, capacity_label, quality_score
       FROM serving.wealth_screening
       WHERE person_id IN (${idList})`,
      ENRICH_TIMEOUT,
    ),
  ]);

  // 3. Build per-person lookup maps
  const tagMap = groupBy(tags.ok ? tags.rows : [], "person_id");
  const eventMap = groupBy(events.ok ? events.rows : [], "person_id");
  const subMap = groupBy(subs.ok ? subs.rows : [], "person_id");
  const giftMap = groupBy(recentGifts.ok ? recentGifts.rows : [], "person_id");
  const wealthMap = groupBy(wealth.ok ? wealth.rows : [], "person_id");

  // 4. Enrich each person row
  const enriched = persons.map((p) => {
    const pid = p.person_id as number;

    // Tags — unique values, up to 10
    const pTags = tagMap[pid] || [];
    const uniqueTags = [...new Set(pTags.map((t) => String(t.tag_value)))].slice(0, 10);

    // Events — unique event names
    const pEvents = eventMap[pid] || [];
    const uniqueEvents = [...new Set(pEvents.map((e) => String(e.event_name)))].slice(0, 5);

    // Subscriptions
    const pSubs = subMap[pid] || [];
    const subSummary = [
      ...new Set(
        pSubs.map((s) => {
          const status = String(s.subscription_status || "").toLowerCase();
          return `${s.product_name} (${status})`;
        }),
      ),
    ];

    // Recent gifts — last 5
    const pGifts = (giftMap[pid] || []).slice(0, 5);
    const giftSummary = pGifts.map((g) => {
      const d = g.donated_at ? new Date(g.donated_at as string) : null;
      const dateStr = d
        ? `${d.toLocaleString("en-US", { month: "short" })} ${d.getFullYear()}`
        : "?";
      const amt = Number(g.amount || 0);
      const fundStr = g.fund ? `, ${g.fund}` : "";
      return `$${amt.toLocaleString("en-US", { maximumFractionDigits: 0 })} (${dateStr}${fundStr})`;
    });

    // Wealth
    const pWealth = (wealthMap[pid] || [])[0];

    // Remove internal ID from output
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { person_id: _pid, ...rest } = p;

    return {
      ...rest,
      // Enrichment columns
      top_tags: uniqueTags.join(", ") || null,
      events_attended: uniqueEvents.join(", ") || null,
      subscriptions: subSummary.join(", ") || null,
      recent_gifts: giftSummary.join(" | ") || null,
      wealth_capacity: pWealth ? pWealth.capacity_label : null,
      wealth_quality_score: pWealth ? pWealth.quality_score : null,
    };
  });

  return { ok: true, data: enriched, count: enriched.length };
}

function groupBy(
  rows: Record<string, unknown>[],
  key: string,
): Record<number, Record<string, unknown>[]> {
  const map: Record<number, Record<string, unknown>[]> = {};
  for (const row of rows) {
    const k = row[key] as number;
    if (k == null) continue;
    if (!map[k]) map[k] = [];
    map[k].push(row);
  }
  return map;
}
