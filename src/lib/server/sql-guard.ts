/**
 * Text-to-SQL guardrails: allowlisted tables, blocked patterns, TOP injection, timeout.
 */

const ALLOWED_TABLES = new Set([
  // Serving layer (primary — pre-joined views)
  "serving.person_360",
  "serving.household_360",
  "serving.donation_detail",
  "serving.order_detail",
  "serving.subscription_detail",
  "serving.payment_detail",
  "serving.invoice_detail",
  "serving.tag_detail",
  "serving.communication_detail",
  // Giving
  "giving.donation",
  "giving.recurring_plan",
  "giving.pledge",
  // Commerce
  "commerce.[order]",
  "commerce.order_line",
  "commerce.subscription",
  "commerce.invoice",
  "commerce.payment",
  // Engagement
  "engagement.activity",
  "engagement.communication",
  "engagement.note",
  "engagement.tag",
  // Event
  "event.event",
  "event.ticket",
  // Person
  "person.profile",
  "person.email",
  "person.phone",
  "person.address",
  "person.source_link",
  // Household
  "household.unit",
  "household.member",
  // Meta
  "meta.source_system",
  "meta.file_lineage",
  // Intel
  "intel.segment",
  "intel.segment_member",
  "intel.donor_score",
]);

const BLOCKED_PATTERNS = [
  /\b(DROP|ALTER|CREATE|TRUNCATE)\b/i,
  /\b(INSERT|UPDATE|DELETE|MERGE)\b/i,
  /\b(EXEC|EXECUTE|GRANT|REVOKE|DENY)\b/i,
  /\b(xp_|sp_)\w+/i,
  /\bINTO\s+\w+/i,
  /--/,
  /\/\*/,
  /;\s*(DROP|ALTER|CREATE|INSERT|UPDATE|DELETE|EXEC)/i,
];

const MAX_ROWS = 500;
const QUERY_TIMEOUT_MS = 15_000;

export interface GuardResult {
  ok: boolean;
  sanitized?: string;
  reason?: string;
}

export function guardSql(rawSql: string): GuardResult {
  const trimmed = rawSql.trim().replace(/;+\s*$/, "");

  // Block dangerous patterns
  for (const pattern of BLOCKED_PATTERNS) {
    if (pattern.test(trimmed)) {
      return {
        ok: false,
        reason: `Blocked: query matches forbidden pattern ${pattern.source}`,
      };
    }
  }

  // Must start with SELECT or WITH
  if (!/^\s*(SELECT|WITH)\b/i.test(trimmed)) {
    return {
      ok: false,
      reason: "Only SELECT and WITH (CTE) queries are allowed.",
    };
  }

  // Inject TOP if missing on the outermost SELECT
  // Match TOP with or without parens: TOP 20, TOP(20), TOP (20)
  let sanitized = trimmed;
  const hasTop = /\bTOP\s*\(?\s*\d+/i.test(sanitized);
  const hasOffsetFetch = /\bOFFSET\b[\s\S]*\bFETCH\b/i.test(sanitized);
  if (!hasTop && !hasOffsetFetch) {
    // For CTEs, inject TOP after the final SELECT
    if (/^\s*WITH\b/i.test(sanitized)) {
      // Find the last SELECT that isn't inside a subquery
      const lastSelectIdx = sanitized.lastIndexOf("SELECT");
      if (lastSelectIdx >= 0) {
        sanitized =
          sanitized.slice(0, lastSelectIdx + 6) +
          ` TOP (${MAX_ROWS})` +
          sanitized.slice(lastSelectIdx + 6);
      }
    } else {
      sanitized = sanitized.replace(
        /^(\s*SELECT)\b/i,
        `$1 TOP (${MAX_ROWS})`,
      );
    }
  }

  return { ok: true, sanitized };
}

export { QUERY_TIMEOUT_MS, ALLOWED_TABLES };
