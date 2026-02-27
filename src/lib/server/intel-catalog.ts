/**
 * Intel catalog: load metric definitions, dimensions, allowlists,
 * synonyms, and policies from the intel schema in Azure SQL.
 *
 * Uses in-memory caching (5 min TTL) to avoid hitting the DB on every request.
 */

import { executeSql } from "./sql-client";
import type {
  MetricDefinition,
  DimensionDefinition,
  MetricDimensionAllowlist,
  SemanticPolicy,
  MetricSynonym,
} from "./sqp-types";

// ── Cache ───────────────────────────────────────────────────────────────────
const CACHE_TTL_MS = 5 * 60 * 1000; // 5 minutes

interface CacheEntry<T> {
  data: T;
  loadedAt: number;
}

let metricsCache: CacheEntry<Map<string, MetricDefinition>> | null = null;
let dimensionsCache: CacheEntry<Map<string, DimensionDefinition>> | null = null;
let allowlistCache: CacheEntry<MetricDimensionAllowlist[]> | null = null;
let synonymsCache: CacheEntry<MetricSynonym[]> | null = null;
let policiesCache: CacheEntry<SemanticPolicy[]> | null = null;

function isStale<T>(cache: CacheEntry<T> | null): boolean {
  if (!cache) return true;
  return Date.now() - cache.loadedAt > CACHE_TTL_MS;
}

// ── Loaders ─────────────────────────────────────────────────────────────────

export async function getMetrics(): Promise<Map<string, MetricDefinition>> {
  if (!isStale(metricsCache)) return metricsCache!.data;

  const result = await executeSql(
    `SELECT metric_key, display_name, description, metric_type, unit, format_hint,
            grain, default_time_window, sql_expression, depends_on_metric_keys,
            is_certified, is_active
     FROM intel.metric_definition
     WHERE is_active = 1`,
    30000,
  );

  if (!result.ok) {
    console.error("[intel-catalog] Failed to load metrics:", result.reason);
    return metricsCache?.data ?? new Map();
  }

  const map = new Map<string, MetricDefinition>();
  for (const row of result.rows) {
    map.set(row.metric_key as string, row as unknown as MetricDefinition);
  }

  metricsCache = { data: map, loadedAt: Date.now() };
  return map;
}

export async function getDimensions(): Promise<Map<string, DimensionDefinition>> {
  if (!isStale(dimensionsCache)) return dimensionsCache!.data;

  const result = await executeSql(
    `SELECT dimension_key, display_name, description, source_table, source_column,
            data_type, is_time_dimension, allowed_values_json, allowed_operators_json, is_active
     FROM intel.dimension_definition
     WHERE is_active = 1`,
    30000,
  );

  if (!result.ok) {
    console.error("[intel-catalog] Failed to load dimensions:", result.reason);
    return dimensionsCache?.data ?? new Map();
  }

  const map = new Map<string, DimensionDefinition>();
  for (const row of result.rows) {
    map.set(row.dimension_key as string, row as unknown as DimensionDefinition);
  }

  dimensionsCache = { data: map, loadedAt: Date.now() };
  return map;
}

export async function getAllowlist(): Promise<MetricDimensionAllowlist[]> {
  if (!isStale(allowlistCache)) return allowlistCache!.data;

  const result = await executeSql(
    `SELECT metric_key, dimension_key, allow_group_by, allow_filter
     FROM intel.metric_dimension_allowlist`,
    30000,
  );

  if (!result.ok) {
    console.error("[intel-catalog] Failed to load allowlist:", result.reason);
    return allowlistCache?.data ?? [];
  }

  const data = result.rows as unknown as MetricDimensionAllowlist[];
  allowlistCache = { data, loadedAt: Date.now() };
  return data;
}

export async function getSynonyms(): Promise<MetricSynonym[]> {
  if (!isStale(synonymsCache)) return synonymsCache!.data;

  const result = await executeSql(
    `SELECT metric_key, synonym, weight, is_active
     FROM intel.metric_synonym
     WHERE is_active = 1
     ORDER BY weight DESC`,
    30000,
  );

  if (!result.ok) {
    console.error("[intel-catalog] Failed to load synonyms:", result.reason);
    return synonymsCache?.data ?? [];
  }

  const data = result.rows as unknown as MetricSynonym[];
  synonymsCache = { data, loadedAt: Date.now() };
  return data;
}

export async function getPolicies(): Promise<SemanticPolicy[]> {
  if (!isStale(policiesCache)) return policiesCache!.data;

  const result = await executeSql(
    `SELECT policy_key, description, policy_json, is_active
     FROM intel.semantic_policy
     WHERE is_active = 1`,
    30000,
  );

  if (!result.ok) {
    console.error("[intel-catalog] Failed to load policies:", result.reason);
    return policiesCache?.data ?? [];
  }

  const data = result.rows as unknown as SemanticPolicy[];
  policiesCache = { data, loadedAt: Date.now() };
  return data;
}

// ── Synonym matching ────────────────────────────────────────────────────────

/**
 * Find the best matching metric key(s) for a user utterance.
 * Returns matches sorted by relevance (weight × match quality).
 */
export async function matchMetricsBySynonym(
  utterance: string,
  maxResults = 3,
): Promise<Array<{ metric_key: string; synonym: string; score: number }>> {
  const synonyms = await getSynonyms();
  const lower = utterance.toLowerCase();

  const scored: Array<{ metric_key: string; synonym: string; score: number }> = [];

  for (const syn of synonyms) {
    const synLower = syn.synonym.toLowerCase();
    if (lower.includes(synLower)) {
      // Score: base weight × length ratio (longer synonym matches are better)
      const lengthRatio = synLower.length / lower.length;
      const score = syn.weight * (0.5 + 0.5 * lengthRatio);
      scored.push({ metric_key: syn.metric_key, synonym: syn.synonym, score });
    }
  }

  // Deduplicate by metric_key, keeping highest score
  const best = new Map<string, (typeof scored)[0]>();
  for (const item of scored) {
    const existing = best.get(item.metric_key);
    if (!existing || item.score > existing.score) {
      best.set(item.metric_key, item);
    }
  }

  return Array.from(best.values())
    .sort((a, b) => b.score - a.score)
    .slice(0, maxResults);
}

/**
 * Build a compact catalog summary for the LLM system prompt.
 * Returns a concise string listing all available metrics and their synonyms.
 */
export async function getCatalogSummaryForPrompt(): Promise<string> {
  const metrics = await getMetrics();
  const synonyms = await getSynonyms();

  // Group synonyms by metric
  const synMap = new Map<string, string[]>();
  for (const s of synonyms) {
    if (!synMap.has(s.metric_key)) synMap.set(s.metric_key, []);
    synMap.get(s.metric_key)!.push(s.synonym);
  }

  const lines: string[] = ["## Available Metrics"];

  // Group by category
  const categories = new Map<string, MetricDefinition[]>();
  metrics.forEach((m) => {
    const cat = m.metric_key.split(".")[0];
    if (!categories.has(cat)) categories.set(cat, []);
    categories.get(cat)!.push(m);
  });

  categories.forEach((defs, cat) => {
    lines.push(`\n### ${cat.charAt(0).toUpperCase() + cat.slice(1)}`);
    for (const d of defs) {
      const syns = synMap.get(d.metric_key);
      const synStr = syns ? ` (aka: ${syns.slice(0, 3).join(", ")})` : "";
      lines.push(`- **${d.display_name}** [${d.metric_key}]${synStr} — ${d.unit}, ${d.grain} grain`);
    }
  });

  return lines.join("\n");
}

/** Force-reload all caches */
export async function reloadCatalog(): Promise<void> {
  metricsCache = null;
  dimensionsCache = null;
  allowlistCache = null;
  synonymsCache = null;
  policiesCache = null;
  await Promise.all([getMetrics(), getDimensions(), getAllowlist(), getSynonyms(), getPolicies()]);
}
