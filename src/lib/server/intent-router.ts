/**
 * Intent Router: enriches the chat context with intelligence layer awareness.
 *
 * Rather than a separate classifier, we:
 * 1. Match user utterance against metric synonyms to detect metric queries
 * 2. Inject matched metric context into the system prompt
 * 3. Provide a `compute_metric` tool so the LLM can use certified metrics
 *
 * This lets the LLM naturally choose between:
 * - compute_metric (certified, pre-validated SQL from the catalog)
 * - query_data (custom SQL for complex/exploratory questions)
 * - search_data (semantic search for discovery)
 */

import { matchMetricsBySynonym, getCatalogSummaryForPrompt, getMetrics, getPolicies } from "./intel-catalog";
import type { MetricDefinition, SemanticPolicy } from "./sqp-types";

export interface IntentContext {
  /** Matched metrics for the current query (empty if none matched) */
  matchedMetrics: Array<{
    metric_key: string;
    display_name: string;
    description: string | null;
    unit: string;
    default_time_window: string | null;
    synonym_matched: string;
  }>;

  /** Compact catalog summary for the system prompt */
  catalogSummary: string;

  /** Active semantic policies to include in the prompt */
  policyNotes: string[];

  /** Whether this looks like a metric query */
  isLikelyMetricQuery: boolean;
}

/**
 * Analyze a user message and build intelligence context.
 */
export async function analyzeIntent(userMessage: string): Promise<IntentContext> {
  // 1. Match metrics by synonym
  const synonymMatches = await matchMetricsBySynonym(userMessage, 5);

  // 2. Look up full metric definitions for matches
  const metricsMap = await getMetrics();
  const matchedMetrics = synonymMatches
    .map((sm) => {
      const def = metricsMap.get(sm.metric_key);
      if (!def) return null;
      return {
        metric_key: sm.metric_key,
        display_name: def.display_name,
        description: def.description,
        unit: def.unit,
        default_time_window: def.default_time_window,
        synonym_matched: sm.synonym,
      };
    })
    .filter((m): m is NonNullable<typeof m> => m !== null);

  // 3. Get catalog summary (cached)
  const catalogSummary = await getCatalogSummaryForPrompt();

  // 4. Get active policies
  const policies = await getPolicies();
  const policyNotes = policies.map((p) => `- ${p.description}`);

  // 5. Heuristic: is this likely a metric query?
  const isLikelyMetricQuery = matchedMetrics.length > 0 && matchedMetrics[0].synonym_matched.length > 3;

  return {
    matchedMetrics,
    catalogSummary,
    policyNotes,
    isLikelyMetricQuery,
  };
}

/**
 * Build the intelligence context block for the system prompt.
 * This is injected into the prompt before the user's message.
 */
export function buildIntelContextBlock(ctx: IntentContext): string {
  const sections: string[] = [];

  // Metric catalog (always present — teaches the LLM what metrics exist)
  sections.push(ctx.catalogSummary);

  // Semantic policies
  if (ctx.policyNotes.length > 0) {
    sections.push(`\n## Semantic Policies (MUST follow)\n${ctx.policyNotes.join("\n")}`);
  }

  // Matched metrics for current query
  if (ctx.matchedMetrics.length > 0) {
    const lines = ctx.matchedMetrics.map(
      (m) => `- **${m.display_name}** [${m.metric_key}]: ${m.description ?? "No description"} (${m.unit}, default window: ${m.default_time_window ?? "all_time"})`,
    );
    sections.push(
      `\n## Metrics Matching This Query\nThe user's question matches these certified metrics. Prefer using compute_metric for accurate, validated results:\n${lines.join("\n")}`,
    );
  }

  return sections.join("\n");
}
