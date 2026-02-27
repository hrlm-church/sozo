/**
 * Structured Query Plan (SQP) types.
 *
 * The LLM never writes SQL directly for metric queries.
 * Instead it outputs a JSON SQP, which the compiler validates against
 * the intel catalog and compiles to safe, parameterized T-SQL.
 */

// ── Time window presets ─────────────────────────────────────────────────────
export type TimePreset =
  | "today"
  | "yesterday"
  | "last_7_days"
  | "last_30_days"
  | "last_90_days"
  | "last_12_months"
  | "ytd"
  | "last_year"
  | "all_time"
  | "as_of"
  | "custom";

export interface TimeWindow {
  preset: TimePreset;
  /** For custom: ISO date strings */
  startDate?: string;
  endDate?: string;
  /** For as_of metrics: the reference date (defaults to today) */
  asOfDate?: string;
}

// ── Filters ─────────────────────────────────────────────────────────────────
export type FilterOp = "=" | "!=" | "in" | "not_in" | ">" | ">=" | "<" | "<=" | "between" | "like";

export interface Filter {
  dimension: string;   // e.g. "giving.fund"
  op: FilterOp;
  value: string | number | boolean | null;
  /** For "in" / "not_in" / "between" operators */
  values?: (string | number)[];
}

// ── Sort ────────────────────────────────────────────────────────────────────
export interface Sort {
  by: string;          // dimension key or "value"
  direction: "asc" | "desc";
}

// ── The Structured Query Plan ───────────────────────────────────────────────
export interface StructuredQueryPlan {
  /** What the user is asking for */
  intent: "metric_query" | "metric_comparison" | "trend" | "breakdown" | "list";

  /** Metric keys from the catalog */
  metrics: string[];

  /** Optional: group-by dimensions */
  dimensions?: string[];

  /** Time window for the query */
  timeWindow?: TimeWindow;

  /** Filters to apply */
  filters?: Filter[];

  /** Sort order */
  sort?: Sort;

  /** Row limit (default 100, max 500) */
  limit?: number;

  /** For comparison: the comparison time window */
  compareWindow?: TimeWindow;
}

// ── Compiled output ─────────────────────────────────────────────────────────
export interface CompiledQuery {
  /** The safe SQL query */
  sql: string;

  /** Parameter values to bind */
  params: Record<string, string | number | null>;

  /** Metrics used (for audit) */
  metricsUsed: string[];

  /** Dimensions used (for audit) */
  dimensionsUsed: string[];

  /** Human-readable explanation */
  explanation: string;
}

// ── Catalog types (loaded from DB) ──────────────────────────────────────────
export interface MetricDefinition {
  metric_key: string;
  display_name: string;
  description: string | null;
  metric_type: string;
  unit: string;
  format_hint: string | null;
  grain: string;
  default_time_window: string | null;
  sql_expression: string;
  depends_on_metric_keys: string | null;
  is_certified: boolean;
  is_active: boolean;
}

export interface DimensionDefinition {
  dimension_key: string;
  display_name: string;
  description: string | null;
  source_table: string;
  source_column: string | null;
  data_type: string;
  is_time_dimension: boolean;
  allowed_values_json: string | null;
  allowed_operators_json: string;
  is_active: boolean;
}

export interface MetricDimensionAllowlist {
  metric_key: string;
  dimension_key: string;
  allow_group_by: boolean;
  allow_filter: boolean;
}

export interface SemanticPolicy {
  policy_key: string;
  description: string | null;
  policy_json: string;
  is_active: boolean;
}

export interface MetricSynonym {
  metric_key: string;
  synonym: string;
  weight: number;
  is_active: boolean;
}
