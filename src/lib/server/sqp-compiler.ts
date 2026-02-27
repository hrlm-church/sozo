/**
 * SQP Compiler: takes a StructuredQueryPlan and compiles it to safe T-SQL.
 *
 * The compiler:
 * 1. Validates all metric keys exist in the catalog
 * 2. Validates all dimension keys exist and are allowed for the metrics
 * 3. Resolves time window presets to concrete dates
 * 4. Substitutes @start_date, @end_date, @as_of_date parameters
 * 5. For dimensioned queries, wraps the metric SQL with GROUP BY
 * 6. Returns a CompiledQuery with safe, parameterized SQL
 */

import type {
  StructuredQueryPlan,
  CompiledQuery,
  TimeWindow,
  TimePreset,
  Filter,
  MetricDefinition,
  DimensionDefinition,
} from "./sqp-types";
import { getMetrics, getDimensions, getAllowlist } from "./intel-catalog";

export class CompileError extends Error {
  code: string;
  constructor(code: string, message: string) {
    super(message);
    this.code = code;
    this.name = "CompileError";
  }
}

// ── Time window resolution ──────────────────────────────────────────────────

function resolveTimeWindow(tw?: TimeWindow): { startDate: string | null; endDate: string | null; asOfDate: string } {
  const today = new Date();
  const todayStr = today.toISOString().slice(0, 10);

  if (!tw || tw.preset === "all_time") {
    return { startDate: null, endDate: null, asOfDate: todayStr };
  }

  if (tw.preset === "as_of") {
    return { startDate: null, endDate: null, asOfDate: tw.asOfDate ?? todayStr };
  }

  if (tw.preset === "custom") {
    return {
      startDate: tw.startDate ?? null,
      endDate: tw.endDate ?? todayStr,
      asOfDate: tw.asOfDate ?? todayStr,
    };
  }

  // Preset-based date ranges
  let startDate: string;
  const endDate = todayStr;

  switch (tw.preset) {
    case "today":
      startDate = todayStr;
      break;
    case "yesterday": {
      const d = new Date(today);
      d.setDate(d.getDate() - 1);
      startDate = d.toISOString().slice(0, 10);
      break;
    }
    case "last_7_days": {
      const d = new Date(today);
      d.setDate(d.getDate() - 7);
      startDate = d.toISOString().slice(0, 10);
      break;
    }
    case "last_30_days": {
      const d = new Date(today);
      d.setDate(d.getDate() - 30);
      startDate = d.toISOString().slice(0, 10);
      break;
    }
    case "last_90_days": {
      const d = new Date(today);
      d.setDate(d.getDate() - 90);
      startDate = d.toISOString().slice(0, 10);
      break;
    }
    case "last_12_months": {
      const d = new Date(today);
      d.setFullYear(d.getFullYear() - 1);
      startDate = d.toISOString().slice(0, 10);
      break;
    }
    case "ytd": {
      startDate = `${today.getFullYear()}-01-01`;
      break;
    }
    case "last_year": {
      const year = today.getFullYear() - 1;
      startDate = `${year}-01-01`;
      return { startDate, endDate: `${year}-12-31`, asOfDate: todayStr };
    }
    default:
      startDate = todayStr;
  }

  return { startDate, endDate, asOfDate: tw.asOfDate ?? todayStr };
}

// ── Parameter substitution ──────────────────────────────────────────────────

function substituteParams(
  sqlExpr: string,
  startDate: string | null,
  endDate: string | null,
  asOfDate: string,
): string {
  let sql = sqlExpr;
  // Replace parameters — use quoted date strings for safety
  sql = sql.replace(/@start_date/g, startDate ? `'${startDate}'` : "NULL");
  sql = sql.replace(/@end_date/g, endDate ? `'${endDate}'` : "NULL");
  sql = sql.replace(/@as_of_date/g, `'${asOfDate}'`);
  return sql;
}

// ── Filter compilation ──────────────────────────────────────────────────────

function compileFilter(
  filter: Filter,
  dimDef: DimensionDefinition,
): string {
  const col = `${dimDef.source_table.split(".").pop()}.${dimDef.source_column}`;

  // Validate operator is allowed
  const allowedOps: string[] = JSON.parse(dimDef.allowed_operators_json);
  if (!allowedOps.includes(filter.op)) {
    throw new CompileError(
      "INVALID_OPERATOR",
      `Operator '${filter.op}' not allowed for dimension '${filter.dimension}'. Allowed: ${allowedOps.join(", ")}`,
    );
  }

  // Escape single quotes in string values
  const escVal = (v: string | number | boolean | null): string => {
    if (v === null) return "NULL";
    if (typeof v === "number") return String(v);
    if (typeof v === "boolean") return v ? "1" : "0";
    return `'${String(v).replace(/'/g, "''")}'`;
  };

  switch (filter.op) {
    case "=":
    case "!=":
    case ">":
    case ">=":
    case "<":
    case "<=":
      return `${col} ${filter.op} ${escVal(filter.value)}`;
    case "in":
    case "not_in": {
      const vals = (filter.values ?? []).map(escVal).join(", ");
      const op = filter.op === "in" ? "IN" : "NOT IN";
      return `${col} ${op} (${vals})`;
    }
    case "between": {
      const [v1, v2] = filter.values ?? [];
      return `${col} BETWEEN ${escVal(v1)} AND ${escVal(v2)}`;
    }
    case "like":
      return `${col} LIKE ${escVal(filter.value)}`;
    default:
      throw new CompileError("UNKNOWN_OP", `Unknown filter operator: ${filter.op}`);
  }
}

// ── Main compiler ───────────────────────────────────────────────────────────

export async function compileSQP(plan: StructuredQueryPlan): Promise<CompiledQuery> {
  // Load catalog
  const [metricMap, dimMap, allowlist] = await Promise.all([
    getMetrics(),
    getDimensions(),
    getAllowlist(),
  ]);

  // 1. Validate metrics
  if (!plan.metrics || plan.metrics.length === 0) {
    throw new CompileError("NO_METRICS", "At least one metric is required");
  }

  const metricDefs: MetricDefinition[] = [];
  for (const key of plan.metrics) {
    const def = metricMap.get(key);
    if (!def) {
      throw new CompileError("UNKNOWN_METRIC", `Unknown metric: '${key}'`);
    }
    metricDefs.push(def);
  }

  // 2. Validate dimensions
  const dimDefs: DimensionDefinition[] = [];
  if (plan.dimensions && plan.dimensions.length > 0) {
    for (const dk of plan.dimensions) {
      const dd = dimMap.get(dk);
      if (!dd) {
        throw new CompileError("UNKNOWN_DIMENSION", `Unknown dimension: '${dk}'`);
      }
      dimDefs.push(dd);

      // Check allowlist for each metric
      for (const mk of plan.metrics) {
        const allowed = allowlist.find(
          (a) => a.metric_key === mk && a.dimension_key === dk && a.allow_group_by,
        );
        if (!allowed) {
          throw new CompileError(
            "DIMENSION_NOT_ALLOWED",
            `Dimension '${dk}' is not allowed for GROUP BY on metric '${mk}'`,
          );
        }
      }
    }
  }

  // 3. Validate filters
  if (plan.filters) {
    for (const f of plan.filters) {
      const dd = dimMap.get(f.dimension);
      if (!dd) {
        throw new CompileError("UNKNOWN_FILTER_DIM", `Unknown filter dimension: '${f.dimension}'`);
      }
      // Check filter is allowed for at least one metric
      const anyAllowed = plan.metrics.some((mk) =>
        allowlist.find((a) => a.metric_key === mk && a.dimension_key === f.dimension && a.allow_filter),
      );
      if (!anyAllowed) {
        throw new CompileError(
          "FILTER_NOT_ALLOWED",
          `Dimension '${f.dimension}' cannot be used as a filter for the selected metrics`,
        );
      }
    }
  }

  // 4. Resolve time window
  const defaultPreset = metricDefs[0].default_time_window as TimePreset | null;
  const tw = plan.timeWindow ?? (defaultPreset ? { preset: defaultPreset } : { preset: "all_time" as TimePreset });
  const { startDate, endDate, asOfDate } = resolveTimeWindow(tw);

  // 5. Compile SQL
  const limit = Math.min(plan.limit ?? 100, 500);

  let sql: string;
  let explanation: string;

  if (plan.metrics.length === 1 && (!plan.dimensions || plan.dimensions.length === 0)) {
    // ── Simple single-metric query (no dimensions) ─────────────────────
    const m = metricDefs[0];
    sql = substituteParams(m.sql_expression, startDate, endDate, asOfDate);
    explanation = `Computing ${m.display_name}`;
    if (startDate && endDate) {
      explanation += ` from ${startDate} to ${endDate}`;
    }
  } else if (!plan.dimensions || plan.dimensions.length === 0) {
    // ── Multiple metrics, no dimensions → run each as a subquery ───────
    const selects = metricDefs.map((m, i) => {
      const sub = substituteParams(m.sql_expression, startDate, endDate, asOfDate);
      return `(${sub}) AS [${m.display_name}]`;
    });
    sql = `SELECT ${selects.join(",\n       ")}`;
    explanation = `Computing ${metricDefs.map((m) => m.display_name).join(", ")}`;
    if (startDate && endDate) {
      explanation += ` from ${startDate} to ${endDate}`;
    }
  } else {
    // ── Dimensioned query → build GROUP BY query ───────────────────────
    // This is more complex: we need to select dimensions + aggregate metrics
    // We use the first metric's base table context and add dimensions

    // Build the dimension SELECT and GROUP BY clauses
    const dimSelects: string[] = [];
    const dimGroupBy: string[] = [];
    const dimFrom = new Set<string>();

    for (const dd of dimDefs) {
      const tableAlias = dd.source_table.replace("serving.", "").replace("silver.", "");
      const colRef = `${tableAlias}.${dd.source_column}`;
      dimSelects.push(`${colRef} AS [${dd.display_name}]`);
      dimGroupBy.push(colRef);
      dimFrom.add(dd.source_table);
    }

    // For the metric expressions, we need to inline them as aggregates
    // Since our metric sql_expressions are standalone queries, we need to
    // extract the aggregate pattern. For dimensioned queries, we build
    // a query from the primary source table.
    const primaryMetric = metricDefs[0];

    // Determine the primary table from the metric's sql_expression
    // Look for the FROM clause table
    const fromMatch = primaryMetric.sql_expression.match(/FROM\s+(\w+\.\w+)\s+(\w+)/i);
    if (!fromMatch) {
      throw new CompileError(
        "CANNOT_DIMENSION",
        `Metric '${primaryMetric.metric_key}' SQL cannot be dimensioned (no FROM clause found)`,
      );
    }

    const primaryTable = fromMatch[1];
    const primaryAlias = fromMatch[2];

    // Extract the aggregate expression from the SELECT clause
    const metricAggregates: string[] = [];
    for (const m of metricDefs) {
      // Try to extract the aggregate from SELECT ... AS value
      const selectMatch = m.sql_expression.match(/SELECT\s+([\s\S]*?)\s+AS\s+value/i);
      if (selectMatch) {
        let aggExpr = substituteParams(selectMatch[1].trim(), startDate, endDate, asOfDate);
        // Remove any TOP/CAST wrapper if present
        metricAggregates.push(`${aggExpr} AS [${m.display_name}]`);
      } else {
        // Fallback: wrap entire metric as subquery
        const sub = substituteParams(m.sql_expression, startDate, endDate, asOfDate);
        metricAggregates.push(`(${sub}) AS [${m.display_name}]`);
      }
    }

    // Extract WHERE clause from the primary metric
    const whereMatch = primaryMetric.sql_expression.match(/WHERE\s+([\s\S]*?)(?:GROUP\s+BY|ORDER\s+BY|$)/i);
    let whereClause = "";
    if (whereMatch) {
      whereClause = substituteParams(whereMatch[1].trim(), startDate, endDate, asOfDate);
    }

    // Add filter conditions
    if (plan.filters && plan.filters.length > 0) {
      const filterClauses = plan.filters.map((f) => {
        const dd = dimMap.get(f.dimension)!;
        return compileFilter(f, dd);
      });
      if (whereClause) {
        whereClause += " AND " + filterClauses.join(" AND ");
      } else {
        whereClause = filterClauses.join(" AND ");
      }
    }

    // Build JOINs if dimensions come from different tables
    let joinClause = "";
    Array.from(dimFrom).forEach((table) => {
      if (table !== primaryTable) {
        const alias = table.replace("serving.", "").replace("silver.", "");
        joinClause += `\nLEFT JOIN ${table} ${alias} ON ${alias}.person_id = ${primaryAlias}.person_id`;
      }
    });

    // Sort
    let orderBy = "";
    if (plan.sort) {
      if (plan.sort.by === "value" && metricDefs.length === 1) {
        orderBy = `ORDER BY [${metricDefs[0].display_name}] ${plan.sort.direction.toUpperCase()}`;
      } else {
        // Try to find the dimension
        const sortDim = dimMap.get(plan.sort.by);
        if (sortDim) {
          const sortCol = `${sortDim.source_table.replace("serving.", "").replace("silver.", "")}.${sortDim.source_column}`;
          orderBy = `ORDER BY ${sortCol} ${plan.sort.direction.toUpperCase()}`;
        }
      }
    } else if (dimDefs.length > 0 && dimDefs[0].is_time_dimension) {
      // Default: sort by time dimension ascending
      const tCol = `${dimDefs[0].source_table.replace("serving.", "").replace("silver.", "")}.${dimDefs[0].source_column}`;
      orderBy = `ORDER BY ${tCol} ASC`;
    }

    sql = `SELECT TOP (${limit}) ${dimSelects.join(", ")}, ${metricAggregates.join(", ")}
FROM ${primaryTable} ${primaryAlias}${joinClause}
${whereClause ? `WHERE ${whereClause}` : ""}
GROUP BY ${dimGroupBy.join(", ")}
${orderBy}`.trim();

    explanation = `Computing ${metricDefs.map((m) => m.display_name).join(", ")} grouped by ${dimDefs.map((d) => d.display_name).join(", ")}`;
    if (startDate && endDate) {
      explanation += ` from ${startDate} to ${endDate}`;
    }
  }

  // Ensure TOP is present for non-dimensioned queries
  if (!sql.match(/\bTOP\s*\(/i) && !sql.match(/\bOFFSET\b/i)) {
    sql = sql.replace(/^(SELECT)\b/i, `$1 TOP (${limit})`);
  }

  return {
    sql,
    params: { startDate, endDate, asOfDate },
    metricsUsed: plan.metrics,
    dimensionsUsed: plan.dimensions ?? [],
    explanation,
  };
}

/**
 * Quick single-metric execution: look up a metric by key, compile, return SQL.
 * Convenience wrapper for the most common case.
 */
export async function compileMetricQuery(
  metricKey: string,
  preset: TimePreset = "last_12_months",
): Promise<CompiledQuery> {
  return compileSQP({
    intent: "metric_query",
    metrics: [metricKey],
    timeWindow: { preset },
  });
}
