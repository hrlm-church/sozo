"use client";

import { useState, useMemo } from "react";
import type { Widget } from "@/types/widget";

/** Column names that indicate counts or IDs — never format as currency, never sum in summaries */
const COUNT_HINTS = /count|number|num|qty|quantity|_id$|_pk$|rank|position|gifts/i;
/** Column names that should be formatted as currency */
const CURRENCY_HINTS = /amount|total|giving|donation|revenue|price|cost|payment|invoice|ltv|monetary|salary|budget|balance|given|spent/i;
/** Column names that should NOT be summed in drill-down summary rows */
const NO_SUM_HINTS = /person_id|_id$|_pk$|year$|donation_year|order_year|_year/i;
/** ISO date strings */
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

function formatHeader(col: string): string {
  return col.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function formatCell(val: unknown, col: string): string {
  if (val === null || val === undefined) return "\u2014";
  if (typeof val === "string" && ISO_DATE_RE.test(val)) {
    try {
      const d = new Date(val);
      if (!isNaN(d.getTime())) return d.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
    } catch { /* fall through */ }
    return val;
  }
  if (typeof val === "number") {
    const isCount = COUNT_HINTS.test(col);
    const looksMonetary = !isCount && CURRENCY_HINTS.test(col);
    if (looksMonetary) return "$" + val.toLocaleString(undefined, { minimumFractionDigits: 2 });
    if (Number.isInteger(val)) return val.toLocaleString();
    return val.toLocaleString(undefined, { maximumFractionDigits: 2 });
  }
  return String(val);
}

interface GroupData {
  key: string;
  summary: Record<string, unknown>;
  rows: Record<string, unknown>[];
}

export function DrillDownTableWidget({ widget }: { widget: Widget }) {
  const { data, config } = widget;
  const groupKey = config.groupKey ?? config.seriesKey ?? "";
  const summaryColumns = config.summaryColumns as string[] | undefined;
  const detailColumns = config.detailColumns as string[] | undefined;

  // Build groups
  const groups = useMemo(() => {
    if (!groupKey || !data.length) return [] as GroupData[];

    const allCols = Object.keys(data[0]);
    // Only sum columns that are numeric AND not IDs/years
    const numericCols = allCols.filter(
      (c) => c !== groupKey && !NO_SUM_HINTS.test(c) && data.some((r) => typeof r[c] === "number"),
    );

    const map = new Map<string, Record<string, unknown>[]>();
    const order: string[] = [];
    for (const row of data) {
      const k = String(row[groupKey] ?? "");
      if (!map.has(k)) { map.set(k, []); order.push(k); }
      map.get(k)!.push(row);
    }

    return order.map((key) => {
      const rows = map.get(key)!;
      // Auto-compute summary: sum numeric columns (excluding IDs/years), count rows
      const summary: Record<string, unknown> = { [groupKey]: key, _count: rows.length };
      for (const col of numericCols) {
        summary[col] = rows.reduce((acc, r) => acc + (typeof r[col] === "number" ? (r[col] as number) : 0), 0);
      }
      return { key, summary, rows };
    });
  }, [data, groupKey]);

  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  if (!groupKey || !data || !data.length) {
    return (
      <div style={{ padding: 16, color: "var(--text-muted)", fontSize: "0.85rem" }}>
        {!groupKey ? "Waiting for data configuration..." : "No data rows returned from query."}
      </div>
    );
  }

  const allCols = Object.keys(data[0]);
  const detailCols = detailColumns ?? allCols.filter((c) => c !== groupKey);

  // Always include groupKey as first summary column + count at end
  // Exclude ID/year columns from auto-computed summaries
  const baseSummary = summaryColumns ?? allCols.filter(
    (c) => c !== groupKey && !NO_SUM_HINTS.test(c) && data.some((r) => typeof r[c] === "number"),
  );
  const summaryCols = [
    // groupKey always first so the name is always visible
    ...(baseSummary.includes(groupKey) ? [] : [groupKey]),
    ...baseSummary,
    // row count always last
    ...(baseSummary.includes("_count") ? [] : ["_count"]),
  ];

  const toggle = (key: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key); else next.add(key);
      return next;
    });
  };

  const thStyle: React.CSSProperties = {
    position: "sticky", top: 0, background: "var(--surface-strong)",
    borderBottom: "2px solid var(--surface-border)", padding: "8px 10px",
    textAlign: "left", fontWeight: 600, color: "var(--text-muted)",
    whiteSpace: "nowrap", fontSize: "0.8rem",
  };

  return (
    <div style={{ overflow: "auto", maxHeight: "100%", fontSize: "0.8rem" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            <th style={{ ...thStyle, width: 28 }}></th>
            {summaryCols.map((col) => (
              <th key={col} style={thStyle}>
                {config.valueLabels?.[col] ?? (col === "_count" ? "Count" : formatHeader(col))}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {groups.map((group) => {
            const isOpen = expanded.has(group.key);
            return (
              <GroupRows
                key={group.key}
                group={group}
                isOpen={isOpen}
                onToggle={() => toggle(group.key)}
                summaryCols={summaryCols}
                detailCols={detailCols}
                groupKey={groupKey}
                config={config}
              />
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function GroupRows({
  group, isOpen, onToggle, summaryCols, detailCols, groupKey, config,
}: {
  group: GroupData;
  isOpen: boolean;
  onToggle: () => void;
  summaryCols: string[];
  detailCols: string[];
  groupKey: string;
  config: { valueLabels?: Record<string, string> };
}) {
  const valueLabels = config.valueLabels ?? {};

  return (
    <>
      {/* Summary row — clickable */}
      <tr
        onClick={onToggle}
        style={{
          cursor: "pointer",
          background: isOpen ? "var(--surface-strong)" : "transparent",
          borderBottom: "1px solid var(--surface-border)",
          fontWeight: 600,
        }}
      >
        <td style={{ padding: "6px 8px", fontSize: "0.9rem", color: "var(--text-muted)" }}>
          {isOpen ? "\u25BC" : "\u25B6"}
        </td>
        {summaryCols.map((col) => (
          <td key={col} style={{ padding: "6px 10px", whiteSpace: "nowrap", color: "var(--text-primary)" }}>
            {formatCell(group.summary[col], col)}
          </td>
        ))}
      </tr>

      {/* Detail rows — visible when expanded */}
      {isOpen && (
        <>
          {/* Detail header */}
          <tr style={{ background: "var(--surface)" }}>
            <td></td>
            {detailCols.map((col) => (
              <td
                key={col}
                colSpan={col === detailCols[detailCols.length - 1] ? summaryCols.length - detailCols.length + 1 : 1}
                style={{
                  padding: "4px 10px", fontSize: "0.72rem", fontWeight: 600,
                  color: "var(--text-muted)", borderBottom: "1px solid var(--surface-border)",
                }}
              >
                {valueLabels[col] ?? formatHeader(col)}
              </td>
            ))}
          </tr>
          {group.rows.map((row, i) => (
            <tr
              key={i}
              style={{
                background: i % 2 === 0 ? "var(--surface)" : "transparent",
                borderBottom: "1px solid var(--surface-border)",
              }}
            >
              <td></td>
              {detailCols.map((col) => (
                <td
                  key={col}
                  colSpan={col === detailCols[detailCols.length - 1] ? summaryCols.length - detailCols.length + 1 : 1}
                  style={{ padding: "4px 10px", whiteSpace: "nowrap", color: "var(--text-secondary)", fontSize: "0.78rem" }}
                >
                  {formatCell(row[col], col)}
                </td>
              ))}
            </tr>
          ))}
        </>
      )}
    </>
  );
}
