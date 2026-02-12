"use client";

import type { Widget } from "@/types/widget";

/** Column names that indicate counts — never format as currency */
const COUNT_HINTS = /count|number|num|qty|quantity|_id$|_pk$|rank|position/i;

/** Column names that should be formatted as currency (only if NOT a count column) */
const CURRENCY_HINTS = /amount|total|giving|donation|gift|revenue|price|cost|payment|invoice|ltv|monetary|salary|budget|balance/i;

/** Detect ISO date strings like 2024-02-16T05:00:00.000Z */
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

/** Format column header: replace underscores, title case */
function formatHeader(col: string): string {
  return col
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/** Format a date string to a readable format */
function formatDate(val: string): string {
  try {
    const d = new Date(val);
    if (isNaN(d.getTime())) return val;
    return d.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
  } catch {
    return val;
  }
}

export function TableWidget({ widget }: { widget: Widget }) {
  const { data, config } = widget;
  if (!data.length) return <div style={{ padding: 16, color: "var(--text-muted)" }}>No data</div>;

  const columns = Object.keys(data[0]);
  const displayRows = data.slice(0, 100);

  const formatCell = (val: unknown, col: string): string => {
    if (val === null || val === undefined) return "\u2014";

    // Date strings
    if (typeof val === "string" && ISO_DATE_RE.test(val)) {
      return formatDate(val);
    }

    // Numbers — smart format based on column name
    if (typeof val === "number") {
      const isCount = COUNT_HINTS.test(col);
      const looksMonetary = !isCount && CURRENCY_HINTS.test(col);
      if (looksMonetary || (config.numberFormat === "currency" && !isCount)) {
        return "$" + val.toLocaleString(undefined, { minimumFractionDigits: 2 });
      }
      if (config.numberFormat === "percent") return val.toFixed(1) + "%";
      // Count-like integers: no decimals
      if (Number.isInteger(val)) return val.toLocaleString();
      return val.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }

    return String(val);
  };

  return (
    <div style={{ overflow: "auto", maxHeight: "100%", fontSize: "0.8rem" }}>
      <table style={{ width: "100%", borderCollapse: "collapse" }}>
        <thead>
          <tr>
            {columns.map((col) => (
              <th
                key={col}
                style={{
                  position: "sticky",
                  top: 0,
                  background: "var(--surface-strong)",
                  borderBottom: "2px solid var(--surface-border)",
                  padding: "8px 10px",
                  textAlign: "left",
                  fontWeight: 600,
                  color: "var(--text-muted)",
                  whiteSpace: "nowrap",
                }}
              >
                {config.valueLabels?.[col] ?? formatHeader(col)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayRows.map((row, i) => (
            <tr
              key={i}
              style={{
                borderBottom: "1px solid var(--surface-border)",
                background: i % 2 === 0 ? "transparent" : "var(--surface)",
              }}
            >
              {columns.map((col) => (
                <td
                  key={col}
                  style={{
                    padding: "6px 10px",
                    whiteSpace: "nowrap",
                    color: "var(--text-primary)",
                  }}
                >
                  {formatCell(row[col], col)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
      {data.length > 100 && (
        <div style={{ padding: 8, color: "var(--text-muted)", textAlign: "center", fontSize: "0.75rem" }}>
          Showing 100 of {data.length} rows
        </div>
      )}
    </div>
  );
}
