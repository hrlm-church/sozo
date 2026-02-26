"use client";

import { useState } from "react";
import type { Widget } from "@/types/widget";

/** Column names that indicate counts — never format as currency */
const COUNT_HINTS = /count|number|num|qty|quantity|_id$|_pk$|rank|position/i;

/** Column names that should be formatted as currency (only if NOT a count column) */
const CURRENCY_HINTS = /amount|total|giving|donation|gift|revenue|price|cost|payment|invoice|ltv|monetary|salary|budget|balance/i;

/** Detect ISO date strings like 2024-02-16T05:00:00.000Z */
const ISO_DATE_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/;

const PAGE_SIZE = 50;

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
  const [page, setPage] = useState(0);

  if (!data.length) return <div style={{ padding: 16, color: "var(--text-muted)" }}>No data</div>;

  const columns = Object.keys(data[0]);
  const totalPages = Math.ceil(data.length / PAGE_SIZE);
  const start = page * PAGE_SIZE;
  const displayRows = data.slice(start, start + PAGE_SIZE);

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
        return "$" + val.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      }
      if (config.numberFormat === "percent") return val.toFixed(1) + "%";
      // Count-like integers: no decimals
      if (Number.isInteger(val)) return val.toLocaleString();
      return val.toLocaleString(undefined, { maximumFractionDigits: 2 });
    }

    return String(val);
  };

  const isWide = columns.length >= 6;

  return (
    <div style={{ display: "flex", flexDirection: "column", maxHeight: "100%" }}>
      <div style={{ overflow: "auto", flex: 1, minHeight: 0, fontSize: isWide ? "0.76rem" : "0.8rem" }}>
        <table style={{ width: "100%", borderCollapse: "collapse" }}>
          <thead>
            <tr>
              {columns.map((col, ci) => (
                <th
                  key={col}
                  style={{
                    position: "sticky",
                    top: 0,
                    ...(ci === 0 ? { left: 0, zIndex: 2 } : {}),
                    background: "var(--surface-strong)",
                    borderBottom: "2px solid var(--surface-border)",
                    padding: isWide ? "6px 8px" : "8px 10px",
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
                key={start + i}
                style={{
                  borderBottom: "1px solid var(--surface-border)",
                  background: i % 2 === 0 ? "transparent" : "var(--surface)",
                }}
              >
                {columns.map((col, ci) => (
                  <td
                    key={col}
                    style={{
                      padding: isWide ? "5px 8px" : "6px 10px",
                      whiteSpace: "nowrap",
                      color: "var(--text-primary)",
                      ...(ci === 0 ? {
                        position: "sticky" as const,
                        left: 0,
                        background: i % 2 === 0 ? "var(--surface-elevated, #fff)" : "var(--surface)",
                        zIndex: 1,
                        fontWeight: 500,
                      } : {}),
                    }}
                  >
                    {formatCell(row[col], col)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination bar */}
      {totalPages > 1 && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "8px 12px",
            borderTop: "1px solid var(--surface-border)",
            fontSize: "0.75rem",
            color: "var(--text-muted)",
            flexShrink: 0,
          }}
        >
          <span>
            {start + 1}–{Math.min(start + PAGE_SIZE, data.length)} of {data.length} rows
          </span>
          <div style={{ display: "flex", gap: 4 }}>
            <PaginationBtn
              onClick={() => setPage(0)}
              disabled={page === 0}
              label="«"
            />
            <PaginationBtn
              onClick={() => setPage(page - 1)}
              disabled={page === 0}
              label="‹"
            />
            {/* Page numbers */}
            {getPageNumbers(page, totalPages).map((p, i) =>
              p === -1 ? (
                <span key={`dot-${i}`} style={{ padding: "0 4px", color: "var(--text-muted)" }}>…</span>
              ) : (
                <PaginationBtn
                  key={p}
                  onClick={() => setPage(p)}
                  disabled={false}
                  label={String(p + 1)}
                  active={p === page}
                />
              )
            )}
            <PaginationBtn
              onClick={() => setPage(page + 1)}
              disabled={page >= totalPages - 1}
              label="›"
            />
            <PaginationBtn
              onClick={() => setPage(totalPages - 1)}
              disabled={page >= totalPages - 1}
              label="»"
            />
          </div>
        </div>
      )}
    </div>
  );
}

/** Generate page numbers with ellipsis for large page counts */
function getPageNumbers(current: number, total: number): number[] {
  if (total <= 7) return Array.from({ length: total }, (_, i) => i);
  const pages: number[] = [];
  pages.push(0);
  if (current > 2) pages.push(-1); // ellipsis
  for (let i = Math.max(1, current - 1); i <= Math.min(total - 2, current + 1); i++) {
    pages.push(i);
  }
  if (current < total - 3) pages.push(-1); // ellipsis
  pages.push(total - 1);
  return pages;
}

function PaginationBtn({ onClick, disabled, label, active }: {
  onClick: () => void;
  disabled: boolean;
  label: string;
  active?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      style={{
        background: active ? "var(--accent)" : "none",
        color: active ? "#fff" : disabled ? "var(--text-muted)" : "var(--text-primary)",
        border: active ? "none" : "1px solid var(--surface-border)",
        borderRadius: 4,
        padding: "2px 8px",
        fontSize: "0.75rem",
        fontWeight: active ? 600 : 400,
        cursor: disabled ? "default" : "pointer",
        opacity: disabled ? 0.4 : 1,
        minWidth: 28,
        transition: "all 120ms ease",
      }}
      onMouseEnter={(e) => {
        if (!disabled && !active) {
          e.currentTarget.style.background = "var(--accent-light)";
          e.currentTarget.style.borderColor = "var(--accent)";
        }
      }}
      onMouseLeave={(e) => {
        if (!disabled && !active) {
          e.currentTarget.style.background = "none";
          e.currentTarget.style.borderColor = "var(--surface-border)";
        }
      }}
    >
      {label}
    </button>
  );
}
