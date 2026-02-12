"use client";

import type { Widget } from "@/types/widget";

function formatValue(val: string | number | undefined, format?: string, unit?: string): string {
  if (val === undefined || val === null) return "\u2014";
  const num = typeof val === "string" ? parseFloat(val) : val;
  if (isNaN(num)) return String(val);

  if (format === "currency" || unit === "$") {
    if (num >= 1_000_000) return "$" + (num / 1_000_000).toFixed(1) + "M";
    if (num >= 10_000) return "$" + (num / 1_000).toFixed(1) + "K";
    return "$" + num.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  }
  if (format === "percent" || unit === "%") return num.toFixed(1) + "%";
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1) + "M";
  if (num >= 10_000) return (num / 1_000).toFixed(1) + "K";
  return num.toLocaleString();
}

export function KpiWidget({ widget }: { widget: Widget }) {
  const { config } = widget;
  const trend = config.trend;
  const arrow = trend === "up" ? "\u25B2" : trend === "down" ? "\u25BC" : "";
  const trendColor = trend === "up" ? "#10b981" : trend === "down" ? "#ef4444" : "var(--text-muted)";
  const trendBg = trend === "up" ? "rgba(16,185,129,0.1)" : trend === "down" ? "rgba(239,68,68,0.1)" : "transparent";

  return (
    <div style={{
      display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
      height: "100%", gap: 8, padding: "12px 0",
    }}>
      <div style={{
        fontSize: "2.8rem", fontWeight: 800, lineHeight: 1,
        color: "var(--text-primary)", letterSpacing: "-0.02em",
      }}>
        {formatValue(config.value, config.numberFormat, config.unit)}
      </div>
      {config.delta !== undefined && (
        <div style={{
          display: "inline-flex", alignItems: "center", gap: 4,
          fontSize: "0.85rem", fontWeight: 600, color: trendColor,
          background: trendBg, padding: "4px 12px", borderRadius: 20,
        }}>
          {arrow && <span style={{ fontSize: "0.65rem" }}>{arrow}</span>}
          {config.delta}
        </div>
      )}
    </div>
  );
}
