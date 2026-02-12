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
  const trendColor = trend === "up" ? "var(--green)" : trend === "down" ? "var(--red)" : "var(--text-muted)";

  return (
    <div style={{
      display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
      height: "100%", gap: 8, padding: "12px 0",
    }}>
      <div style={{
        fontSize: "3rem", fontWeight: 700, lineHeight: 1,
        background: "var(--accent-gradient)", WebkitBackgroundClip: "text",
        WebkitTextFillColor: "transparent", letterSpacing: "-0.04em",
      }}>
        {formatValue(config.value, config.numberFormat, config.unit)}
      </div>
      {config.delta !== undefined && (
        <div style={{
          display: "inline-flex", alignItems: "center", gap: 4,
          fontSize: "0.82rem", fontWeight: 500, color: trendColor,
        }}>
          {arrow && <span style={{ fontSize: "0.6rem" }}>{arrow}</span>}
          {config.delta}
        </div>
      )}
    </div>
  );
}
