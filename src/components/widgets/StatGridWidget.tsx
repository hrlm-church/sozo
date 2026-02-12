"use client";

import type { Widget } from "@/types/widget";

export function StatGridWidget({ widget }: { widget: Widget }) {
  const { config } = widget;
  const stats = config.stats ?? [];

  if (!stats.length) return <div style={{ padding: 16, color: "var(--text-muted)" }}>No stats configured</div>;

  return (
    <div style={{
      display: "grid",
      gridTemplateColumns: `repeat(${Math.min(stats.length, 4)}, 1fr)`,
      gap: 12, height: "100%", alignContent: "center", padding: "4px 0",
    }}>
      {stats.map((stat, i) => {
        const arrow = stat.trend === "up" ? "\u25B2" : stat.trend === "down" ? "\u25BC" : "";
        const trendColor = stat.trend === "up" ? "var(--green)" : stat.trend === "down" ? "var(--red)" : "var(--text-muted)";
        const num = typeof stat.value === "number" ? stat.value : parseFloat(String(stat.value));
        let formatted: string;
        if (!isNaN(num)) {
          if (stat.unit === "$") {
            formatted = num >= 1_000_000 ? "$" + (num / 1_000_000).toFixed(1) + "M"
              : num >= 10_000 ? "$" + (num / 1_000).toFixed(1) + "K"
              : "$" + num.toLocaleString();
          } else if (stat.unit === "%") {
            formatted = num.toFixed(1) + "%";
          } else {
            formatted = num >= 10_000 ? (num / 1_000).toFixed(1) + "K" : num.toLocaleString();
          }
        } else {
          formatted = String(stat.value);
        }

        return (
          <div key={i} style={{
            textAlign: "center", padding: "16px 10px", borderRadius: "var(--r-md)",
            background: "var(--app-bg)",
          }}>
            <div style={{
              fontSize: "0.7rem", fontWeight: 500, color: "var(--text-muted)",
              marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em",
            }}>
              {stat.label}
            </div>
            <div style={{
              fontSize: "1.5rem", fontWeight: 700,
              background: "var(--accent-gradient)", WebkitBackgroundClip: "text",
              WebkitTextFillColor: "transparent",
              lineHeight: 1.1, letterSpacing: "-0.03em",
            }}>
              {formatted}
            </div>
            {arrow && (
              <div style={{
                display: "inline-flex", alignItems: "center", gap: 3,
                fontSize: "0.72rem", fontWeight: 500, color: trendColor, marginTop: 6,
              }}>
                <span style={{ fontSize: "0.55rem" }}>{arrow}</span>
                {stat.trend}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
