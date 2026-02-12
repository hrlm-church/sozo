"use client";

import type { Widget } from "@/types/widget";

const GRID_COLORS = ["#6f43ea", "#2f7ff6", "#17c6b8", "#f59e0b"];

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
        const trendColor = stat.trend === "up" ? "#10b981" : stat.trend === "down" ? "#ef4444" : "var(--text-muted)";
        const accent = GRID_COLORS[i % GRID_COLORS.length];
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
            textAlign: "center", padding: "16px 10px", borderRadius: 14,
            background: `linear-gradient(135deg, ${accent}08 0%, ${accent}04 100%)`,
            border: `1px solid ${accent}18`,
            transition: "transform 150ms ease, box-shadow 150ms ease",
          }}>
            <div style={{
              fontSize: "0.72rem", fontWeight: 600, color: "var(--text-muted)",
              marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em",
            }}>
              {stat.label}
            </div>
            <div style={{
              fontSize: "1.6rem", fontWeight: 800, color: "var(--text-primary)",
              lineHeight: 1.1, letterSpacing: "-0.02em",
            }}>
              {formatted}
            </div>
            {arrow && (
              <div style={{
                display: "inline-flex", alignItems: "center", gap: 3,
                fontSize: "0.72rem", fontWeight: 600, color: trendColor,
                marginTop: 6,
              }}>
                <span style={{ fontSize: "0.6rem" }}>{arrow}</span>
                {stat.trend}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}
