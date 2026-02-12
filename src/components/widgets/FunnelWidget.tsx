"use client";

import type { Widget } from "@/types/widget";

const COLORS = ["#0693e3", "#9b51e0", "#17c6b8", "#f59e0b", "#f43f5e", "#3ba4e8"];

export function FunnelWidget({ widget }: { widget: Widget }) {
  const { data, config } = widget;
  const categoryKey = config.categoryKey ?? Object.keys(data[0] ?? {})[0] ?? "name";
  const valueKey = config.valueKeys?.[0] ?? Object.keys(data[0] ?? {}).find((k) => k !== categoryKey) ?? "value";

  const maxVal = Math.max(...data.map((r) => Number(r[valueKey]) || 0), 1);

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8, height: "100%", justifyContent: "center", padding: "8px 0" }}>
      {data.map((row, i) => {
        const val = Number(row[valueKey]) || 0;
        const pct = (val / maxVal) * 100;
        const label = String(row[categoryKey] ?? "");
        return (
          <div key={i} style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <div style={{ width: 90, fontSize: "0.8rem", color: "var(--text-muted)", textAlign: "right", flexShrink: 0 }}>
              {label}
            </div>
            <div style={{ flex: 1, background: "var(--surface-elevated)", borderRadius: 6, overflow: "hidden", height: 28 }}>
              <div
                style={{
                  width: `${pct}%`,
                  height: "100%",
                  background: COLORS[i % COLORS.length],
                  borderRadius: 6,
                  display: "flex",
                  alignItems: "center",
                  paddingLeft: 8,
                  fontSize: "0.75rem",
                  fontWeight: 600,
                  color: "#fff",
                  minWidth: 32,
                }}
              >
                {val.toLocaleString()}
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}
