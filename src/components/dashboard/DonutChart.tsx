"use client";

import { ResponsiveContainer, PieChart, Pie, Cell, Tooltip } from "recharts";

interface DonutChartProps {
  data: { name: string; value: number; color: string }[];
  size?: number;
  showLegend?: boolean;
}

export function DonutChart({ data, size = 220, showLegend = true }: DonutChartProps) {
  if (!data || data.length === 0) {
    return (
      <div style={{ textAlign: "center", color: "var(--text-muted)", fontSize: "0.82rem", padding: 20 }}>
        No data available
      </div>
    );
  }

  return (
    <div style={{ display: "flex", alignItems: "center", gap: 24, flexWrap: "wrap" }}>
      <ResponsiveContainer width={size} height={size}>
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={size * 0.3}
            outerRadius={size * 0.42}
            paddingAngle={2}
            dataKey="value"
            strokeWidth={0}
          >
            {data.map((entry, index) => (
              <Cell key={index} fill={entry.color} />
            ))}
          </Pie>
          <Tooltip
            contentStyle={{
              background: "var(--surface)",
              border: "1px solid var(--surface-border)",
              borderRadius: 10,
              fontSize: "0.78rem",
              boxShadow: "var(--shadow-md)",
            }}
            formatter={(value, name) => [(value ?? 0).toLocaleString(), name ?? ""]}
          />
        </PieChart>
      </ResponsiveContainer>
      {showLegend && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {data.map((entry, i) => (
            <div key={i} style={{ display: "flex", alignItems: "center", gap: 8 }}>
              <span style={{ width: 10, height: 10, borderRadius: "50%", background: entry.color, flexShrink: 0 }} />
              <span style={{ fontSize: "0.8rem", color: "var(--text-secondary)" }}>{entry.name}</span>
              <span style={{ fontSize: "0.8rem", fontWeight: 600, color: "var(--text-primary)", marginLeft: 4 }}>
                {entry.value.toLocaleString()}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
