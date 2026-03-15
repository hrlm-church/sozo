"use client";

import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid } from "recharts";

interface TrendChartProps {
  data: { date: string; value: number; comparison?: number }[];
  color?: string;
  height?: number;
  valueFormatter?: (v: number) => string;
  title?: string;
}

export function TrendChart({ data, color = "var(--accent)", height = 260, valueFormatter, title }: TrendChartProps) {
  const fmt = valueFormatter ?? ((v: number) => v.toLocaleString());

  if (!data || data.length === 0) {
    return (
      <div className="card-base" style={{ padding: "20px", textAlign: "center", color: "var(--text-muted)", fontSize: "0.82rem" }}>
        No trend data available
      </div>
    );
  }

  return (
    <div className="card-base" style={{ padding: "20px 20px 12px" }}>
      {title && (
        <div style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-secondary)", marginBottom: 12 }}>
          {title}
        </div>
      )}
      <ResponsiveContainer width="100%" height={height}>
        <AreaChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 8 }}>
          <defs>
            <linearGradient id={`trendGrad-${color}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={0.15} />
              <stop offset="100%" stopColor={color} stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" />
          <XAxis
            dataKey="date"
            tick={{ fontSize: 11, fill: "var(--text-muted)" }}
            tickLine={false}
            axisLine={false}
          />
          <YAxis
            tick={{ fontSize: 11, fill: "var(--text-muted)" }}
            tickLine={false}
            axisLine={false}
            tickFormatter={fmt}
          />
          <Tooltip
            contentStyle={{
              background: "var(--surface)",
              border: "1px solid var(--surface-border)",
              borderRadius: 10,
              fontSize: "0.78rem",
              boxShadow: "var(--shadow-md)",
            }}
            formatter={(value) => [fmt(Number(value ?? 0)), "Value"]}
          />
          <Area
            type="monotone"
            dataKey="value"
            stroke={color}
            strokeWidth={2}
            fill={`url(#trendGrad-${color})`}
          />
          {data[0]?.comparison !== undefined && (
            <Area
              type="monotone"
              dataKey="comparison"
              stroke="var(--text-muted)"
              strokeWidth={1.5}
              strokeDasharray="4 4"
              fill="none"
            />
          )}
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
