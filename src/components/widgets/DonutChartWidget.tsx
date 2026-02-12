"use client";

import type { Widget } from "@/types/widget";
import { ChartContainer, ChartTooltip, type ChartConfig } from "@/components/ui/chart";
import { PieChart, Pie, Cell, Legend } from "recharts";

const DEFAULT_COLORS = ["#0693e3", "#9b51e0", "#17c6b8", "#f59e0b", "#f43f5e", "#3ba4e8", "#ec4899", "#14b8a6"];

export function DonutChartWidget({ widget }: { widget: Widget }) {
  const { data, config } = widget;
  const categoryKey = config.categoryKey ?? Object.keys(data[0] ?? {})[0] ?? "name";
  const valueKey = config.valueKeys?.[0] ?? Object.keys(data[0] ?? {}).find((k) => k !== categoryKey) ?? "value";

  const chartConfig: ChartConfig = {};
  const colors: string[] = [];
  data.forEach((row, i) => {
    const label = String(row[categoryKey] ?? `Item ${i}`);
    const color = config.colors?.[label] ?? DEFAULT_COLORS[i % DEFAULT_COLORS.length];
    chartConfig[label] = { label, color };
    colors.push(color);
  });

  return (
    <ChartContainer config={chartConfig} style={{ height: "100%" }}>
      <PieChart>
        <defs>
          {colors.map((color, i) => (
            <linearGradient key={i} id={`donut-${i}`} x1="0" y1="0" x2="1" y2="1">
              <stop offset="0%" stopColor={color} stopOpacity={1} />
              <stop offset="100%" stopColor={color} stopOpacity={0.7} />
            </linearGradient>
          ))}
        </defs>
        <Pie
          data={data}
          dataKey={valueKey}
          nameKey={categoryKey}
          cx="50%"
          cy="50%"
          innerRadius="52%"
          outerRadius="82%"
          paddingAngle={3}
          cornerRadius={4}
          label={({ name, percent }) => `${name} ${((percent ?? 0) * 100).toFixed(0)}%`}
          labelLine={false}
          style={{ fontSize: 11, fontWeight: 600 }}
          isAnimationActive={false}
        >
          {data.map((_row, i) => (
            <Cell key={i} fill={`url(#donut-${i})`} stroke="none" />
          ))}
        </Pie>
        <ChartTooltip />
        <Legend
          wrapperStyle={{ fontSize: "0.75rem", paddingTop: 8 }}
          formatter={(value: string) => <span style={{ color: "var(--text-muted)", fontWeight: 500 }}>{value}</span>}
        />
      </PieChart>
    </ChartContainer>
  );
}
