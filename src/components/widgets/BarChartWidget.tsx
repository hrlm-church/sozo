"use client";

import type { Widget } from "@/types/widget";
import { ChartContainer, ChartTooltip, ChartLegend, type ChartConfig } from "@/components/ui/chart";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid } from "recharts";
import { pivotIfNeeded } from "./pivot-data";

const DEFAULT_COLORS = ["#0693e3", "#9b51e0", "#17c6b8", "#f59e0b", "#f43f5e", "#3ba4e8", "#ec4899", "#14b8a6"];

export function BarChartWidget({ widget }: { widget: Widget }) {
  const { config } = widget;
  const pivoted = pivotIfNeeded(widget.data, config);
  const data = pivoted.data;
  const categoryKey = config.categoryKey ?? Object.keys(data[0] ?? {})[0] ?? "name";
  const valueKeys = pivoted.valueKeys.length ? pivoted.valueKeys : (config.valueKeys ?? Object.keys(data[0] ?? {}).filter((k) => k !== categoryKey));
  const showLegend = valueKeys.length > 1;

  const chartConfig: ChartConfig = {};
  valueKeys.forEach((key, i) => {
    chartConfig[key] = {
      label: config.valueLabels?.[key] ?? key,
      color: pivoted.colors[key] ?? config.colors?.[key] ?? DEFAULT_COLORS[i % DEFAULT_COLORS.length],
    };
  });

  return (
    <ChartContainer config={chartConfig} style={{ height: "100%" }}>
      <BarChart data={data} margin={{ top: 8, right: 8, bottom: showLegend ? 4 : 0, left: 0 }}>
        <defs>
          {valueKeys.map((key) => (
            <linearGradient key={key} id={`bar-${key}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor={chartConfig[key].color} stopOpacity={0.9} />
              <stop offset="100%" stopColor={chartConfig[key].color} stopOpacity={0.6} />
            </linearGradient>
          ))}
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" vertical={false} />
        <XAxis dataKey={categoryKey} tick={{ fontSize: 11, fill: "var(--text-muted)" }} axisLine={false} tickLine={false} />
        <YAxis tick={{ fontSize: 11, fill: "var(--text-muted)" }} axisLine={false} tickLine={false} />
        <ChartTooltip />
        {showLegend && <ChartLegend />}
        {valueKeys.map((key) => (
          <Bar
            key={key}
            dataKey={key}
            fill={`url(#bar-${key})`}
            radius={[6, 6, 0, 0]}
            isAnimationActive={false}
          />
        ))}
      </BarChart>
    </ChartContainer>
  );
}
