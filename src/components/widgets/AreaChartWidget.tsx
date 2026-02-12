"use client";

import type { Widget } from "@/types/widget";
import { ChartContainer, ChartTooltip, ChartLegend, type ChartConfig } from "@/components/ui/chart";
import { AreaChart, Area, XAxis, YAxis, CartesianGrid } from "recharts";
import { pivotIfNeeded } from "./pivot-data";

const DEFAULT_COLORS = ["#6f43ea", "#2f7ff6", "#17c6b8"];

export function AreaChartWidget({ widget }: { widget: Widget }) {
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
      <AreaChart data={data} margin={{ top: 8, right: 8, bottom: showLegend ? 4 : 0, left: 0 }}>
        <defs>
          {valueKeys.map((key) => (
            <linearGradient key={key} id={`area-${key}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor={chartConfig[key].color} stopOpacity={0.35} />
              <stop offset="95%" stopColor={chartConfig[key].color} stopOpacity={0.02} />
            </linearGradient>
          ))}
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" vertical={false} />
        <XAxis dataKey={categoryKey} tick={{ fontSize: 11, fill: "var(--text-muted)" }} axisLine={false} tickLine={false} />
        <YAxis tick={{ fontSize: 11, fill: "var(--text-muted)" }} axisLine={false} tickLine={false} />
        <ChartTooltip />
        {showLegend && <ChartLegend />}
        {valueKeys.map((key) => (
          <Area
            key={key}
            type="monotone"
            dataKey={key}
            stroke={chartConfig[key].color}
            fill={`url(#area-${key})`}
            strokeWidth={2.5}
            isAnimationActive={false}
          />
        ))}
      </AreaChart>
    </ChartContainer>
  );
}
