"use client";

import React from "react";
import type { Widget } from "@/types/widget";
import { KpiWidget } from "./KpiWidget";
import { BarChartWidget } from "./BarChartWidget";
import { LineChartWidget } from "./LineChartWidget";
import { AreaChartWidget } from "./AreaChartWidget";
import { DonutChartWidget } from "./DonutChartWidget";
import { TableWidget } from "./TableWidget";
import { DrillDownTableWidget } from "./DrillDownTableWidget";
import { StatGridWidget } from "./StatGridWidget";
import { FunnelWidget } from "./FunnelWidget";
import { TextWidget } from "./TextWidget";
import { WidgetCard } from "./WidgetCard";

interface WidgetRendererProps {
  widget: Widget;
  onPin?: () => void;
  onRemove?: () => void;
  isPinned?: boolean;
}

/** Error boundary to catch rendering crashes in individual widgets */
class WidgetErrorBoundary extends React.Component<
  { children: React.ReactNode; title: string },
  { error: Error | null }
> {
  constructor(props: { children: React.ReactNode; title: string }) {
    super(props);
    this.state = { error: null };
  }
  static getDerivedStateFromError(error: Error) {
    return { error };
  }
  render() {
    if (this.state.error) {
      return (
        <div style={{ padding: 16, color: "#f43f5e", fontSize: "0.85rem" }}>
          <strong>Widget error:</strong> {this.state.error.message}
        </div>
      );
    }
    return this.props.children;
  }
}

function WidgetContent({ widget }: { widget: Widget }) {
  switch (widget.type) {
    case "kpi":
      return <KpiWidget widget={widget} />;
    case "bar_chart":
      return <BarChartWidget widget={widget} />;
    case "line_chart":
      return <LineChartWidget widget={widget} />;
    case "area_chart":
      return <AreaChartWidget widget={widget} />;
    case "donut_chart":
      return <DonutChartWidget widget={widget} />;
    case "table":
      return <TableWidget widget={widget} />;
    case "drill_down_table":
      return <DrillDownTableWidget widget={widget} />;
    case "stat_grid":
      return <StatGridWidget widget={widget} />;
    case "funnel":
      return <FunnelWidget widget={widget} />;
    case "text":
      return <TextWidget widget={widget} />;
    default:
      return <div style={{ color: "var(--text-muted)" }}>Unknown widget type: {widget.type}</div>;
  }
}

export function WidgetRenderer({ widget, onPin, onRemove, isPinned }: WidgetRendererProps) {
  return (
    <WidgetCard widget={widget} onPin={onPin} onRemove={onRemove} isPinned={isPinned}>
      <WidgetErrorBoundary title={widget.title}>
        <WidgetContent widget={widget} />
      </WidgetErrorBoundary>
    </WidgetCard>
  );
}
