export type WidgetType =
  | "kpi"
  | "bar_chart"
  | "line_chart"
  | "area_chart"
  | "donut_chart"
  | "table"
  | "drill_down_table"
  | "funnel"
  | "stat_grid"
  | "text";

export interface Widget {
  id: string;
  type: WidgetType;
  title: string;
  /** SQL query that produced this widget's data */
  sql?: string;
  /** Structured data rows from the query */
  data: Record<string, unknown>[];
  /** Chart config: axis keys, colors, formatting */
  config: WidgetConfig;
  /** When the widget was created */
  createdAt: string;
}

export interface WidgetConfig {
  /** Key in data[] for the x-axis / category */
  categoryKey?: string;
  /** Keys in data[] for the y-axis / values */
  valueKeys?: string[];
  /** Key in data[] to split into separate series (auto-pivots long-format data to wide-format) */
  seriesKey?: string;
  /** Key in data[] to group rows by for drill-down tables */
  groupKey?: string;
  /** Columns to show in the summary (collapsed) row */
  summaryColumns?: string[];
  /** Columns to show in the detail (expanded) rows */
  detailColumns?: string[];
  /** Display labels for each value key */
  valueLabels?: Record<string, string>;
  /** Colors for each value key (hex) */
  colors?: Record<string, string>;
  /** For KPI: the main value */
  value?: string | number;
  /** For KPI: comparison/delta */
  delta?: string | number;
  /** For KPI: trend direction */
  trend?: "up" | "down" | "flat";
  /** For KPI: unit label (e.g., "$", "%") */
  unit?: string;
  /** For text: markdown content */
  markdown?: string;
  /** For stat_grid: array of mini-KPI items */
  stats?: Array<{
    label: string;
    value: string | number;
    unit?: string;
    trend?: "up" | "down" | "flat";
  }>;
  /** Number format: "currency" | "percent" | "number" */
  numberFormat?: "currency" | "percent" | "number";
}

export interface WidgetLayout {
  /** Matches Widget.id */
  i: string;
  x: number;
  y: number;
  w: number;
  h: number;
  minW?: number;
  minH?: number;
}

export interface SavedDashboard {
  id: string;
  name: string;
  ownerEmail: string;
  widgets: Widget[];
  layouts: WidgetLayout[];
  createdAt: string;
  updatedAt: string;
}
