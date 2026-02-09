export type DashboardMetricTab = "revenue" | "patients" | "specialists";

export type TimeRangePreset =
  | "today"
  | "yesterday"
  | "last_week"
  | "last_month"
  | "last_quarter";

export type KpiTrend = "up" | "down" | "neutral";

export interface KpiCardData {
  label: string;
  value: string;
  delta?: string;
  trend?: KpiTrend;
}

export interface SpecialistItem {
  id: string;
  name: string;
  rating: number;
  distanceMi: number;
  role: string;
  slots: number;
  avatar?: string;
}

export interface PaymentItem {
  id: string;
  name: string;
  role: string;
  amount: number;
  time: string;
  avatar?: string;
}

export interface NavItem {
  id: string;
  label: string;
  badge?: number;
}
