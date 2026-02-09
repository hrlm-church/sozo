import {
  KpiCardData,
  NavItem,
  PaymentItem,
  SpecialistItem,
  TimeRangePreset,
} from "@/types/dashboard";

export const navItems: NavItem[] = [
  { id: "dashboard", label: "Dashboard" },
  { id: "sources", label: "Data Sources" },
  { id: "profiles", label: "Profiles" },
  { id: "insights", label: "Insights", badge: 4 },
  { id: "reports", label: "Reports" },
  { id: "governance", label: "Governance" },
];

export const kpiCards: KpiCardData[] = [
  { label: "Profiles linked", value: "83,257", delta: "+3.1%", trend: "up" },
  { label: "High-risk households", value: "1,634", delta: "-2.4%", trend: "up" },
  { label: "Migration exceptions", value: "315", delta: "-12%", trend: "down" },
];

export const specialists: SpecialistItem[] = [
  { id: "1", name: "Bergey Household", rating: 4.6, distanceMi: 1.2, role: "Recurring mismatch", slots: 2 },
  { id: "2", name: "Jones Family", rating: 4.9, distanceMi: 1.2, role: "Token unresolved", slots: 2 },
  { id: "3", name: "Laskin Household", rating: 4.9, distanceMi: 1.2, role: "High-value non-recurring", slots: 6 },
  { id: "4", name: "Schultz Family", rating: 4.9, distanceMi: 1.2, role: "Coverage gap", slots: 2 },
  { id: "5", name: "Heinz Household", rating: 4.5, distanceMi: 1.6, role: "At-risk retention", slots: 2 },
];

export const payments: PaymentItem[] = [
  { id: "p1", name: "Entity Reconciliation Job", role: "Identity graph", amount: 120, time: "18:40" },
  { id: "p2", name: "Recurring Risk Scan", role: "Signal engine", amount: 480, time: "18:40" },
  { id: "p3", name: "Token Mapping Audit", role: "Migration QA", amount: 300, time: "17:15" },
  { id: "p4", name: "Household Opportunity Pass", role: "Insight model", amount: 220, time: "16:30" },
];

export const datePresets: { id: TimeRangePreset; label: string }[] = [
  { id: "today", label: "Today" },
  { id: "yesterday", label: "Yesterday" },
  { id: "last_week", label: "Last week" },
  { id: "last_month", label: "Last month" },
  { id: "last_quarter", label: "Last quarter" },
];

export const performanceBars = [42, 21, 34, 38, 46, 14, 28];
