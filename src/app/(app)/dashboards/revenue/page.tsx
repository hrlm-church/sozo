"use client";

import { useEffect, useState } from "react";
import { ResponsiveContainer, AreaChart, Area, XAxis, YAxis, Tooltip, CartesianGrid, BarChart, Bar, Legend } from "recharts";
import { StatCardEnhanced } from "@/components/dashboard/StatCardEnhanced";
import { DonutChart } from "@/components/dashboard/DonutChart";
import { TrendChart } from "@/components/dashboard/TrendChart";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface RevenueData {
  revenue_by_stream: { month: string; donations: number; commerce: number; subscriptions: number; events: number }[];
  top_funds: { fund: string; total_raised: number; donor_count: number }[];
  payment_methods: { payment_method: string; total_amount: number; count: number }[];
  yoy_comparison: { month_num: number; month_label: string; this_year: number; last_year: number }[];
  summary_kpis: { total_revenue: number; total_donors: number; avg_gift: number; total_transactions: number };
}

const STREAM_COLORS: Record<string, string> = { donations: "#34c759", commerce: "#0071e3", subscriptions: "#5856d6", events: "#ff9500" };
const METHOD_COLORS = ["#0071e3", "#34c759", "#5856d6", "#ff9500", "#ff3b30", "#86868b"];

export default function RevenuePage() {
  const [data, setData] = useState<RevenueData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboards/revenue")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <EmptyState message="Loading revenue data..." loading />;
  if (!data) return <EmptyState message="Unable to load revenue data" />;

  const kpis = data.summary_kpis;
  const yoyTrend = (data.yoy_comparison ?? []).map((y) => ({ date: y.month_label, value: y.this_year, comparison: y.last_year }));
  const paymentDonut = (data.payment_methods ?? []).map((p, i) => ({ name: p.payment_method || "Unknown", value: p.total_amount, color: METHOD_COLORS[i % METHOD_COLORS.length] }));

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>Revenue Intelligence</h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>Revenue streams, fund performance & trends</p>
      </div>

      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12, marginBottom: 24 }}>
        <StatCardEnhanced label="Total Revenue" value={`$${(kpis?.total_revenue ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`} icon={<span>&#128176;</span>} color="var(--green)" />
        <StatCardEnhanced label="Total Donors" value={(kpis?.total_donors ?? 0).toLocaleString()} icon={<span>&#128101;</span>} color="var(--accent)" />
        <StatCardEnhanced label="Avg Gift" value={`$${(kpis?.avg_gift ?? 0).toFixed(0)}`} icon={<span>&#127873;</span>} color="var(--accent-secondary)" />
        <StatCardEnhanced label="Transactions" value={(kpis?.total_transactions ?? 0).toLocaleString()} icon={<span>&#128221;</span>} color="var(--orange)" />
      </div>

      {/* Revenue by Stream */}
      {(data.revenue_by_stream ?? []).length > 0 && (
        <>
          <SectionHeader title="Revenue by Stream" subtitle="Monthly breakdown by revenue type" />
          <div className="card-base" style={{ padding: "20px 20px 12px" }}>
            <ResponsiveContainer width="100%" height={280}>
              <AreaChart data={data.revenue_by_stream} margin={{ top: 4, right: 8, bottom: 0, left: 8 }}>
                <defs>
                  {Object.entries(STREAM_COLORS).map(([key, color]) => (
                    <linearGradient key={key} id={`grad-${key}`} x1="0" y1="0" x2="0" y2="1">
                      <stop offset="0%" stopColor={color} stopOpacity={0.2} />
                      <stop offset="100%" stopColor={color} stopOpacity={0} />
                    </linearGradient>
                  ))}
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" />
                <XAxis dataKey="month" tick={{ fontSize: 11, fill: "var(--text-muted)" }} tickLine={false} axisLine={false} />
                <YAxis tick={{ fontSize: 11, fill: "var(--text-muted)" }} tickLine={false} axisLine={false} tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`} />
                <Tooltip contentStyle={{ background: "var(--surface)", border: "1px solid var(--surface-border)", borderRadius: 10, fontSize: "0.78rem", boxShadow: "var(--shadow-md)" }} formatter={(value) => [`$${(value ?? 0).toLocaleString()}`, ""]} />
                <Legend iconType="circle" iconSize={8} wrapperStyle={{ fontSize: "0.72rem" }} />
                {Object.entries(STREAM_COLORS).map(([key, color]) => (
                  <Area key={key} type="monotone" dataKey={key} stackId="1" stroke={color} fill={`url(#grad-${key})`} strokeWidth={1.5} />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </>
      )}

      {/* Top Funds */}
      {(data.top_funds ?? []).length > 0 && (
        <>
          <SectionHeader title="Top Funds" subtitle="By total raised" />
          <div className="card-base" style={{ padding: "20px 20px 12px" }}>
            <ResponsiveContainer width="100%" height={Math.max(200, data.top_funds.length * 36)}>
              <BarChart data={data.top_funds} layout="vertical" margin={{ top: 4, right: 20, bottom: 0, left: 120 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" horizontal={false} />
                <XAxis type="number" tick={{ fontSize: 11, fill: "var(--text-muted)" }} tickFormatter={(v) => `$${(v / 1000).toFixed(0)}K`} />
                <YAxis dataKey="fund" type="category" tick={{ fontSize: 11, fill: "var(--text-secondary)" }} width={110} />
                <Tooltip contentStyle={{ background: "var(--surface)", border: "1px solid var(--surface-border)", borderRadius: 10, fontSize: "0.78rem" }} formatter={(value) => [`$${(value ?? 0).toLocaleString()}`, "Raised"]} />
                <Bar dataKey="total_raised" fill="var(--accent)" radius={[0, 4, 4, 0]} />
              </BarChart>
            </ResponsiveContainer>
          </div>
        </>
      )}

      {/* Payment Methods & YoY */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginTop: 28 }}>
        <div>
          <SectionHeader title="Payment Methods" />
          <div className="card-base" style={{ padding: 20 }}>
            <DonutChart data={paymentDonut} />
          </div>
        </div>
        <div>
          <SectionHeader title="Year-over-Year" subtitle="This year vs last year" />
          <TrendChart data={yoyTrend} color="var(--green)" valueFormatter={(v) => `$${(v / 1000).toFixed(0)}K`} />
        </div>
      </div>
    </div>
  );
}
