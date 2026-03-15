"use client";

import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell } from "recharts";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { DataTable } from "@/components/dashboard/DataTable";
import { TrendChart } from "@/components/dashboard/TrendChart";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface EngagementData {
  signal_groups?: { tag_group: string; count: number }[];
  most_engaged?: Record<string, unknown>[];
  unengaged_high_value?: Record<string, unknown>[];
  communication_trend?: { date: string; value: number }[];
}

const BAR_COLORS = [
  "#0071e3", "#5856d6", "#34c759", "#ff9500", "#ff3b30",
  "#af52de", "#007aff", "#30b0c7", "#ff2d55", "#a2845e",
];

export default function EngagementDashboardPage() {
  const [data, setData] = useState<EngagementData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboards/engagement")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Loading engagement data..." loading />
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Unable to load engagement data" />
      </div>
    );
  }

  const signalGroups = (data.signal_groups ?? []).sort((a, b) => b.count - a.count);

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Engagement &amp; Signals
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Tag-based engagement analysis and communication activity
        </p>
      </div>

      {/* Signal Groups Bar Chart */}
      <SectionHeader title="Signal Groups" subtitle="Tag group counts across your database" />
      {signalGroups.length > 0 ? (
        <div className="card-base" style={{ padding: "20px 20px 12px" }}>
          <ResponsiveContainer width="100%" height={Math.max(260, signalGroups.length * 36)}>
            <BarChart data={signalGroups} layout="vertical" margin={{ top: 4, right: 20, bottom: 4, left: 120 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" horizontal={false} />
              <XAxis type="number" tick={{ fontSize: 11, fill: "var(--text-muted)" }} tickLine={false} axisLine={false} />
              <YAxis
                type="category"
                dataKey="tag_group"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                tickLine={false}
                axisLine={false}
                width={110}
              />
              <Tooltip
                contentStyle={{
                  background: "var(--surface)",
                  border: "1px solid var(--surface-border)",
                  borderRadius: 10,
                  fontSize: "0.78rem",
                  boxShadow: "var(--shadow-md)",
                }}
                formatter={(value) => [(value ?? 0).toLocaleString(), "Count"]}
              />
              <Bar dataKey="count" radius={[0, 4, 4, 0]} barSize={20}>
                {signalGroups.map((_, i) => (
                  <Cell key={i} fill={BAR_COLORS[i % BAR_COLORS.length]} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <EmptyState message="No signal group data available" />
      )}

      {/* Most Engaged People */}
      <SectionHeader title="Most Engaged People" subtitle="Top 30 by tag count" />
      <DataTable
        columns={[
          { key: "name", label: "Name" },
          { key: "tag_count", label: "Tags", align: "right", format: "number" },
          { key: "email", label: "Email" },
          { key: "total_given", label: "Total Given", align: "right", format: "currency" },
          { key: "lifecycle_stage", label: "Stage" },
        ]}
        data={data.most_engaged ?? []}
        maxRows={30}
      />

      {/* Unengaged High-Value Donors */}
      <SectionHeader title="Unengaged High-Value Donors" subtitle="High-value donors with low engagement — at risk" />
      <DataTable
        columns={[
          { key: "name", label: "Name" },
          { key: "total_given", label: "Total Given", align: "right", format: "currency" },
          { key: "tag_count", label: "Tags", align: "right", format: "number" },
        ]}
        data={data.unengaged_high_value ?? []}
      />

      {/* Communication Activity */}
      <SectionHeader title="Communication Activity" subtitle="Monthly communication count" />
      <TrendChart
        data={data.communication_trend ?? []}
        color="var(--accent-secondary)"
        valueFormatter={(v) => v.toLocaleString()}
      />
    </div>
  );
}
