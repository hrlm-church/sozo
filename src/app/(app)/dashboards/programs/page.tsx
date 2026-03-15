"use client";

import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Cell } from "recharts";
import { HealthRing } from "@/components/dashboard/HealthRing";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface Program {
  program_name: string;
  people_count: number;
  donor_count: number;
  total_revenue: number;
}

interface ProgramsData {
  programs?: Program[];
}

const PROGRAM_COLORS: Record<string, string> = {
  "True Girl": "#ff2d55",
  "B2BB": "#5856d6",
  "Dannah Gresh": "#0071e3",
  "Pure Freedom": "#34c759",
};

function computeHealthScore(p: Program): number {
  if (!p.people_count || !p.donor_count) return 0;
  const donorRatio = Math.min(p.donor_count / p.people_count, 1);
  const revPerDonor = p.donor_count > 0 ? p.total_revenue / p.donor_count : 0;
  const revScore = Math.min(revPerDonor / 500, 1);
  return Math.round((donorRatio * 50 + revScore * 50));
}

export default function ProgramsDashboardPage() {
  const [data, setData] = useState<ProgramsData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboards/programs")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Loading programs data..." loading />
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Unable to load programs data" />
      </div>
    );
  }

  const programs = data.programs ?? [];

  const revenueChartData = programs.map((p) => ({
    name: p.program_name,
    revenue: p.total_revenue,
    fill: PROGRAM_COLORS[p.program_name] ?? "var(--accent)",
  }));

  const peopleChartData = programs.map((p) => ({
    name: p.program_name,
    people: p.people_count,
    fill: PROGRAM_COLORS[p.program_name] ?? "var(--accent)",
  }));

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Program Comparison
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Side-by-side analysis of ministry programs
        </p>
      </div>

      {/* Program Cards Grid */}
      {programs.length > 0 ? (
        <div style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(programs.length, 4)}, 1fr)`, gap: 14, marginBottom: 28 }}>
          {programs.map((p) => {
            const health = computeHealthScore(p);
            const color = PROGRAM_COLORS[p.program_name] ?? "var(--accent)";
            return (
              <div key={p.program_name} className="card-base" style={{ padding: "20px 22px", borderTop: `3px solid ${color}` }}>
                <div style={{ fontSize: "0.9rem", fontWeight: 700, color: "var(--text-primary)", marginBottom: 14 }}>
                  {p.program_name}
                </div>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 14 }}>
                  <HealthRing score={health} size="sm" />
                  <div style={{ textAlign: "right" }}>
                    <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em" }}>
                      Health
                    </div>
                    <div style={{ fontSize: "0.84rem", fontWeight: 600, color: "var(--text-primary)" }}>
                      {health}/100
                    </div>
                  </div>
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                  <MetricRow label="People" value={p.people_count.toLocaleString()} />
                  <MetricRow label="Donors" value={p.donor_count.toLocaleString()} />
                  <MetricRow label="Revenue" value={`$${p.total_revenue.toLocaleString(undefined, { maximumFractionDigits: 0 })}`} />
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <EmptyState message="No program data available" />
      )}

      {/* Revenue by Program */}
      <SectionHeader title="Revenue by Program" />
      {revenueChartData.length > 0 ? (
        <div className="card-base" style={{ padding: "20px 20px 12px" }}>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={revenueChartData} margin={{ top: 8, right: 20, bottom: 4, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis
                tick={{ fontSize: 11, fill: "var(--text-muted)" }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(v: number) => `$${(v / 1000).toFixed(0)}K`}
              />
              <Tooltip
                contentStyle={{
                  background: "var(--surface)",
                  border: "1px solid var(--surface-border)",
                  borderRadius: 10,
                  fontSize: "0.78rem",
                  boxShadow: "var(--shadow-md)",
                }}
                formatter={(value) => [`$${(value ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`, "Revenue"]}
              />
              <Bar dataKey="revenue" radius={[6, 6, 0, 0]} barSize={48}>
                {revenueChartData.map((entry, i) => (
                  <Cell key={i} fill={entry.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : null}

      {/* People by Program */}
      <SectionHeader title="People by Program" />
      {peopleChartData.length > 0 ? (
        <div className="card-base" style={{ padding: "20px 20px 12px" }}>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={peopleChartData} margin={{ top: 8, right: 20, bottom: 4, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" />
              <XAxis
                dataKey="name"
                tick={{ fontSize: 12, fill: "var(--text-secondary)" }}
                tickLine={false}
                axisLine={false}
              />
              <YAxis
                tick={{ fontSize: 11, fill: "var(--text-muted)" }}
                tickLine={false}
                axisLine={false}
                tickFormatter={(v: number) => v.toLocaleString()}
              />
              <Tooltip
                contentStyle={{
                  background: "var(--surface)",
                  border: "1px solid var(--surface-border)",
                  borderRadius: 10,
                  fontSize: "0.78rem",
                  boxShadow: "var(--shadow-md)",
                }}
                formatter={(value) => [(value ?? 0).toLocaleString(), "People"]}
              />
              <Bar dataKey="people" radius={[6, 6, 0, 0]} barSize={48}>
                {peopleChartData.map((entry, i) => (
                  <Cell key={i} fill={entry.fill} />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : null}
    </div>
  );
}

function MetricRow({ label, value }: { label: string; value: string }) {
  return (
    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "baseline" }}>
      <span style={{ fontSize: "0.76rem", color: "var(--text-muted)" }}>{label}</span>
      <span style={{ fontSize: "0.84rem", fontWeight: 600, color: "var(--text-primary)" }}>{value}</span>
    </div>
  );
}

