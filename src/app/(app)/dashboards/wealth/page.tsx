"use client";

import { useEffect, useState } from "react";
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid, Legend } from "recharts";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { DonutChart } from "@/components/dashboard/DonutChart";
import { DataTable } from "@/components/dashboard/DataTable";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface WealthData {
  capacity_tiers?: { name: string; value: number; color: string }[];
  screening_stats?: { screened: number; total_donors: number };
  giving_gap?: { tier: string; estimated_capacity: number; actual_giving: number }[];
  upgrade_candidates?: Record<string, unknown>[];
}

export default function WealthDashboardPage() {
  const [data, setData] = useState<WealthData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboards/wealth")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Loading wealth data..." loading />
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Unable to load wealth data" />
      </div>
    );
  }

  const ss = data.screening_stats ?? { screened: 0, total_donors: 0 };
  const screenedPct = ss.total_donors > 0 ? ((ss.screened / ss.total_donors) * 100).toFixed(1) : "0";

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Wealth &amp; Capacity Analysis
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Giving capacity and upgrade opportunity analysis
        </p>
      </div>

      {/* Capacity Tiers + Screened Stats */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 28 }}>
        <div>
          <SectionHeader title="Capacity Tiers" />
          <div className="card-base" style={{ padding: 24 }}>
            <DonutChart data={data.capacity_tiers ?? []} />
          </div>
        </div>
        <div>
          <SectionHeader title="Screening Coverage" />
          <div className="card-base" style={{ padding: "32px 28px" }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 16 }}>
              <span style={{ fontSize: "2rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.02em" }}>
                {ss.screened.toLocaleString()}
              </span>
              <span style={{ fontSize: "0.88rem", color: "var(--text-muted)" }}>
                screened
              </span>
            </div>
            <div style={{ display: "flex", alignItems: "baseline", gap: 8, marginBottom: 16 }}>
              <span style={{ fontSize: "1.1rem", fontWeight: 600, color: "var(--text-secondary)" }}>
                of {ss.total_donors.toLocaleString()} total donors
              </span>
            </div>
            <div style={{ height: 8, borderRadius: 4, background: "var(--surface-border)", overflow: "hidden", marginBottom: 8 }}>
              <div style={{
                width: `${screenedPct}%`,
                height: "100%",
                borderRadius: 4,
                background: "var(--accent)",
                transition: "width 400ms ease",
              }} />
            </div>
            <div style={{ fontSize: "0.78rem", color: "var(--text-muted)" }}>
              {screenedPct}% coverage
            </div>
          </div>
        </div>
      </div>

      {/* Giving Gap Analysis */}
      <SectionHeader title="Giving Gap Analysis" subtitle="Estimated capacity vs actual giving per tier" />
      {(data.giving_gap ?? []).length > 0 ? (
        <div className="card-base" style={{ padding: "20px 20px 12px" }}>
          <ResponsiveContainer width="100%" height={320}>
            <BarChart data={data.giving_gap} margin={{ top: 8, right: 20, bottom: 4, left: 20 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="var(--surface-border)" />
              <XAxis
                dataKey="tier"
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
                formatter={(value, name) => [
                  `$${(value ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
                  name === "estimated_capacity" ? "Estimated Capacity" : "Actual Giving",
                ]}
              />
              <Legend
                formatter={(value: string) => value === "estimated_capacity" ? "Estimated Capacity" : "Actual Giving"}
                wrapperStyle={{ fontSize: "0.78rem" }}
              />
              <Bar dataKey="estimated_capacity" fill="var(--accent)" radius={[4, 4, 0, 0]} barSize={28} />
              <Bar dataKey="actual_giving" fill="var(--green)" radius={[4, 4, 0, 0]} barSize={28} />
            </BarChart>
          </ResponsiveContainer>
        </div>
      ) : (
        <EmptyState message="No giving gap data available" />
      )}

      {/* Top Upgrade Candidates */}
      <SectionHeader title="Top Upgrade Candidates" subtitle="Donors with the largest gap between capacity and current giving" />
      <DataTable
        columns={[
          { key: "name", label: "Name" },
          { key: "giving_capacity_label", label: "Capacity Label" },
          { key: "estimated_annual_capacity", label: "Est. Annual Capacity", align: "right", format: "currency" },
          { key: "current_annual_giving", label: "Current Annual Giving", align: "right", format: "currency" },
          { key: "gap_amount", label: "Gap", align: "right", format: "currency" },
        ]}
        data={data.upgrade_candidates ?? []}
        maxRows={20}
      />

      {/* Footnote */}
      <div style={{
        marginTop: 20,
        padding: "12px 16px",
        background: "rgba(0, 113, 227, 0.04)",
        borderRadius: 10,
        fontSize: "0.78rem",
        color: "var(--text-muted)",
        lineHeight: 1.5,
      }}>
        <strong style={{ color: "var(--text-secondary)" }}>Note:</strong> Giving capacity is ANNUAL — compared against annualized giving.
      </div>
    </div>
  );
}
