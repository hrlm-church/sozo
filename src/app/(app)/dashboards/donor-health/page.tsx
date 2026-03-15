"use client";

import { useEffect, useState } from "react";
import { StatCardEnhanced } from "@/components/dashboard/StatCardEnhanced";
import { FunnelChart } from "@/components/dashboard/FunnelChart";
import { TrendChart } from "@/components/dashboard/TrendChart";
import { DataTable } from "@/components/dashboard/DataTable";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface DonorHealthData {
  lifecycle_funnel: { lifecycle_stage: string; donor_count: number; total_given: number }[];
  at_risk_donors: { display_name: string; risk_level: string; last_gift_date: string; days_since_last: number; total_given: number; annual_revenue_at_risk: number }[];
  lost_recurring: { lost_count: number; monthly_value_lost: number };
  giving_trend: { month: string; total_amount: number }[];
  avg_gift_trend: { month: string; avg_gift: number }[];
}

const STAGE_ORDER = ["active", "cooling", "lapsed", "lost"];
const STAGE_COLORS: Record<string, string> = { active: "var(--green)", cooling: "var(--orange)", lapsed: "var(--red)", lost: "#6e6e73" };

export default function DonorHealthPage() {
  const [data, setData] = useState<DonorHealthData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboards/donor-health")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <EmptyState message="Loading donor health data..." loading />;
  if (!data) return <EmptyState message="Unable to load donor health data" />;

  const funnel = data.lifecycle_funnel ?? [];
  const sortedFunnel = STAGE_ORDER.map((s) => funnel.find((f) => f.lifecycle_stage === s)).filter(Boolean);
  const totalDonors = funnel.reduce((s, f) => s + f.donor_count, 0);
  const activeDonors = funnel.find((f) => f.lifecycle_stage === "active")?.donor_count ?? 0;

  const givingTrend = (data.giving_trend ?? []).map((g) => ({ date: g.month, value: g.total_amount }));
  const avgGiftTrend = (data.avg_gift_trend ?? []).map((g) => ({ date: g.month, value: g.avg_gift }));

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Donor Health
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Lifecycle stages, retention trends & at-risk analysis
        </p>
      </div>

      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12, marginBottom: 24 }}>
        <StatCardEnhanced label="Total Donors" value={totalDonors.toLocaleString()} icon={<span>&#128101;</span>} color="var(--accent)" />
        <StatCardEnhanced label="Active Donors" value={activeDonors.toLocaleString()} icon={<span>&#128994;</span>} color="var(--green)" />
        <StatCardEnhanced label="Retention Rate" value={totalDonors > 0 ? `${((activeDonors / totalDonors) * 100).toFixed(1)}%` : "0%"} icon={<span>&#128200;</span>} color="var(--accent-secondary)" />
        <StatCardEnhanced label="Lost MRR" value={`$${(data.lost_recurring?.monthly_value_lost ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}/mo`} icon={<span>&#128680;</span>} color="var(--red)" />
      </div>

      {/* Lifecycle Funnel */}
      <SectionHeader title="Donor Lifecycle Funnel" subtitle="Distribution across engagement stages" />
      <FunnelChart
        stages={sortedFunnel.map((f) => ({
          label: `${(f!.lifecycle_stage ?? "unknown").charAt(0).toUpperCase()}${(f!.lifecycle_stage ?? "").slice(1)} — $${f!.total_given.toLocaleString(undefined, { maximumFractionDigits: 0 })}`,
          value: f!.donor_count,
          color: STAGE_COLORS[f!.lifecycle_stage] ?? "var(--text-muted)",
        }))}
      />

      {/* Giving Trend */}
      <SectionHeader title="Monthly Giving Trend" subtitle="Total donations over time" />
      <TrendChart
        data={givingTrend}
        color="var(--green)"
        valueFormatter={(v) => `$${(v / 1000).toFixed(0)}K`}
      />

      {/* Average Gift Trend */}
      {avgGiftTrend.length > 0 && (
        <>
          <SectionHeader title="Average Gift Size" subtitle="Trend over time" />
          <TrendChart data={avgGiftTrend} color="var(--accent-secondary)" valueFormatter={(v) => `$${v.toFixed(0)}`} />
        </>
      )}

      {/* At-Risk Donors */}
      <SectionHeader title="At-Risk Donors" href="/intelligence" subtitle="Donors likely to lapse without intervention" />
      <DataTable
        columns={[
          { key: "display_name", label: "Donor" },
          { key: "risk_level", label: "Risk" },
          { key: "last_gift_date", label: "Last Gift", format: "date" },
          { key: "days_since_last", label: "Days Since", align: "right", format: "number" },
          { key: "total_given", label: "Total Given", align: "right", format: "currency" },
          { key: "annual_revenue_at_risk", label: "Revenue at Risk", align: "right", format: "currency" },
        ]}
        data={data.at_risk_donors ?? []}
        maxRows={15}
      />

      {/* Lost Recurring */}
      {data.lost_recurring && data.lost_recurring.lost_count > 0 && (
        <>
          <SectionHeader title="Lost Recurring Donors" />
          <div className="card-base" style={{ padding: "24px 28px", borderLeft: "4px solid var(--red)" }}>
            <div style={{ display: "flex", gap: 40 }}>
              <div>
                <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 4 }}>Lost Donors</div>
                <div style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--text-primary)" }}>{data.lost_recurring.lost_count}</div>
              </div>
              <div>
                <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 4 }}>Monthly Value Lost</div>
                <div style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--red)" }}>${data.lost_recurring.monthly_value_lost.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
              </div>
              <div>
                <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 4 }}>Annual Opportunity</div>
                <div style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--orange)" }}>${(data.lost_recurring.monthly_value_lost * 12).toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
}
