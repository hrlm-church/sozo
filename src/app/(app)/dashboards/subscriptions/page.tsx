"use client";

import { useEffect, useState } from "react";
import { StatCardEnhanced } from "@/components/dashboard/StatCardEnhanced";
import { DonutChart } from "@/components/dashboard/DonutChart";
import { DataTable } from "@/components/dashboard/DataTable";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface SubData {
  summary: { active_count: number; inactive_count: number; estimated_mrr: number };
  lost_subscribers: { display_name: string; monthly_value: number; last_active_date: string }[];
  status_breakdown: { subscription_status: string; count: number }[];
}

export default function SubscriptionsPage() {
  const [data, setData] = useState<SubData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboards/subscriptions")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <EmptyState message="Loading subscription data..." loading />;
  if (!data) return <EmptyState message="Unable to load subscription data" />;

  const s = data.summary ?? { active_count: 0, inactive_count: 0, estimated_mrr: 0 };
  const lostMonthly = (data.lost_subscribers ?? []).reduce((sum, l) => sum + (l.monthly_value ?? 0), 0);

  const statusDonut = (data.status_breakdown ?? []).map((st, i) => ({
    name: st.subscription_status || "Unknown",
    value: st.count,
    color: ["var(--green)", "var(--red)", "var(--orange)", "var(--text-muted)"][i % 4],
  }));

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>Subscriptions & Recurring Revenue</h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>Monthly recurring revenue health</p>
      </div>

      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12, marginBottom: 24 }}>
        <StatCardEnhanced label="Estimated MRR" value={`$${s.estimated_mrr.toLocaleString(undefined, { maximumFractionDigits: 0 })}`} icon={<span>&#128257;</span>} color="var(--green)" />
        <StatCardEnhanced label="Active Subscribers" value={s.active_count.toLocaleString()} icon={<span>&#128994;</span>} color="var(--accent)" />
        <StatCardEnhanced label="Inactive Subscribers" value={s.inactive_count.toLocaleString()} icon={<span>&#128308;</span>} color="var(--red)" />
      </div>

      {/* Two-column */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16 }}>
        <div>
          <SectionHeader title="Status Breakdown" />
          <div className="card-base" style={{ padding: 20 }}>
            <DonutChart data={statusDonut} />
          </div>
        </div>
        <div>
          <SectionHeader title="Lost Revenue Summary" />
          <div className="card-base" style={{ padding: "24px 28px", borderLeft: "4px solid var(--red)" }}>
            <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
              <div>
                <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 4 }}>Lost Subscribers</div>
                <div style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--text-primary)" }}>{(data.lost_subscribers ?? []).length}</div>
              </div>
              <div>
                <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 4 }}>Monthly Value Lost</div>
                <div style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--red)" }}>${lostMonthly.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
              </div>
              <div>
                <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 4 }}>Annual Opportunity</div>
                <div style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--orange)" }}>${(lostMonthly * 12).toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* Lost Subscribers Table */}
      {(data.lost_subscribers ?? []).length > 0 && (
        <>
          <SectionHeader title="Lost Subscribers" subtitle="Recently churned recurring donors" />
          <DataTable
            columns={[
              { key: "display_name", label: "Name" },
              { key: "monthly_value", label: "Monthly Value", align: "right", format: "currency" },
              { key: "last_active_date", label: "Last Active", format: "date" },
            ]}
            data={data.lost_subscribers}
            maxRows={15}
          />
        </>
      )}
    </div>
  );
}
