"use client";

import { useEffect, useState } from "react";
import { useSession } from "next-auth/react";
import Link from "next/link";
import { StatCardEnhanced } from "@/components/dashboard/StatCardEnhanced";
import { BriefingCard } from "@/components/dashboard/BriefingCard";
import { InsightPanel } from "@/components/dashboard/InsightPanel";
import { EmptyState } from "@/components/dashboard/EmptyState";
import { SectionHeader } from "@/components/dashboard/SectionHeader";

interface BriefingData {
  summary_kpis: { total_donors: number; total_given: number; mrr: number; at_risk_count: number; open_insights: number };
  top_alerts: { insight_id: string; severity: string; title: string; summary: string; insight_type: string }[];
  recent_metrics: { metric_key: string; display_name: string; value: number; delta_pct: number; format_hint: string; unit: string }[];
  at_risk_preview: { person_id: string; display_name: string; risk_level: string; annual_revenue_at_risk: number }[];
}

export default function BriefingPage() {
  const { data: session } = useSession();
  const [data, setData] = useState<BriefingData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboards/briefing")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  const greeting = () => {
    const hour = new Date().getHours();
    if (hour < 12) return "Good morning";
    if (hour < 17) return "Good afternoon";
    return "Good evening";
  };

  const today = new Date().toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" });

  if (loading) return <EmptyState message="Loading executive briefing..." loading />;

  const kpis = data?.summary_kpis;
  const alerts = data?.top_alerts ?? [];
  const trends = alerts.filter((a) => a.insight_type === "anomaly" || a.insight_type === "trend").map((a) => a.summary);
  const alertMsgs = alerts.filter((a) => a.severity === "critical" || a.severity === "high").map((a) => a.summary);
  const opportunities = alerts.filter((a) => a.insight_type === "opportunity").map((a) => a.summary);

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          {greeting()}{session?.user?.name ? `, ${session.user.name}` : ""}
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>{today}</p>
      </div>

      {/* Briefing Card */}
      <BriefingCard
        title="Executive Briefing"
        headline={alertMsgs.length > 0 ? alertMsgs[0] : "Your ministry data is up to date. Review the key numbers below."}
        strengths={trends.length > 0 ? trends.slice(0, 3) : ["Data pipeline running smoothly"]}
        risks={alertMsgs.length > 0 ? alertMsgs.slice(0, 3) : ["No critical risks detected"]}
        action="Review at-risk donors and explore engagement opportunities in the Intelligence hub."
        metrics={[
          { label: "Total Donors", value: (kpis?.total_donors ?? 0).toLocaleString() },
          { label: "Total Given", value: `$${(kpis?.total_given ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}` },
          { label: "At Risk", value: (kpis?.at_risk_count ?? 0).toLocaleString() },
        ]}
        generatedAt={today}
      />

      {/* KPIs */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12, marginTop: 20 }}>
        <StatCardEnhanced label="Total Donors" value={(kpis?.total_donors ?? 0).toLocaleString()} icon={<span>&#128101;</span>} color="var(--accent)" />
        <StatCardEnhanced label="Total Given" value={`$${(kpis?.total_given ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`} icon={<span>&#128176;</span>} color="var(--green)" />
        <StatCardEnhanced label="Est. MRR" value={`$${(kpis?.mrr ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`} icon={<span>&#128257;</span>} color="var(--accent-secondary)" />
        <StatCardEnhanced label="At-Risk Donors" value={(kpis?.at_risk_count ?? 0).toLocaleString()} icon={<span>&#9888;</span>} color="var(--orange)" />
        <StatCardEnhanced label="Open Insights" value={(kpis?.open_insights ?? 0).toLocaleString()} icon={<span>&#128161;</span>} color="var(--accent)" />
      </div>

      {/* Insight Panel */}
      <SectionHeader title="Intelligence Summary" />
      <InsightPanel trends={trends} alerts={alertMsgs} opportunities={opportunities} />

      {/* At-Risk Preview */}
      {(data?.at_risk_preview ?? []).length > 0 && (
        <>
          <SectionHeader title="Top At-Risk Donors" href="/dashboards/donor-health" />
          <div className="card-base" style={{ overflow: "hidden" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--surface-border)" }}>
                  <th style={{ textAlign: "left", padding: "10px 14px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.7rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Donor</th>
                  <th style={{ textAlign: "left", padding: "10px 14px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.7rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Risk</th>
                  <th style={{ textAlign: "right", padding: "10px 14px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.7rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Revenue at Risk</th>
                </tr>
              </thead>
              <tbody>
                {data!.at_risk_preview.map((r) => (
                  <tr key={r.person_id} style={{ borderBottom: "1px solid var(--surface-border)" }}>
                    <td style={{ padding: "10px 14px", fontWeight: 500, color: "var(--text-primary)" }}>{r.display_name}</td>
                    <td style={{ padding: "10px 14px" }}>
                      <span style={{ fontSize: "0.7rem", fontWeight: 600, padding: "2px 10px", borderRadius: 20, color: r.risk_level === "critical" ? "var(--red)" : "var(--orange)", background: r.risk_level === "critical" ? "rgba(255,59,48,0.1)" : "rgba(255,149,0,0.1)", textTransform: "capitalize" }}>
                        {r.risk_level}
                      </span>
                    </td>
                    <td style={{ padding: "10px 14px", textAlign: "right", fontWeight: 600, color: "var(--red)" }}>
                      ${(r.annual_revenue_at_risk ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* Quick Actions */}
      <SectionHeader title="Explore More" />
      <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
        <Link href="/dashboards/donor-health" style={{ textDecoration: "none" }}>
          <div className="card-base" style={{ padding: "16px 20px", borderTop: "3px solid var(--green)", cursor: "pointer" }}>
            <div style={{ fontSize: "0.84rem", fontWeight: 600, color: "var(--text-primary)" }}>Donor Health</div>
            <div style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>Lifecycle & retention</div>
          </div>
        </Link>
        <Link href="/dashboards/revenue" style={{ textDecoration: "none" }}>
          <div className="card-base" style={{ padding: "16px 20px", borderTop: "3px solid var(--accent-secondary)", cursor: "pointer" }}>
            <div style={{ fontSize: "0.84rem", fontWeight: 600, color: "var(--text-primary)" }}>Revenue</div>
            <div style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>Streams & trends</div>
          </div>
        </Link>
        <Link href="/chat" style={{ textDecoration: "none" }}>
          <div className="card-base" style={{ padding: "16px 20px", borderTop: "3px solid var(--accent)", cursor: "pointer" }}>
            <div style={{ fontSize: "0.84rem", fontWeight: 600, color: "var(--text-primary)" }}>Ask Sozo</div>
            <div style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>Chat with your data</div>
          </div>
        </Link>
      </div>
    </div>
  );
}
