"use client";

import { useEffect, useState } from "react";
import { StatCardEnhanced } from "@/components/dashboard/StatCardEnhanced";
import { HealthRing } from "@/components/dashboard/HealthRing";
import { BriefingCard } from "@/components/dashboard/BriefingCard";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface BriefingData {
  headline?: string;
  strengths?: string[];
  risks?: string[];
  action?: string;
  metrics?: { label: string; value: string }[];
  generated_at?: string;
}

interface RevenueData {
  total_revenue?: number;
  delta_pct?: number;
  recurring_revenue?: number;
  recurring_delta?: number;
  avg_gift?: number;
  avg_gift_delta?: number;
}

interface DonorHealthData {
  retention_rate?: number;
  retention_delta?: number;
  active_donors?: number;
  active_delta?: number;
  new_donors?: number;
  churn_rate?: number;
  engagement_score?: number;
  subscription_health?: number;
  events_score?: number;
}

interface ReportState {
  briefing: BriefingData | null;
  revenue: RevenueData | null;
  donorHealth: DonorHealthData | null;
}

function computeMinistryScore(r: RevenueData | null, d: DonorHealthData | null): number {
  const givingTrend = Math.max(0, Math.min(100, 50 + (r?.delta_pct ?? 0)));
  const retention = d?.retention_rate ?? 50;
  const engagement = d?.engagement_score ?? 50;
  const subHealth = d?.subscription_health ?? 50;
  const events = d?.events_score ?? 50;
  return Math.round(
    givingTrend * 0.30 +
    retention * 0.25 +
    engagement * 0.20 +
    subHealth * 0.15 +
    events * 0.10
  );
}

function scoreToGrade(score: number): string {
  if (score >= 90) return "A";
  if (score >= 80) return "B";
  if (score >= 70) return "C";
  if (score >= 60) return "D";
  return "F";
}

function scoreToTrajectory(score: number, delta: number): string {
  if (delta > 5) return "Trending up";
  if (delta < -5) return "Trending down";
  if (score >= 80) return "Strong and steady";
  if (score >= 60) return "Stable with room to grow";
  return "Needs attention";
}

export default function MonthlyReportPage() {
  const [state, setState] = useState<ReportState>({ briefing: null, revenue: null, donorHealth: null });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      fetch("/api/dashboards/briefing").then((r) => r.json()).catch(() => null),
      fetch("/api/dashboards/revenue").then((r) => r.json()).catch(() => null),
      fetch("/api/dashboards/donor-health").then((r) => r.json()).catch(() => null),
    ])
      .then(([briefing, revenue, donorHealth]) => {
        setState({ briefing, revenue, donorHealth });
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Loading monthly report..." loading />
      </div>
    );
  }

  const { briefing, revenue, donorHealth } = state;
  const score = computeMinistryScore(revenue, donorHealth);
  const grade = scoreToGrade(score);
  const trajectory = scoreToTrajectory(score, revenue?.delta_pct ?? 0);

  const now = new Date();
  const monthYear = now.toLocaleDateString("en-US", { month: "long", year: "numeric" });

  // Build strengths/risks from briefing or defaults
  const strengths = briefing?.strengths ?? [];
  const risks = briefing?.risks ?? [];

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 28 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Monthly Ministry Report
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          {monthYear}
        </p>
      </div>

      {/* Ministry Health Score */}
      <div className="card-base" style={{ padding: "32px 40px", marginBottom: 28, display: "flex", alignItems: "center", gap: 40 }}>
        <HealthRing score={score} size="lg" />
        <div>
          <div style={{ display: "flex", alignItems: "baseline", gap: 12, marginBottom: 6 }}>
            <span style={{ fontSize: "2.4rem", fontWeight: 800, color: "var(--text-primary)", letterSpacing: "-0.03em" }}>
              {grade}
            </span>
            <span style={{ fontSize: "1rem", color: "var(--text-muted)" }}>
              {score}/100
            </span>
          </div>
          <div style={{ fontSize: "0.88rem", color: "var(--text-secondary)", marginBottom: 4 }}>
            Ministry Health Score
          </div>
          <div style={{
            fontSize: "0.78rem",
            color: score >= 70 ? "var(--green)" : score >= 50 ? "var(--orange)" : "var(--red)",
            fontWeight: 600,
          }}>
            {trajectory}
          </div>
          <div style={{ fontSize: "0.7rem", color: "var(--text-muted)", marginTop: 8 }}>
            Giving trend 30% + Retention 25% + Engagement 20% + Subscriptions 15% + Events 10%
          </div>
        </div>
      </div>

      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(5, 1fr)", gap: 12, marginBottom: 28 }}>
        <StatCardEnhanced
          label="Total Revenue"
          value={`$${(revenue?.total_revenue ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          delta={revenue?.delta_pct}
        />
        <StatCardEnhanced
          label="Recurring Revenue"
          value={`$${(revenue?.recurring_revenue ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          delta={revenue?.recurring_delta}
        />
        <StatCardEnhanced
          label="Avg Gift"
          value={`$${(revenue?.avg_gift ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
          delta={revenue?.avg_gift_delta}
        />
        <StatCardEnhanced
          label="Active Donors"
          value={(donorHealth?.active_donors ?? 0).toLocaleString()}
          delta={donorHealth?.active_delta}
        />
        <StatCardEnhanced
          label="Retention Rate"
          value={`${(donorHealth?.retention_rate ?? 0).toFixed(1)}%`}
          delta={donorHealth?.retention_delta}
        />
      </div>

      {/* Briefing Card */}
      {briefing && (
        <div style={{ marginBottom: 28 }}>
          <BriefingCard
            title="AI Briefing"
            headline={briefing.headline ?? "Monthly ministry performance summary"}
            strengths={strengths}
            risks={risks}
            action={briefing.action ?? "Review the detailed dashboards for more information."}
            metrics={briefing.metrics ?? []}
            generatedAt={briefing.generated_at}
          />
        </div>
      )}

      {/* Key Wins */}
      {strengths.length > 0 && (
        <>
          <SectionHeader title="Key Wins" />
          <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 28 }}>
            {strengths.map((s, i) => (
              <div key={i} className="card-base" style={{
                padding: "14px 20px",
                borderLeft: "4px solid var(--green)",
                fontSize: "0.84rem",
                color: "var(--text-secondary)",
                lineHeight: 1.6,
              }}>
                {s}
              </div>
            ))}
          </div>
        </>
      )}

      {/* Key Risks */}
      {risks.length > 0 && (
        <>
          <SectionHeader title="Key Risks" />
          <div style={{ display: "flex", flexDirection: "column", gap: 10, marginBottom: 28 }}>
            {risks.map((r, i) => (
              <div key={i} className="card-base" style={{
                padding: "14px 20px",
                borderLeft: "4px solid var(--red)",
                fontSize: "0.84rem",
                color: "var(--text-secondary)",
                lineHeight: 1.6,
              }}>
                {r}
              </div>
            ))}
          </div>
        </>
      )}

      {/* Recommended Actions */}
      {briefing?.action && (
        <>
          <SectionHeader title="Recommended Actions" />
          <div className="card-base" style={{
            padding: "20px 24px",
            borderLeft: "4px solid var(--accent)",
          }}>
            <ol style={{ margin: 0, paddingLeft: 20, fontSize: "0.84rem", color: "var(--text-secondary)", lineHeight: 1.8 }}>
              <li>{briefing.action}</li>
              {strengths.length > 0 && <li>Double down on strengths: leverage top-performing areas</li>}
              {risks.length > 0 && <li>Address identified risks with targeted outreach campaigns</li>}
              <li>Schedule leadership review to discuss findings</li>
            </ol>
          </div>
        </>
      )}
    </div>
  );
}
