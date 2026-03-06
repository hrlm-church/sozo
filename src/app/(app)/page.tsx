"use client";

import { useEffect, useState } from "react";
import { useSession } from "next-auth/react";
import Link from "next/link";

interface Metric {
  metric_key: string;
  display_name: string;
  unit: string;
  format_hint: string;
  value: number;
  prior_value: number;
  delta_pct: number;
}

interface Insight {
  insight_id: string;
  insight_type: string;
  severity: string;
  title: string;
  summary: string;
  metric_key: string;
  current_value: number;
  baseline_value: number;
  delta_pct: number;
  created_at: string;
}

interface RiskDonor {
  person_id: string;
  display_name: string;
  risk_score: number;
  risk_level: string;
  annual_revenue_at_risk: number;
}

interface IntelData {
  metrics?: Metric[];
  insights?: Insight[];
  risks?: RiskDonor[];
  summary?: { open_insights?: number; high_risk_donors?: number; metrics_computed?: number; last_snapshot_date?: string };
}

function formatMetricValue(value: number, hint: string, unit: string): string {
  if (hint === "currency" || unit === "USD") return `$${value?.toLocaleString(undefined, { maximumFractionDigits: 0 }) ?? "0"}`;
  if (hint === "percentage" || unit === "%") return `${(value ?? 0).toFixed(1)}%`;
  if (hint === "integer") return (value ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
  return (value ?? 0).toLocaleString(undefined, { maximumFractionDigits: 1 });
}

export default function DashboardHome() {
  const { data: session } = useSession();
  const [intel, setIntel] = useState<IntelData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/intel")
      .then((r) => r.json())
      .then(setIntel)
      .catch(() => setIntel(null))
      .finally(() => setLoading(false));
  }, []);

  const greeting = () => {
    const hour = new Date().getHours();
    if (hour < 12) return "Good morning";
    if (hour < 17) return "Good afternoon";
    return "Good evening";
  };

  // Filter out zero/empty metrics — only show ones with real data
  const meaningfulMetrics = (intel?.metrics ?? []).filter(
    (m) => m.value !== null && m.value !== undefined && m.value !== 0,
  );

  // Total revenue at risk across all high-risk donors
  const totalRevenueAtRisk = (intel?.risks ?? []).reduce(
    (sum, r) => sum + (r.annual_revenue_at_risk ?? 0), 0,
  );

  const snapshotDate = intel?.summary?.last_snapshot_date
    ? new Date(intel.summary.last_snapshot_date).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })
    : null;

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1100 }}>
      {/* Header */}
      <div style={{ marginBottom: 28 }}>
        <h1 style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          {greeting()}{session?.user?.name ? `, ${session.user.name}` : ""}
        </h1>
        {snapshotDate && (
          <p style={{ fontSize: "0.78rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
            Last updated {snapshotDate}
          </p>
        )}
      </div>

      {loading ? (
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--text-muted)", fontSize: "0.84rem", padding: "60px 0" }}>
          <span className="loading-pulse" style={{ width: 8, height: 8, borderRadius: "50%", background: "var(--accent)" }} />
          Loading intelligence briefing...
        </div>
      ) : (
        <>
          {/* ── Briefing Card (hero) ── */}
          {(intel?.insights ?? []).length > 0 && (
            <div
              className="card-base"
              style={{
                padding: "28px 32px",
                marginBottom: 24,
                borderLeft: "4px solid var(--accent)",
              }}
            >
              <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 16 }}>
                <span style={{ fontSize: "1.1rem" }}>&#10024;</span>
                <span style={{ fontSize: "0.88rem", fontWeight: 700, color: "var(--text-primary)" }}>
                  Weekly Briefing
                </span>
                <span style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginLeft: "auto" }}>
                  {snapshotDate}
                </span>
              </div>

              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
                {/* Attention needed */}
                <div>
                  <div style={{ fontSize: "0.72rem", fontWeight: 700, color: "var(--orange)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 10 }}>
                    Attention Needed
                  </div>
                  {intel!.insights!.slice(0, 3).map((insight) => (
                    <div key={insight.insight_id} style={{ display: "flex", alignItems: "flex-start", gap: 8, marginBottom: 10 }}>
                      <span style={{
                        color: insight.severity === "critical" ? "var(--red)" : "var(--orange)",
                        fontSize: "0.78rem",
                        lineHeight: 1.5,
                        flexShrink: 0,
                      }}>
                        {insight.severity === "critical" ? "\u2716" : "\u26A0"}
                      </span>
                      <span style={{ fontSize: "0.82rem", color: "var(--text-secondary)", lineHeight: 1.5 }}>
                        {insight.summary}
                      </span>
                    </div>
                  ))}
                </div>

                {/* Key numbers */}
                <div>
                  <div style={{ fontSize: "0.72rem", fontWeight: 700, color: "var(--accent)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 10 }}>
                    Key Numbers
                  </div>
                  <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                    <KeyNumber
                      label="Donors at risk"
                      value={(intel?.summary?.high_risk_donors ?? 0).toLocaleString()}
                      color="var(--orange)"
                    />
                    <KeyNumber
                      label="Annual revenue at risk"
                      value={`$${totalRevenueAtRisk.toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
                      color="var(--red)"
                    />
                    <KeyNumber
                      label="Open insights"
                      value={String(intel?.summary?.open_insights ?? 0)}
                      color="var(--accent)"
                    />
                  </div>
                </div>
              </div>

              {/* Recommendation */}
              {intel!.insights!.length > 0 && (
                <div style={{
                  marginTop: 18,
                  padding: "12px 16px",
                  background: "rgba(0, 113, 227, 0.04)",
                  borderRadius: 10,
                  fontSize: "0.82rem",
                  color: "var(--text-secondary)",
                  lineHeight: 1.5,
                }}>
                  <strong style={{ color: "var(--accent)" }}>Recommended:</strong>{" "}
                  <Link href="/chat" style={{ color: "var(--accent)", textDecoration: "none", fontWeight: 500 }}>
                    Ask Sozo
                  </Link>{" "}
                  to analyze at-risk donors and generate a re-engagement strategy, or{" "}
                  <Link href="/intelligence" style={{ color: "var(--accent)", textDecoration: "none", fontWeight: 500 }}>
                    view full intelligence report
                  </Link>.
                </div>
              )}
            </div>
          )}

          {/* ── Hero KPIs ── */}
          {meaningfulMetrics.length > 0 && (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12, marginBottom: 28 }}>
              {meaningfulMetrics.slice(0, 6).map((m) => (
                <div key={m.metric_key} className="card-base" style={{ padding: "18px 20px" }}>
                  <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", fontWeight: 500, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                    {m.display_name}
                  </div>
                  <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
                    <span style={{ fontSize: "1.3rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.02em" }}>
                      {formatMetricValue(m.value, m.format_hint, m.unit)}
                    </span>
                    {m.delta_pct !== null && m.delta_pct !== undefined && m.delta_pct !== 0 && (
                      <span style={{
                        fontSize: "0.7rem",
                        fontWeight: 600,
                        color: m.delta_pct >= 0 ? "var(--green)" : "var(--red)",
                      }}>
                        {m.delta_pct >= 0 ? "+" : ""}{m.delta_pct.toFixed(1)}%
                      </span>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* ── Top At-Risk Donors ── */}
          {(intel?.risks ?? []).length > 0 && (
            <div style={{ marginBottom: 28 }}>
              <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 10 }}>
                <h2 style={{ fontSize: "0.88rem", fontWeight: 600, color: "var(--text-primary)", margin: 0 }}>
                  Top At-Risk Donors
                </h2>
                <Link href="/intelligence" style={{ fontSize: "0.76rem", color: "var(--accent)", textDecoration: "none", fontWeight: 500 }}>
                  View all &rarr;
                </Link>
              </div>
              <div className="card-base" style={{ overflow: "hidden" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--surface-border)" }}>
                      <th style={thStyle}>Donor</th>
                      <th style={thStyle}>Risk</th>
                      <th style={{ ...thStyle, textAlign: "right" }}>Revenue at Risk</th>
                    </tr>
                  </thead>
                  <tbody>
                    {intel!.risks!.slice(0, 5).map((r) => (
                      <tr key={r.person_id} style={{ borderBottom: "1px solid var(--surface-border)" }}>
                        <td style={{ padding: "10px 16px", fontWeight: 500, color: "var(--text-primary)" }}>
                          {r.display_name}
                        </td>
                        <td style={{ padding: "10px 16px" }}>
                          <span style={{
                            fontSize: "0.7rem",
                            fontWeight: 600,
                            padding: "2px 10px",
                            borderRadius: 20,
                            color: r.risk_level === "critical" ? "var(--red)" : "var(--orange)",
                            background: r.risk_level === "critical" ? "rgba(255, 59, 48, 0.1)" : "rgba(255, 149, 0, 0.1)",
                            textTransform: "capitalize",
                          }}>
                            {r.risk_level}
                          </span>
                        </td>
                        <td style={{ padding: "10px 16px", textAlign: "right", fontWeight: 600, color: "var(--text-primary)" }}>
                          ${(r.annual_revenue_at_risk ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* ── Quick Actions ── */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
            <ActionCard href="/chat" label="Ask Sozo" sub="Chat with your data" accent="var(--accent)" />
            <ActionCard href="/intelligence" label="Intelligence" sub="Insights & risk analysis" accent="var(--accent-secondary)" />
            <ActionCard href="/settings" label="Settings" sub="Manage workspace" accent="var(--text-muted)" />
          </div>
        </>
      )}
    </div>
  );
}

const thStyle: React.CSSProperties = {
  textAlign: "left",
  padding: "10px 16px",
  color: "var(--text-muted)",
  fontWeight: 500,
  fontSize: "0.7rem",
  textTransform: "uppercase",
  letterSpacing: "0.04em",
};

function KeyNumber({ label, value, color }: { label: string; value: string; color: string }) {
  return (
    <div style={{ display: "flex", alignItems: "baseline", gap: 8 }}>
      <span style={{ fontSize: "1.1rem", fontWeight: 700, color, letterSpacing: "-0.02em" }}>{value}</span>
      <span style={{ fontSize: "0.76rem", color: "var(--text-muted)" }}>{label}</span>
    </div>
  );
}

function ActionCard({ href, label, sub, accent }: { href: string; label: string; sub: string; accent: string }) {
  return (
    <Link href={href} style={{ textDecoration: "none" }}>
      <div className="card-base" style={{ padding: "18px 20px", cursor: "pointer", borderTop: `3px solid ${accent}` }}>
        <div style={{ fontSize: "0.86rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: 2 }}>{label}</div>
        <div style={{ fontSize: "0.74rem", color: "var(--text-muted)" }}>{sub}</div>
      </div>
    </Link>
  );
}
