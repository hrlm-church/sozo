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

interface IntelData {
  metrics?: Metric[];
  insights?: Insight[];
  risks?: { person_id: string; display_name: string; risk_score: number; risk_level: string; annual_revenue_at_risk: number }[];
  summary?: { open_insights?: number; high_risk_donors?: number; metrics_computed?: number; last_snapshot_date?: string };
}

function formatValue(value: number, hint: string, unit: string): string {
  if (hint === "currency" || unit === "USD") return `$${value?.toLocaleString(undefined, { maximumFractionDigits: 0 }) ?? "0"}`;
  if (hint === "percentage" || unit === "%") return `${(value ?? 0).toFixed(1)}%`;
  if (hint === "integer") return (value ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
  return (value ?? 0).toLocaleString(undefined, { maximumFractionDigits: 1 });
}

function DeltaBadge({ delta }: { delta: number | null }) {
  if (delta === null || delta === undefined) return null;
  const positive = delta >= 0;
  return (
    <span
      style={{
        fontSize: "0.72rem",
        fontWeight: 600,
        color: positive ? "var(--green)" : "var(--red)",
        background: positive ? "rgba(52, 199, 89, 0.1)" : "rgba(255, 59, 48, 0.1)",
        padding: "2px 8px",
        borderRadius: 20,
      }}
    >
      {positive ? "+" : ""}{delta.toFixed(1)}%
    </span>
  );
}

function SeverityDot({ severity }: { severity: string }) {
  const color = severity === "critical" ? "var(--red)" : severity === "high" ? "var(--orange)" : "var(--accent)";
  return <span style={{ display: "inline-block", width: 8, height: 8, borderRadius: "50%", background: color, flexShrink: 0 }} />;
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

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 32 }}>
        <h1 style={{ fontSize: "1.6rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          {greeting()}{session?.user?.name ? `, ${session.user.name}` : ""}
        </h1>
        <p style={{ fontSize: "0.88rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Here&apos;s your ministry intelligence overview
        </p>
      </div>

      {loading ? (
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--text-muted)", fontSize: "0.84rem" }}>
          <span className="loading-pulse" style={{ width: 8, height: 8, borderRadius: "50%", background: "var(--accent)" }} />
          Loading intelligence data...
        </div>
      ) : (
        <>
          {/* Summary stat cards */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))", gap: 16, marginBottom: 32 }}>
            <StatCard label="Open Insights" value={String(intel?.summary?.open_insights ?? 0)} icon="insight" />
            <StatCard label="High-Risk Donors" value={String(intel?.summary?.high_risk_donors ?? 0)} icon="risk" />
            <StatCard label="Metrics Tracked" value={String(intel?.summary?.metrics_computed ?? 0)} icon="metric" />
            <StatCard
              label="Last Snapshot"
              value={intel?.summary?.last_snapshot_date ? new Date(intel.summary.last_snapshot_date).toLocaleDateString() : "N/A"}
              icon="date"
            />
          </div>

          {/* Metrics grid */}
          {intel?.metrics && intel.metrics.length > 0 && (
            <Section title="Key Metrics">
              <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))", gap: 12 }}>
                {intel.metrics.slice(0, 8).map((m) => (
                  <div key={m.metric_key} className="card-base" style={{ padding: "16px 20px" }}>
                    <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                      {m.display_name}
                    </div>
                    <div style={{ display: "flex", alignItems: "baseline", gap: 10 }}>
                      <span style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.02em" }}>
                        {formatValue(m.value, m.format_hint, m.unit)}
                      </span>
                      <DeltaBadge delta={m.delta_pct} />
                    </div>
                  </div>
                ))}
              </div>
            </Section>
          )}

          {/* Active insights */}
          {intel?.insights && intel.insights.length > 0 && (
            <Section title="Active Insights">
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {intel.insights.slice(0, 6).map((insight) => (
                  <div key={insight.insight_id} className="card-base" style={{ padding: "14px 20px", display: "flex", alignItems: "flex-start", gap: 12 }}>
                    <SeverityDot severity={insight.severity} />
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <div style={{ fontSize: "0.84rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: 2 }}>
                        {insight.title}
                      </div>
                      <div style={{ fontSize: "0.78rem", color: "var(--text-secondary)", lineHeight: 1.4 }}>
                        {insight.summary}
                      </div>
                    </div>
                    {insight.delta_pct !== null && (
                      <DeltaBadge delta={insight.delta_pct} />
                    )}
                  </div>
                ))}
              </div>
            </Section>
          )}

          {/* High-risk donors */}
          {intel?.risks && intel.risks.length > 0 && (
            <Section title="High-Risk Donors">
              <div className="card-base" style={{ overflow: "hidden" }}>
                <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
                  <thead>
                    <tr style={{ borderBottom: "1px solid var(--surface-border)" }}>
                      <th style={{ textAlign: "left", padding: "10px 16px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Donor</th>
                      <th style={{ textAlign: "left", padding: "10px 16px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Risk Level</th>
                      <th style={{ textAlign: "right", padding: "10px 16px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Revenue at Risk</th>
                    </tr>
                  </thead>
                  <tbody>
                    {intel.risks.slice(0, 8).map((r) => (
                      <tr key={r.person_id} style={{ borderBottom: "1px solid var(--surface-border)" }}>
                        <td style={{ padding: "10px 16px", color: "var(--text-primary)", fontWeight: 500 }}>{r.display_name}</td>
                        <td style={{ padding: "10px 16px" }}>
                          <span style={{
                            fontSize: "0.72rem",
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
            </Section>
          )}

          {/* Quick actions */}
          <Section title="Quick Actions">
            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(200px, 1fr))", gap: 12 }}>
              <QuickAction href="/chat" title="Ask Sozo" description="Chat with your data" icon="chat" />
              <QuickAction href="/intelligence" title="Intelligence" description="View insights & alerts" icon="intel" />
              <QuickAction href="/settings" title="Settings" description="Manage your workspace" icon="settings" />
            </div>
          </Section>
        </>
      )}
    </div>
  );
}

function StatCard({ label, value, icon }: { label: string; value: string; icon: string }) {
  const iconMap: Record<string, string> = { insight: "\u{1F4CA}", risk: "\u26A0\uFE0F", metric: "\u{1F4C8}", date: "\u{1F4C5}" };
  return (
    <div className="card-base" style={{ padding: "18px 20px", display: "flex", alignItems: "center", gap: 14 }}>
      <div style={{ width: 40, height: 40, borderRadius: 10, background: "var(--accent-light)", display: "flex", alignItems: "center", justifyContent: "center", fontSize: "1.1rem", flexShrink: 0 }}>
        {iconMap[icon] ?? "\u{1F4CB}"}
      </div>
      <div>
        <div style={{ fontSize: "1.2rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.02em" }}>{value}</div>
        <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500 }}>{label}</div>
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section style={{ marginBottom: 32 }}>
      <h2 style={{ fontSize: "0.88rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: 12, letterSpacing: "-0.01em" }}>
        {title}
      </h2>
      {children}
    </section>
  );
}

function QuickAction({ href, title, description, icon }: { href: string; title: string; description: string; icon: string }) {
  return (
    <Link
      href={href}
      style={{
        textDecoration: "none",
        display: "block",
      }}
    >
      <div
        className="card-base"
        style={{
          padding: "20px",
          cursor: "pointer",
          transition: "all 150ms ease",
        }}
      >
        <div style={{ fontSize: "0.88rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: 4 }}>
          {title}
        </div>
        <div style={{ fontSize: "0.76rem", color: "var(--text-muted)" }}>
          {description}
        </div>
      </div>
    </Link>
  );
}
