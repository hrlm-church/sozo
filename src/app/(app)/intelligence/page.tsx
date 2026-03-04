"use client";

import { useEffect, useState, useCallback } from "react";

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
  status: string;
  created_at: string;
}

interface Metric {
  metric_key: string;
  display_name: string;
  unit: string;
  format_hint: string;
  value: number;
  prior_value: number;
  delta_pct: number;
  as_of_date: string;
}

interface RiskDonor {
  person_id: string;
  display_name: string;
  email: string;
  total_given: number;
  last_gift_date: string;
  days_since_last: number;
  lifecycle_stage: string;
  risk_score: number;
  risk_level: string;
  drivers_json: string;
  annual_revenue_at_risk: number;
}

type TabId = "insights" | "metrics" | "risks";

export default function IntelligencePage() {
  const [activeTab, setActiveTab] = useState<TabId>("insights");
  const [insights, setInsights] = useState<Insight[]>([]);
  const [metrics, setMetrics] = useState<Metric[]>([]);
  const [risks, setRisks] = useState<RiskDonor[]>([]);
  const [loading, setLoading] = useState(true);
  const [filterSeverity, setFilterSeverity] = useState<string>("all");

  useEffect(() => {
    setLoading(true);
    fetch("/api/intel")
      .then((r) => r.json())
      .then((data) => {
        setInsights(data.insights ?? []);
        setMetrics(data.metrics ?? []);
        setRisks(data.risks ?? []);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, []);

  const tabs: { id: TabId; label: string; count: number }[] = [
    { id: "insights", label: "Insights", count: insights.length },
    { id: "metrics", label: "Metrics", count: metrics.length },
    { id: "risks", label: "At-Risk Donors", count: risks.length },
  ];

  const filteredInsights = filterSeverity === "all"
    ? insights
    : insights.filter((i) => i.severity === filterSeverity);

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Intelligence
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Insights, metrics, and risk analysis from your data
        </p>
      </div>

      {/* Tabs */}
      <div style={{ display: "flex", gap: 4, marginBottom: 24, borderBottom: "1px solid var(--surface-border)", paddingBottom: 0 }}>
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => setActiveTab(tab.id)}
            style={{
              padding: "10px 18px",
              border: "none",
              background: "transparent",
              color: activeTab === tab.id ? "var(--accent)" : "var(--text-muted)",
              fontWeight: activeTab === tab.id ? 600 : 400,
              fontSize: "0.84rem",
              cursor: "pointer",
              borderBottom: activeTab === tab.id ? "2px solid var(--accent)" : "2px solid transparent",
              marginBottom: -1,
              transition: "all 150ms ease",
            }}
          >
            {tab.label}
            <span style={{
              marginLeft: 6,
              fontSize: "0.72rem",
              background: activeTab === tab.id ? "var(--accent-light)" : "var(--surface-border)",
              padding: "1px 7px",
              borderRadius: 10,
              fontWeight: 600,
            }}>
              {tab.count}
            </span>
          </button>
        ))}
      </div>

      {loading ? (
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--text-muted)", fontSize: "0.84rem", padding: "40px 0" }}>
          <span className="loading-pulse" style={{ width: 8, height: 8, borderRadius: "50%", background: "var(--accent)" }} />
          Loading intelligence data...
        </div>
      ) : (
        <>
          {/* Insights tab */}
          {activeTab === "insights" && (
            <div>
              {/* Severity filter */}
              <div style={{ display: "flex", gap: 6, marginBottom: 16 }}>
                {["all", "critical", "high", "medium", "low"].map((sev) => (
                  <button
                    key={sev}
                    onClick={() => setFilterSeverity(sev)}
                    style={{
                      padding: "5px 14px",
                      borderRadius: 20,
                      border: "1px solid",
                      borderColor: filterSeverity === sev ? "var(--accent)" : "var(--surface-border)",
                      background: filterSeverity === sev ? "var(--accent-light)" : "var(--surface)",
                      color: filterSeverity === sev ? "var(--accent)" : "var(--text-muted)",
                      fontSize: "0.76rem",
                      fontWeight: 500,
                      cursor: "pointer",
                      textTransform: "capitalize",
                      transition: "all 150ms ease",
                    }}
                  >
                    {sev}
                  </button>
                ))}
              </div>

              {filteredInsights.length === 0 ? (
                <EmptyState message="No insights matching the selected filter" />
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {filteredInsights.map((insight) => (
                    <InsightCard key={insight.insight_id} insight={insight} />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Metrics tab */}
          {activeTab === "metrics" && (
            <div>
              {metrics.length === 0 ? (
                <EmptyState message="No metrics available yet" />
              ) : (
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 12 }}>
                  {metrics.map((m) => (
                    <MetricCard key={m.metric_key} metric={m} />
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Risks tab */}
          {activeTab === "risks" && (
            <div>
              {risks.length === 0 ? (
                <EmptyState message="No high-risk donors identified" />
              ) : (
                <div className="card-base" style={{ overflow: "hidden" }}>
                  <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
                    <thead>
                      <tr style={{ borderBottom: "1px solid var(--surface-border)" }}>
                        {["Donor", "Risk Level", "Score", "Lifecycle", "Last Gift", "Days Since", "Total Given", "Revenue at Risk"].map((h) => (
                          <th key={h} style={{ textAlign: h === "Donor" || h === "Risk Level" || h === "Lifecycle" ? "left" : "right", padding: "10px 14px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>
                            {h}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {risks.map((r) => (
                        <tr key={r.person_id} style={{ borderBottom: "1px solid var(--surface-border)" }}>
                          <td style={{ padding: "10px 14px", fontWeight: 500, color: "var(--text-primary)" }}>{r.display_name}</td>
                          <td style={{ padding: "10px 14px" }}>
                            <RiskBadge level={r.risk_level} />
                          </td>
                          <td style={{ padding: "10px 14px", textAlign: "right", fontFamily: "var(--font-geist-mono)", fontSize: "0.78rem" }}>
                            {(r.risk_score ?? 0).toFixed(2)}
                          </td>
                          <td style={{ padding: "10px 14px", color: "var(--text-secondary)", fontSize: "0.78rem" }}>
                            {r.lifecycle_stage ?? "-"}
                          </td>
                          <td style={{ padding: "10px 14px", textAlign: "right", color: "var(--text-secondary)", fontSize: "0.78rem" }}>
                            {r.last_gift_date ? new Date(r.last_gift_date).toLocaleDateString() : "-"}
                          </td>
                          <td style={{ padding: "10px 14px", textAlign: "right", color: "var(--text-secondary)", fontSize: "0.78rem" }}>
                            {r.days_since_last ?? "-"}
                          </td>
                          <td style={{ padding: "10px 14px", textAlign: "right", fontWeight: 600, color: "var(--text-primary)" }}>
                            ${(r.total_given ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                          </td>
                          <td style={{ padding: "10px 14px", textAlign: "right", fontWeight: 600, color: "var(--red)" }}>
                            ${(r.annual_revenue_at_risk ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function InsightCard({ insight }: { insight: Insight }) {
  const severityColors: Record<string, { bg: string; fg: string }> = {
    critical: { bg: "rgba(255, 59, 48, 0.1)", fg: "var(--red)" },
    high: { bg: "rgba(255, 149, 0, 0.1)", fg: "var(--orange)" },
    medium: { bg: "rgba(0, 113, 227, 0.08)", fg: "var(--accent)" },
    low: { bg: "var(--surface-border)", fg: "var(--text-muted)" },
  };

  const colors = severityColors[insight.severity] ?? severityColors.low;

  return (
    <div className="card-base" style={{ padding: "16px 20px" }}>
      <div style={{ display: "flex", alignItems: "flex-start", gap: 12 }}>
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 6 }}>
            <span style={{
              fontSize: "0.68rem",
              fontWeight: 600,
              padding: "2px 10px",
              borderRadius: 20,
              color: colors.fg,
              background: colors.bg,
              textTransform: "capitalize",
            }}>
              {insight.severity}
            </span>
            <span style={{ fontSize: "0.68rem", color: "var(--text-muted)" }}>
              {insight.insight_type}
            </span>
          </div>
          <div style={{ fontSize: "0.88rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: 4 }}>
            {insight.title}
          </div>
          <div style={{ fontSize: "0.8rem", color: "var(--text-secondary)", lineHeight: 1.5 }}>
            {insight.summary}
          </div>
        </div>
        {insight.delta_pct !== null && insight.delta_pct !== undefined && (
          <div style={{ textAlign: "right", flexShrink: 0 }}>
            <div style={{ fontSize: "1.1rem", fontWeight: 700, color: insight.delta_pct >= 0 ? "var(--green)" : "var(--red)" }}>
              {insight.delta_pct >= 0 ? "+" : ""}{insight.delta_pct.toFixed(1)}%
            </div>
            <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", marginTop: 2 }}>
              vs baseline
            </div>
          </div>
        )}
      </div>
      <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", marginTop: 8 }}>
        {new Date(insight.created_at).toLocaleDateString()} &middot; {insight.metric_key}
      </div>
    </div>
  );
}

function MetricCard({ metric }: { metric: Metric }) {
  const formatVal = (val: number) => {
    if (metric.format_hint === "currency" || metric.unit === "USD") return `$${val?.toLocaleString(undefined, { maximumFractionDigits: 0 }) ?? "0"}`;
    if (metric.format_hint === "percentage" || metric.unit === "%") return `${(val ?? 0).toFixed(1)}%`;
    if (metric.format_hint === "integer") return (val ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
    return (val ?? 0).toLocaleString(undefined, { maximumFractionDigits: 1 });
  };

  const positive = (metric.delta_pct ?? 0) >= 0;

  return (
    <div className="card-base" style={{ padding: "18px 20px" }}>
      <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, marginBottom: 8, textTransform: "uppercase", letterSpacing: "0.04em" }}>
        {metric.display_name}
      </div>
      <div style={{ display: "flex", alignItems: "baseline", gap: 10, marginBottom: 6 }}>
        <span style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.02em" }}>
          {formatVal(metric.value)}
        </span>
        {metric.delta_pct !== null && metric.delta_pct !== undefined && (
          <span style={{
            fontSize: "0.74rem",
            fontWeight: 600,
            color: positive ? "var(--green)" : "var(--red)",
            background: positive ? "rgba(52, 199, 89, 0.1)" : "rgba(255, 59, 48, 0.1)",
            padding: "2px 8px",
            borderRadius: 20,
          }}>
            {positive ? "+" : ""}{metric.delta_pct.toFixed(1)}%
          </span>
        )}
      </div>
      {metric.prior_value !== null && metric.prior_value !== undefined && (
        <div style={{ fontSize: "0.72rem", color: "var(--text-muted)" }}>
          Prior: {formatVal(metric.prior_value)}
        </div>
      )}
    </div>
  );
}

function RiskBadge({ level }: { level: string }) {
  const color = level === "critical" ? "var(--red)" : "var(--orange)";
  const bg = level === "critical" ? "rgba(255, 59, 48, 0.1)" : "rgba(255, 149, 0, 0.1)";
  return (
    <span style={{
      fontSize: "0.72rem",
      fontWeight: 600,
      padding: "2px 10px",
      borderRadius: 20,
      color,
      background: bg,
      textTransform: "capitalize",
    }}>
      {level}
    </span>
  );
}

function EmptyState({ message }: { message: string }) {
  return (
    <div style={{ padding: "48px 20px", textAlign: "center", color: "var(--text-muted)", fontSize: "0.84rem" }}>
      {message}
    </div>
  );
}
