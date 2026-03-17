"use client";

import { useEffect, useState, useCallback } from "react";

interface BriefingSection {
  title: string;
  content: string;
  data?: unknown;
}

interface SuggestedAction {
  title: string;
  type: string;
  priority: number;
  person_name?: string;
}

interface BriefingContent {
  date: string;
  summary: string;
  sections: BriefingSection[];
  suggested_actions: SuggestedAction[];
}

interface Briefing {
  id: string;
  date: string;
  content: BriefingContent;
  metrics: Record<string, unknown> | null;
  action_count: number;
  created_at: string;
}

export default function BriefingPage() {
  const [briefing, setBriefing] = useState<Briefing | null>(null);
  const [loading, setLoading] = useState(true);
  const [generating, setGenerating] = useState(false);

  const fetchBriefing = useCallback(async () => {
    try {
      const res = await fetch("/api/briefing/latest");
      if (!res.ok) return;
      const data = await res.json();
      setBriefing(data.briefing);
    } catch {
      // Silent fail
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchBriefing();
  }, [fetchBriefing]);

  const handleGenerate = async () => {
    setGenerating(true);
    try {
      const res = await fetch("/api/briefing/generate", { method: "POST" });
      if (res.ok) {
        await fetchBriefing();
      }
    } catch {
      // Silent fail
    } finally {
      setGenerating(false);
    }
  };

  const severityIcon = (type: string) => {
    switch (type) {
      case "thank": return { bg: "rgba(52, 199, 89, 0.1)", color: "var(--green)", label: "Thank" };
      case "reengage": return { bg: "rgba(255, 149, 0, 0.1)", color: "var(--orange)", label: "Re-engage" };
      case "call": return { bg: "rgba(0, 113, 227, 0.1)", color: "var(--accent)", label: "Call" };
      default: return { bg: "rgba(0, 0, 0, 0.04)", color: "var(--text-muted)", label: type };
    }
  };

  if (loading) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 900 }}>
        <div style={{ textAlign: "center", padding: 60, color: "var(--text-muted)", fontSize: "0.84rem" }}>
          Loading briefing...
        </div>
      </div>
    );
  }

  return (
    <div style={{ padding: "32px 40px", maxWidth: 900 }}>
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "flex-start", marginBottom: 28 }}>
        <div>
          <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
            Daily Briefing
          </h1>
          <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
            {briefing
              ? `Generated ${new Date(briefing.created_at).toLocaleString()}`
              : "No briefing generated yet"}
          </p>
        </div>
        <button
          onClick={handleGenerate}
          disabled={generating}
          className="btn-primary"
          style={{ fontSize: "0.82rem" }}
        >
          {generating ? "Generating..." : "Generate Now"}
        </button>
      </div>

      {!briefing ? (
        <div className="card-base" style={{ padding: "60px 40px", textAlign: "center" }}>
          <div style={{ fontSize: "0.92rem", fontWeight: 600, color: "var(--text-secondary)", marginBottom: 8 }}>
            No briefing available
          </div>
          <div style={{ fontSize: "0.82rem", color: "var(--text-muted)", maxWidth: 400, margin: "0 auto", lineHeight: 1.6 }}>
            Click &ldquo;Generate Now&rdquo; to create your first daily intelligence briefing.
          </div>
        </div>
      ) : (
        <>
          {/* Metrics bar */}
          {briefing.metrics && (
            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 20 }}>
              {briefing.metrics.active_donors !== undefined && (
                <div className="card-base" style={{ padding: "16px 20px" }}>
                  <div style={{ fontSize: "0.68rem", fontWeight: 500, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em" }}>Active Donors</div>
                  <div style={{ fontSize: "1.3rem", fontWeight: 700, color: "var(--text-primary)", marginTop: 4 }}>{Number(briefing.metrics.active_donors).toLocaleString()}</div>
                </div>
              )}
              {briefing.metrics.giving_this_week !== undefined && (
                <div className="card-base" style={{ padding: "16px 20px" }}>
                  <div style={{ fontSize: "0.68rem", fontWeight: 500, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em" }}>Giving This Week</div>
                  <div style={{ fontSize: "1.3rem", fontWeight: 700, color: "var(--green)", marginTop: 4 }}>${Number(briefing.metrics.giving_this_week ?? 0).toLocaleString()}</div>
                </div>
              )}
              {briefing.metrics.new_donor_count !== undefined && (
                <div className="card-base" style={{ padding: "16px 20px" }}>
                  <div style={{ fontSize: "0.68rem", fontWeight: 500, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em" }}>New Donors</div>
                  <div style={{ fontSize: "1.3rem", fontWeight: 700, color: "var(--accent)", marginTop: 4 }}>{Number(briefing.metrics.new_donor_count)}</div>
                </div>
              )}
              {briefing.metrics.churn_critical !== undefined && (
                <div className="card-base" style={{ padding: "16px 20px" }}>
                  <div style={{ fontSize: "0.68rem", fontWeight: 500, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em" }}>Churn Risk</div>
                  <div style={{ fontSize: "1.3rem", fontWeight: 700, color: "var(--red)", marginTop: 4 }}>{Number(briefing.metrics.churn_critical)} critical</div>
                </div>
              )}
            </div>
          )}

          {/* Sections */}
          <div style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {briefing.content.sections.map((section, i) => (
              <div key={i} className="card-base" style={{ padding: "20px 24px" }}>
                <div style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 8 }}>
                  {section.title}
                </div>
                <div style={{ fontSize: "0.88rem", color: "var(--text-secondary)", lineHeight: 1.6 }}>
                  {section.content}
                </div>
              </div>
            ))}
          </div>

          {/* Suggested Actions */}
          {briefing.content.suggested_actions.length > 0 && (
            <div style={{ marginTop: 24 }}>
              <div style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 12 }}>
                Suggested Actions
              </div>
              <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
                {briefing.content.suggested_actions.map((action, i) => {
                  const style = severityIcon(action.type);
                  return (
                    <div key={i} className="card-base" style={{ padding: "14px 20px", display: "flex", alignItems: "center", gap: 12 }}>
                      <span style={{
                        padding: "4px 10px",
                        borderRadius: 20,
                        fontSize: "0.68rem",
                        fontWeight: 600,
                        background: style.bg,
                        color: style.color,
                        textTransform: "uppercase",
                        letterSpacing: "0.02em",
                      }}>
                        {style.label}
                      </span>
                      <span style={{ fontSize: "0.84rem", color: "var(--text-primary)", flex: 1 }}>
                        {action.title}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}
