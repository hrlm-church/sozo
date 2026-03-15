"use client";

interface BriefingCardProps {
  title: string;
  headline: string;
  strengths: string[];
  risks: string[];
  action: string;
  metrics: { label: string; value: string }[];
  generatedAt?: string;
  loading?: boolean;
}

export function BriefingCard({ title, headline, strengths, risks, action, metrics, generatedAt, loading }: BriefingCardProps) {
  if (loading) {
    return (
      <div className="card-base" style={{ padding: "28px 32px" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--text-muted)", fontSize: "0.84rem" }}>
          <span className="loading-pulse" style={{ width: 8, height: 8, borderRadius: "50%", background: "var(--accent)" }} />
          Loading briefing...
        </div>
      </div>
    );
  }

  return (
    <div className="card-base" style={{ padding: "28px 32px", borderLeft: "4px solid var(--accent)" }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 16 }}>
        <span style={{ fontSize: "0.9rem", fontWeight: 700, color: "var(--text-primary)" }}>{title}</span>
        {generatedAt && (
          <span style={{ fontSize: "0.7rem", color: "var(--text-muted)" }}>{generatedAt}</span>
        )}
      </div>

      <p style={{ fontSize: "0.88rem", color: "var(--text-secondary)", lineHeight: 1.6, marginBottom: 20, margin: "0 0 20px" }}>
        {headline}
      </p>

      {metrics.length > 0 && (
        <div style={{ display: "flex", gap: 24, marginBottom: 20, flexWrap: "wrap" }}>
          {metrics.map((m) => (
            <div key={m.label}>
              <div style={{ fontSize: "1.1rem", fontWeight: 700, color: "var(--text-primary)" }}>{m.value}</div>
              <div style={{ fontSize: "0.7rem", color: "var(--text-muted)", marginTop: 2 }}>{m.label}</div>
            </div>
          ))}
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginBottom: 20 }}>
        <div>
          <div style={{ fontSize: "0.72rem", fontWeight: 700, color: "var(--green)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 8 }}>
            Strengths
          </div>
          {strengths.map((s, i) => (
            <div key={i} style={{ fontSize: "0.82rem", color: "var(--text-secondary)", lineHeight: 1.6, marginBottom: 4, paddingLeft: 12, position: "relative" }}>
              <span style={{ position: "absolute", left: 0, color: "var(--green)" }}>+</span>
              {s}
            </div>
          ))}
        </div>
        <div>
          <div style={{ fontSize: "0.72rem", fontWeight: 700, color: "var(--red)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 8 }}>
            Risks
          </div>
          {risks.map((r, i) => (
            <div key={i} style={{ fontSize: "0.82rem", color: "var(--text-secondary)", lineHeight: 1.6, marginBottom: 4, paddingLeft: 12, position: "relative" }}>
              <span style={{ position: "absolute", left: 0, color: "var(--red)" }}>-</span>
              {r}
            </div>
          ))}
        </div>
      </div>

      {action && (
        <div style={{
          padding: "12px 16px",
          background: "rgba(0, 113, 227, 0.04)",
          borderRadius: 10,
          fontSize: "0.82rem",
          color: "var(--text-secondary)",
          lineHeight: 1.5,
        }}>
          <strong style={{ color: "var(--accent)" }}>Recommended:</strong> {action}
        </div>
      )}
    </div>
  );
}
