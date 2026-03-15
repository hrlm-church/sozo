"use client";

interface InsightPanelProps {
  trends: string[];
  alerts: string[];
  opportunities: string[];
}

export function InsightPanel({ trends, alerts, opportunities }: InsightPanelProps) {
  const sections: { label: string; items: string[]; color: string; icon: string }[] = [
    { label: "Trends", items: trends, color: "var(--accent)", icon: "\u2191" },
    { label: "Alerts", items: alerts, color: "var(--orange)", icon: "\u26A0" },
    { label: "Opportunities", items: opportunities, color: "var(--green)", icon: "\u2605" },
  ];

  return (
    <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 16 }}>
      {sections.map((sec) => (
        <div key={sec.label} className="card-base" style={{ padding: "18px 20px" }}>
          <div style={{
            fontSize: "0.72rem",
            fontWeight: 700,
            color: sec.color,
            textTransform: "uppercase",
            letterSpacing: "0.05em",
            marginBottom: 12,
          }}>
            {sec.icon} {sec.label}
          </div>
          {sec.items.length === 0 ? (
            <div style={{ fontSize: "0.8rem", color: "var(--text-muted)" }}>None identified</div>
          ) : (
            sec.items.map((item, i) => (
              <div key={i} style={{
                fontSize: "0.82rem",
                color: "var(--text-secondary)",
                lineHeight: 1.6,
                marginBottom: 6,
                paddingLeft: 8,
                borderLeft: `2px solid ${sec.color}`,
              }}>
                {item}
              </div>
            ))
          )}
        </div>
      ))}
    </div>
  );
}
