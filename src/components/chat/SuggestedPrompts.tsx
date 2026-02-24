"use client";

const PROMPTS = [
  {
    label: "Show me donors who gave over $1K but haven't given in 6+ months",
    desc: "High-value churn risk — who needs outreach now",
  },
  {
    label: "Compare active vs cooling vs lapsed donors — count, total giving, and avg gift",
    desc: "Donor health segmentation at a glance",
  },
  {
    label: "Which donors also buy products and attend events?",
    desc: "Multi-channel engaged supporters",
  },
  {
    label: "Show wealth-screened donors giving below 10% of their capacity",
    desc: "Major gift prospects with untapped potential",
  },
  {
    label: "Show monthly giving trends for the last 24 months",
    desc: "Revenue trajectory and seasonal patterns",
  },
  {
    label: "Active Subbly subscribers — how many also donate?",
    desc: "Subscription-to-donor conversion opportunity",
  },
  {
    label: "Show the 383 lost recurring donors ranked by annual value",
    desc: "MRR recovery priorities — $205K/year at stake",
  },
  {
    label: "Top 20 people by total engagement across giving, orders, events, and subscriptions",
    desc: "Most connected supporters across all streams",
  },
];

interface SuggestedPromptsProps {
  onSelect: (prompt: string) => void;
  compact?: boolean;
}

export function SuggestedPrompts({ onSelect, compact }: SuggestedPromptsProps) {
  return (
    <div style={{ padding: compact ? "16px 0" : "40px 0" }}>
      {!compact && (
        <div style={{ textAlign: "center", marginBottom: 32 }}>
          <h2
            style={{
              fontSize: "1.4rem",
              fontWeight: 700,
              letterSpacing: "-0.025em",
              color: "var(--text-primary)",
              margin: 0,
            }}
          >
            What would you like to know?
          </h2>
          <p
            style={{
              color: "var(--text-muted)",
              fontSize: "0.84rem",
              margin: "8px 0 0",
            }}
          >
            89K people across 13 sources — ask anything
          </p>
        </div>
      )}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 10,
        }}
      >
        {PROMPTS.map((item) => (
          <button
            key={item.label}
            onClick={() => onSelect(item.label)}
            style={{
              background: "var(--surface-elevated)",
              border: "1px solid var(--surface-border)",
              borderRadius: "var(--r-md)",
              padding: compact ? "12px 14px" : "16px 18px",
              fontSize: "0.82rem",
              fontWeight: 500,
              color: "var(--text-primary)",
              cursor: "pointer",
              textAlign: "left",
              transition: "all 200ms ease",
              lineHeight: 1.45,
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(0, 113, 227, 0.04)";
              e.currentTarget.style.borderColor = "rgba(0, 113, 227, 0.2)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "var(--surface-elevated)";
              e.currentTarget.style.borderColor = "var(--surface-border)";
            }}
          >
            <div>{item.label}</div>
            <div
              style={{
                fontSize: "0.72rem",
                color: "var(--text-muted)",
                marginTop: 4,
                fontWeight: 400,
              }}
            >
              {item.desc}
            </div>
          </button>
        ))}
      </div>
    </div>
  );
}
