"use client";

const PROMPTS = [
  {
    label: "Build me an executive dashboard of our ministry's health",
    desc: "The big picture — giving, engagement, and risk at a glance",
  },
  {
    label: "Tell me the story of our giving over the last 3 years — what's working and what's fading?",
    desc: "Trends, seasonality, and the inflection points that matter",
  },
  {
    label: "Who are our hidden champions — people supporting us across every channel?",
    desc: "The donors who also buy, attend, and subscribe",
  },
  {
    label: "Find the donors whose generosity doesn't match their wealth",
    desc: "Untapped capacity hiding in our database",
  },
  {
    label: "What would we recover if we re-engaged our lapsed recurring donors?",
    desc: "The $205K/year opportunity sitting in our data",
  },
  {
    label: "Map our supporter journey — from first touch to loyal donor",
    desc: "The lifecycle funnel that shows where people drop off",
  },
  {
    label: "Which events created the most new donors afterward?",
    desc: "The conversion power of live experiences",
  },
  {
    label: "Show me who's quietly slipping away — our biggest donors at risk right now",
    desc: "The calls you need to make this week",
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
