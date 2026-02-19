"use client";

const PROMPTS = [
  {
    icon: "\uD83D\uDCCA",
    label: "Executive Dashboard",
    prompt: "Build me an executive giving dashboard with total donations, top donors, and monthly trends",
  },
  {
    icon: "\u2B50",
    label: "Top Donors",
    prompt: "Who are our top 20 donors? Show me their giving by month over the last 2 years",
  },
  {
    icon: "\uD83C\uDFAF",
    label: "Fund Breakdown",
    prompt: "Break down donations by fund \u2014 which funds bring in the most?",
  },
  {
    icon: "\uD83D\uDC65",
    label: "Lifecycle Stages",
    prompt: "How many people do we have vs actual donors? Show me the lifecycle stages",
  },
  {
    icon: "\uD83D\uDED2",
    label: "Commerce Overview",
    prompt: "Show commerce overview \u2014 total orders, payments, and active subscriptions",
  },
  {
    icon: "\uD83D\uDD17",
    label: "Source Systems",
    prompt: "Which source systems contribute the most data? Compare Keap, Donor Direct, Bloomerang, and the rest",
  },
];

interface SuggestedPromptsProps {
  onSelect: (prompt: string) => void;
}

export function SuggestedPrompts({ onSelect }: SuggestedPromptsProps) {
  return (
    <div style={{ padding: "32px 0" }}>
      <div style={{ textAlign: "center", marginBottom: 28 }}>
        <h2
          style={{
            fontSize: "1.5rem",
            fontWeight: 700,
            letterSpacing: "-0.025em",
            background: "var(--accent-gradient)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
            margin: 0,
          }}
        >
          What can I help you explore?
        </h2>
        <p
          style={{
            color: "var(--text-muted)",
            fontSize: "0.84rem",
            margin: "6px 0 0",
          }}
        >
          Ask a question or pick a suggestion
        </p>
      </div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 10,
        }}
      >
        {PROMPTS.map((item) => (
          <button
            key={item.prompt}
            onClick={() => onSelect(item.prompt)}
            style={{
              background: "var(--surface-elevated)",
              border: "1px solid var(--surface-border)",
              borderRadius: "var(--r-md)",
              padding: "14px 16px",
              fontSize: "0.82rem",
              fontWeight: 400,
              color: "var(--text-secondary)",
              cursor: "pointer",
              textAlign: "left",
              transition: "all 200ms ease",
              lineHeight: 1.4,
              display: "flex",
              alignItems: "flex-start",
              gap: 10,
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(0, 113, 227, 0.04)";
              e.currentTarget.style.borderColor = "rgba(0, 113, 227, 0.2)";
              e.currentTarget.style.color = "var(--text-primary)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "var(--surface-elevated)";
              e.currentTarget.style.borderColor = "var(--surface-border)";
              e.currentTarget.style.color = "var(--text-secondary)";
            }}
          >
            <span style={{ fontSize: "1.2rem", flexShrink: 0, lineHeight: 1 }}>
              {item.icon}
            </span>
            <span>{item.label}</span>
          </button>
        ))}
      </div>
    </div>
  );
}
