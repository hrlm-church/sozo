"use client";

const PROMPTS = [
  "Build me an executive giving dashboard with total donations, top donors, and monthly trends",
  "Who are our top 20 donors? Show me their giving by month over the last 2 years",
  "Break down donations by fund — which funds bring in the most?",
  "How many people do we have vs actual donors? Show me the lifecycle stages",
  "Show commerce overview — total orders, payments, and active subscriptions",
  "Which source systems contribute the most data? Compare Keap, Donor Direct, Bloomerang, and the rest",
];

interface SuggestedPromptsProps {
  onSelect: (prompt: string) => void;
}

export function SuggestedPrompts({ onSelect }: SuggestedPromptsProps) {
  return (
    <div style={{ padding: "32px 0" }}>
      <div style={{ textAlign: "center", marginBottom: 28 }}>
        <h2 style={{
          fontSize: "1.5rem", fontWeight: 700, letterSpacing: "-0.025em",
          background: "var(--accent-gradient)", WebkitBackgroundClip: "text",
          WebkitTextFillColor: "transparent", margin: 0,
        }}>
          What can I help you explore?
        </h2>
        <p style={{ color: "var(--text-muted)", fontSize: "0.84rem", margin: "6px 0 0" }}>
          Ask a question or pick a suggestion
        </p>
      </div>
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {PROMPTS.map((prompt) => (
          <button
            key={prompt}
            onClick={() => onSelect(prompt)}
            style={{
              background: "transparent",
              border: "1px solid transparent",
              borderRadius: "var(--r-md)",
              padding: "12px 14px",
              fontSize: "0.82rem",
              fontWeight: 400,
              color: "var(--text-secondary)",
              cursor: "pointer",
              textAlign: "left",
              transition: "all 150ms ease",
              lineHeight: 1.4,
            }}
            onMouseEnter={(e) => { e.currentTarget.style.background = "var(--accent-light)"; e.currentTarget.style.borderColor = "var(--surface-border-strong)"; e.currentTarget.style.color = "var(--accent)"; }}
            onMouseLeave={(e) => { e.currentTarget.style.background = "transparent"; e.currentTarget.style.borderColor = "transparent"; e.currentTarget.style.color = "var(--text-secondary)"; }}
          >
            {prompt}
          </button>
        ))}
      </div>
    </div>
  );
}
