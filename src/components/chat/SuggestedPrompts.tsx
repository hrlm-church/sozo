"use client";

const PROMPTS = [
  "Build me an executive overview dashboard",
  "Show giving trends by month for the last 2 years",
  "Lifecycle stage breakdown with churn risk",
  "Top products and subscriptions by revenue",
  "Engagement activity across all 7 source systems",
  "Household health scores and giving trends",
];

interface SuggestedPromptsProps {
  onSelect: (prompt: string) => void;
}

export function SuggestedPrompts({ onSelect }: SuggestedPromptsProps) {
  return (
    <div style={{ padding: "24px 0" }}>
      <p style={{ color: "var(--text-muted)", fontSize: "0.85rem", marginBottom: 12 }}>
        Try asking:
      </p>
      <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
        {PROMPTS.map((prompt) => (
          <button
            key={prompt}
            onClick={() => onSelect(prompt)}
            className="chip-base chip-muted"
            style={{ cursor: "pointer", fontSize: "0.8rem" }}
          >
            {prompt}
          </button>
        ))}
      </div>
    </div>
  );
}
