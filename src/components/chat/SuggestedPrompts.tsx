"use client";

const PROMPTS = [
  {
    label: "Who are our most faithful givers and what's their journey?",
    desc: "Top donors, giving trends, and engagement history",
  },
  {
    label: "Show me donors we're at risk of losing",
    desc: "Lapsed or declining giving patterns to act on",
  },
  {
    label: "Build an executive giving dashboard",
    desc: "Total donations, monthly trends, fund breakdown",
  },
  {
    label: "How healthy is our donor pipeline?",
    desc: "New vs returning donors, lifecycle stages, retention",
  },
  {
    label: "What does our commerce and subscription revenue look like?",
    desc: "Orders, subscriptions, Stripe charges, WooCommerce",
  },
  {
    label: "Find people tagged with a specific interest and show their profile",
    desc: "Search across 5.7M tags from Keap, Mailchimp, and more",
  },
];

interface SuggestedPromptsProps {
  onSelect: (prompt: string) => void;
}

export function SuggestedPrompts({ onSelect }: SuggestedPromptsProps) {
  return (
    <div style={{ padding: "40px 0" }}>
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
              padding: "16px 18px",
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
