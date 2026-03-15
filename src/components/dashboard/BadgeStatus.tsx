"use client";

interface BadgeStatusProps {
  status: "positive" | "negative" | "warning" | "neutral" | "info";
  label: string;
}

const STYLES: Record<string, { color: string; bg: string }> = {
  positive: { color: "var(--green)", bg: "rgba(52, 199, 89, 0.1)" },
  negative: { color: "var(--red)", bg: "rgba(255, 59, 48, 0.1)" },
  warning: { color: "var(--orange)", bg: "rgba(255, 149, 0, 0.1)" },
  neutral: { color: "var(--text-muted)", bg: "var(--surface-border)" },
  info: { color: "var(--accent)", bg: "var(--accent-light)" },
};

export function BadgeStatus({ status, label }: BadgeStatusProps) {
  const s = STYLES[status] ?? STYLES.neutral;

  return (
    <span
      style={{
        display: "inline-block",
        fontSize: "0.7rem",
        fontWeight: 600,
        padding: "2px 10px",
        borderRadius: 20,
        color: s.color,
        background: s.bg,
        textTransform: "capitalize",
        whiteSpace: "nowrap",
      }}
    >
      {label}
    </span>
  );
}
