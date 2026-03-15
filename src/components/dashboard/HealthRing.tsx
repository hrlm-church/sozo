"use client";

interface HealthRingProps {
  score: number;
  size?: "sm" | "md" | "lg";
  label?: string;
}

const sizes = { sm: 48, md: 72, lg: 120 };
const strokes = { sm: 4, md: 5, lg: 7 };
const fontSizes = { sm: "0.72rem", md: "1rem", lg: "1.6rem" };

export function HealthRing({ score, size = "md", label }: HealthRingProps) {
  const dim = sizes[size];
  const strokeWidth = strokes[size];
  const radius = (dim - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const clamped = Math.max(0, Math.min(100, score));
  const offset = circumference - (clamped / 100) * circumference;

  const color =
    clamped >= 80 ? "var(--green)" :
    clamped >= 60 ? "var(--orange)" :
    "var(--red)";

  return (
    <div style={{ display: "inline-flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
      <svg width={dim} height={dim} style={{ transform: "rotate(-90deg)" }}>
        <circle
          cx={dim / 2}
          cy={dim / 2}
          r={radius}
          fill="none"
          stroke="var(--surface-border)"
          strokeWidth={strokeWidth}
        />
        <circle
          cx={dim / 2}
          cy={dim / 2}
          r={radius}
          fill="none"
          stroke={color}
          strokeWidth={strokeWidth}
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          strokeLinecap="round"
          style={{ transition: "stroke-dashoffset 600ms ease" }}
        />
      </svg>
      <div style={{
        position: "relative",
        marginTop: -dim - 4,
        height: dim,
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
      }}>
        <span style={{
          fontSize: fontSizes[size],
          fontWeight: 700,
          color: "var(--text-primary)",
          letterSpacing: "-0.02em",
        }}>
          {Math.round(clamped)}
        </span>
      </div>
      {label && (
        <span style={{
          fontSize: "0.72rem",
          color: "var(--text-muted)",
          fontWeight: 500,
          marginTop: 2,
        }}>
          {label}
        </span>
      )}
    </div>
  );
}
