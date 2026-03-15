"use client";

interface FunnelChartProps {
  stages: { label: string; value: number; color?: string }[];
}

export function FunnelChart({ stages }: FunnelChartProps) {
  if (!stages || stages.length === 0) return null;
  const max = Math.max(...stages.map((s) => s.value));

  return (
    <div className="card-base" style={{ padding: "20px 24px" }}>
      <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
        {stages.map((stage, i) => {
          const pct = max > 0 ? (stage.value / max) * 100 : 0;
          const color = stage.color ?? `hsl(${220 - i * 30}, 65%, 55%)`;
          return (
            <div key={i}>
              <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
                <span style={{ fontSize: "0.8rem", color: "var(--text-secondary)", fontWeight: 500 }}>
                  {stage.label}
                </span>
                <span style={{ fontSize: "0.8rem", fontWeight: 600, color: "var(--text-primary)" }}>
                  {stage.value.toLocaleString()}
                </span>
              </div>
              <div style={{ height: 8, borderRadius: 4, background: "var(--surface-border)", overflow: "hidden" }}>
                <div style={{
                  width: `${pct}%`,
                  height: "100%",
                  borderRadius: 4,
                  background: color,
                  transition: "width 400ms ease",
                }} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
