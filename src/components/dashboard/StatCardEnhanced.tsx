"use client";

import { ReactNode } from "react";

interface StatCardEnhancedProps {
  label: string;
  value: string;
  delta?: number;
  sparklineData?: number[];
  icon?: ReactNode;
  color?: string;
}

export function StatCardEnhanced({ label, value, delta, sparklineData, icon, color }: StatCardEnhancedProps) {
  const positive = (delta ?? 0) >= 0;

  return (
    <div className="card-base" style={{ padding: "18px 20px" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 8 }}>
        {icon && (
          <span style={{ color: color ?? "var(--accent)", fontSize: "1rem", flexShrink: 0 }}>
            {icon}
          </span>
        )}
        <span style={{
          fontSize: "0.72rem",
          color: "var(--text-muted)",
          fontWeight: 500,
          textTransform: "uppercase",
          letterSpacing: "0.04em",
        }}>
          {label}
        </span>
      </div>
      <div style={{ display: "flex", alignItems: "baseline", gap: 10 }}>
        <span style={{
          fontSize: "1.4rem",
          fontWeight: 700,
          color: color ?? "var(--text-primary)",
          letterSpacing: "-0.02em",
        }}>
          {value}
        </span>
        {delta !== undefined && delta !== null && delta !== 0 && (
          <span style={{
            fontSize: "0.72rem",
            fontWeight: 600,
            color: positive ? "var(--green)" : "var(--red)",
            background: positive ? "rgba(52, 199, 89, 0.1)" : "rgba(255, 59, 48, 0.1)",
            padding: "2px 8px",
            borderRadius: 20,
          }}>
            {positive ? "+" : ""}{delta.toFixed(1)}%
          </span>
        )}
      </div>
      {sparklineData && sparklineData.length > 1 && (
        <MiniSparkline data={sparklineData} color={color ?? "var(--accent)"} />
      )}
    </div>
  );
}

function MiniSparkline({ data, color }: { data: number[]; color: string }) {
  const width = 120;
  const height = 28;
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  const step = width / (data.length - 1);
  const points = data
    .map((v, i) => `${i * step},${height - ((v - min) / range) * height}`)
    .join(" ");

  return (
    <svg width={width} height={height} style={{ marginTop: 8, display: "block" }}>
      <polyline
        points={points}
        fill="none"
        stroke={color}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
    </svg>
  );
}
