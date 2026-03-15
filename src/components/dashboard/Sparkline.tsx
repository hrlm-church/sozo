"use client";

interface SparklineProps {
  data: number[];
  color?: string;
  width?: number;
  height?: number;
}

export function Sparkline({ data, color, width = 64, height = 20 }: SparklineProps) {
  if (!data || data.length < 2) return null;

  const trending = data[data.length - 1] >= data[0];
  const stroke = color ?? (trending ? "var(--green)" : "var(--red)");

  const min = Math.min(...data);
  const max = Math.max(...data);
  const range = max - min || 1;
  const pad = 2;

  const points = data
    .map((v, i) => {
      const x = pad + (i / (data.length - 1)) * (width - pad * 2);
      const y = height - pad - ((v - min) / range) * (height - pad * 2);
      return `${x},${y}`;
    })
    .join(" ");

  const first = points.split(" ")[0].split(",");
  const last = points.split(" ").slice(-1)[0].split(",");

  return (
    <svg width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
      <polyline
        points={points}
        fill="none"
        stroke={stroke}
        strokeWidth={1.5}
        strokeLinecap="round"
        strokeLinejoin="round"
      />
      <circle cx={first[0]} cy={first[1]} r={1.5} fill={stroke} />
      <circle cx={last[0]} cy={last[1]} r={1.5} fill={stroke} />
    </svg>
  );
}
