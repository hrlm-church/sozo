"use client";

import {
  type ComponentProps,
  createContext,
  useContext,
  useId,
  useMemo,
} from "react";
import {
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  Legend as RechartsLegend,
} from "recharts";

export type ChartConfig = Record<
  string,
  { label: string; color: string }
>;

const ChartContext = createContext<ChartConfig>({});
export const useChart = () => useContext(ChartContext);

interface ChartContainerProps extends ComponentProps<"div"> {
  config: ChartConfig;
  children: ComponentProps<typeof ResponsiveContainer>["children"];
}

export function ChartContainer({
  config,
  children,
  className,
  ...props
}: ChartContainerProps) {
  const chartId = useId();

  const cssVars = useMemo(() => {
    const vars: Record<string, string> = {};
    for (const [key, value] of Object.entries(config)) {
      vars[`--color-${key}`] = value.color;
    }
    return vars;
  }, [config]);

  return (
    <ChartContext.Provider value={config}>
      <div
        data-chart={chartId}
        className={className}
        style={{ ...cssVars, width: "100%", minHeight: 200 }}
        {...props}
      >
        <ResponsiveContainer width="100%" height="100%">
          {children}
        </ResponsiveContainer>
      </div>
    </ChartContext.Provider>
  );
}

function CustomTooltipContent({ active, payload, label }: {
  active?: boolean;
  payload?: Array<{ name: string; value: number; color: string }>;
  label?: string;
}) {
  if (!active || !payload?.length) return null;

  return (
    <div style={{
      background: "rgba(22,26,42,0.92)", backdropFilter: "blur(8px)",
      border: "1px solid rgba(255,255,255,0.08)",
      borderRadius: 10, padding: "10px 14px",
      boxShadow: "0 8px 32px rgba(0,0,0,0.24)",
    }}>
      {label && (
        <div style={{ fontSize: "0.72rem", color: "rgba(255,255,255,0.6)", marginBottom: 6, fontWeight: 600 }}>
          {label}
        </div>
      )}
      {payload.map((entry, i) => (
        <div key={i} style={{ display: "flex", alignItems: "center", gap: 8, padding: "2px 0" }}>
          <div style={{
            width: 8, height: 8, borderRadius: "50%", background: entry.color, flexShrink: 0,
          }} />
          <span style={{ fontSize: "0.78rem", color: "rgba(255,255,255,0.7)", minWidth: 60 }}>
            {entry.name}
          </span>
          <span style={{ fontSize: "0.78rem", color: "#fff", fontWeight: 700, marginLeft: "auto" }}>
            {typeof entry.value === "number" ? entry.value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : entry.value}
          </span>
        </div>
      ))}
    </div>
  );
}

export function ChartTooltip(props: ComponentProps<typeof RechartsTooltip>) {
  return (
    <RechartsTooltip
      cursor={{ fill: "rgba(111,67,234,0.06)" }}
      content={<CustomTooltipContent />}
      {...props}
    />
  );
}

export function ChartLegend(props: ComponentProps<typeof RechartsLegend>) {
  return (
    <RechartsLegend
      wrapperStyle={{ fontSize: "0.78rem", paddingTop: 10 }}
      {...props}
    />
  );
}
