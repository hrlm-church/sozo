import { DashboardMetricTab } from "@/types/dashboard";
import { SegmentedControl } from "@/components/ui/SegmentedControl";

interface PerformanceCardProps {
  metricTab: DashboardMetricTab;
  onMetricTabChange: (tab: DashboardMetricTab) => void;
  bars: number[];
}

const metricItems: { id: DashboardMetricTab; label: string }[] = [
  { id: "revenue", label: "Revenue" },
  { id: "patients", label: "Profiles" },
  { id: "specialists", label: "Households" },
];

export function PerformanceCard({
  metricTab,
  onMetricTabChange,
  bars,
}: PerformanceCardProps) {
  const axisLabels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul"];

  return (
    <section className="card-base p-6">
      <div className="mb-6 flex flex-wrap items-center justify-between gap-3">
        <h2 className="section-title">Data Signal Overview</h2>
        <SegmentedControl
          items={metricItems}
          value={metricTab}
          onChange={onMetricTabChange}
        />
      </div>

      <div className="grid h-72 grid-cols-7 items-end gap-4 rounded-[var(--r-lg)] border border-[var(--surface-border)] bg-white p-4">
        {bars.map((bar, index) => (
          <div key={index} className="flex h-full flex-col justify-end gap-3">
            <div className="w-full rounded-[var(--r-lg)] bg-[var(--bar-muted)]" style={{ height: `${bar}%` }}>
              <div
                className="w-full rounded-[var(--r-lg)] bg-[var(--accent-gradient)]"
                style={{ height: `${Math.max(12, bar - 8)}%` }}
              />
            </div>
            <div className="mx-auto inline-flex size-8 items-center justify-center rounded-full bg-[var(--chip-active-bg)] text-[11px] font-semibold text-[var(--text-muted)]">
              {axisLabels[index]}
            </div>
          </div>
        ))}
      </div>
    </section>
  );
}
