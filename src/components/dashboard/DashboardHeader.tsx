import { TimeRangePreset } from "@/types/dashboard";
import { Chip } from "@/components/ui/Chip";

interface DashboardHeaderProps {
  datePresets: { id: TimeRangePreset; label: string }[];
  selectedPreset: TimeRangePreset;
  onPresetChange: (preset: TimeRangePreset) => void;
  period: "weekly" | "monthly";
  onPeriodChange: (period: "weekly" | "monthly") => void;
}

export function DashboardHeader({
  datePresets,
  selectedPreset,
  onPresetChange,
  period,
  onPeriodChange,
}: DashboardHeaderProps) {
  return (
    <section className="mb-6 flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
      <h1 className="text-4xl font-bold tracking-tight text-[var(--text-primary)] md:text-6xl">
        Data Command Center
      </h1>

      <div className="flex flex-wrap items-center gap-3">
        <div className="card-base flex items-center gap-3 px-4 py-3">
          <span aria-hidden="true">📅</span>
          <span className="text-base font-medium text-[var(--text-primary)]">
            Snapshot: 19 Sep - 26 Sep, 2025
          </span>
          <span aria-hidden="true" className="text-[var(--text-muted)]">
            ▾
          </span>
        </div>

        <div className="card-base flex items-center gap-2 px-2 py-2">
          <button
            type="button"
            onClick={() => onPeriodChange("weekly")}
            className={`rounded-[var(--r-lg)] px-4 py-2 text-sm font-medium ${
              period === "weekly"
                ? "bg-[var(--chip-active-bg)] text-[var(--text-primary)]"
                : "text-[var(--text-muted)]"
            }`}
          >
            Weekly
          </button>
          <button
            type="button"
            onClick={() => onPeriodChange("monthly")}
            className={`rounded-[var(--r-lg)] px-4 py-2 text-sm font-medium ${
              period === "monthly"
                ? "bg-[var(--chip-active-bg)] text-[var(--text-primary)]"
                : "text-[var(--text-muted)]"
            }`}
          >
            Monthly
          </button>
        </div>

        <div className="hidden flex-wrap gap-2 lg:flex">
          {datePresets.map((preset) => (
            <Chip
              key={preset.id}
              label={preset.label}
              active={preset.id === selectedPreset}
              onClick={() => onPresetChange(preset.id)}
            />
          ))}
        </div>
      </div>
    </section>
  );
}
