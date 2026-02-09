import { SpecialistItem } from "@/types/dashboard";
import { GradientButton } from "@/components/ui/GradientButton";

interface ScheduleRailProps {
  specialists: SpecialistItem[];
}

export function ScheduleRail({ specialists }: ScheduleRailProps) {
  return (
    <section className="card-base p-6">
      <div className="mb-4 flex items-center justify-between">
        <h3 className="section-title">Priority Households</h3>
        <button type="button" className="chip-base chip-muted">
          Priority score ▾
        </button>
      </div>

      <div className="space-y-3">
        {specialists.map((item) => (
          <article key={item.id} className="card-base flex items-center justify-between gap-3 p-3">
            <div className="min-w-0">
              <p className="truncate text-lg font-semibold text-[var(--text-primary)]">{item.name}</p>
              <p className="text-sm text-[var(--text-muted)]">
                ⭐ {item.rating} • {item.role} • {item.distanceMi} risk index
              </p>
            </div>
            <div className="flex items-center gap-2">
              <button type="button" className="chip-base chip-muted">
                • {item.slots} open issues
              </button>
              <GradientButton label="Open profile" />
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}
