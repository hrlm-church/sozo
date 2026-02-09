import { AvatarRow } from "@/components/ui/AvatarRow";

export function UtilizationCard() {
  return (
    <section className="card-base p-6">
      <h3 className="section-title">Data Coverage</h3>

      <div className="mt-5 rounded-[var(--r-lg)] bg-[var(--chip-active-bg)] p-4">
        <div className="h-14 rounded-full bg-white p-1 shadow-[var(--shadow-soft)]">
          <div className="flex h-full w-[80%] items-center justify-end rounded-full bg-[var(--accent-gradient)] px-4 text-lg font-semibold text-white">
            80%
          </div>
        </div>
        <div className="mt-3 flex items-center justify-between text-sm text-[var(--text-muted)]">
          <span>Mapped entities</span>
          <span>Unmapped entities</span>
        </div>
      </div>

      <div className="mt-5 space-y-3">
        <div className="card-base flex items-center justify-between p-3">
          <AvatarRow name="Keap → Person Graph" subtitle="83,061 emails mapped" />
          <span className="text-[var(--text-muted)]">⋯</span>
        </div>
        <div className="card-base flex items-center justify-between p-3">
          <AvatarRow name="Donor Direct → Person Graph" subtitle="5,048 emails linked" />
          <span className="text-[var(--text-muted)]">⋯</span>
        </div>
      </div>
    </section>
  );
}
