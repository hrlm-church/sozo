import { PaymentItem } from "@/types/dashboard";
import { AvatarRow } from "@/components/ui/AvatarRow";

interface RecentPaymentsCardProps {
  items: PaymentItem[];
}

export function RecentPaymentsCard({ items }: RecentPaymentsCardProps) {
  return (
    <section className="card-base p-6">
      <h3 className="section-title">Recent Insight Runs</h3>

      <div className="mt-5 space-y-3">
        {items.map((item) => (
          <article key={item.id} className="card-base flex items-center justify-between p-3">
            <AvatarRow name={item.name} subtitle={`${item.time} • ${item.role}`} />
            <div className="text-right">
              <p className="text-3xl font-semibold tracking-tight text-[var(--text-primary)]">
                +{item.amount}
              </p>
              <p className="text-sm text-[var(--text-muted)]">Signal score</p>
            </div>
          </article>
        ))}
      </div>
    </section>
  );
}
