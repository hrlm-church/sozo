import { KpiCardData } from "@/types/dashboard";

interface StatCardProps {
  data: KpiCardData;
}

export function StatCard({ data }: StatCardProps) {
  const trendColor =
    data.trend === "up"
      ? "text-emerald-500"
      : data.trend === "down"
        ? "text-rose-500"
        : "text-[var(--text-muted)]";

  return (
    <article className="card-base p-5">
      <p className="text-sm text-[var(--text-muted)]">{data.label}</p>
      <p className="mt-1 text-4xl font-semibold tracking-tight text-[var(--text-primary)]">
        {data.value}
      </p>
      {data.delta ? (
        <p className={`mt-2 text-sm font-medium ${trendColor}`}>{data.delta}</p>
      ) : null}
    </article>
  );
}
