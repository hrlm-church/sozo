"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { StatCardEnhanced } from "@/components/dashboard/StatCardEnhanced";
import { HealthRing } from "@/components/dashboard/HealthRing";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { TrendChart } from "@/components/dashboard/TrendChart";
import { BadgeStatus } from "@/components/dashboard/BadgeStatus";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface TagItem {
  tag_name: string;
  tag_group: string;
}

interface EventItem {
  event_name: string;
  event_date: string;
}

interface Subscription {
  plan_name: string;
  status: string;
  amount: number;
  start_date: string;
}

interface PersonDetail {
  person_id: string;
  display_name: string;
  email: string;
  lifecycle_stage: string;
  total_given: number;
  avg_gift: number;
  gift_count: number;
  days_since_last: number;
  risk_score?: number;
  giving_timeline?: { date: string; value: number }[];
  tags?: TagItem[];
  events?: EventItem[];
  subscriptions?: Subscription[];
  wealth?: {
    giving_capacity_label?: string;
    estimated_capacity?: number;
  };
}

export default function PersonDetailPage() {
  const params = useParams();
  const id = params?.id as string;
  const [data, setData] = useState<PersonDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!id) return;
    fetch(`/api/people/${id}`)
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, [id]);

  if (loading) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Loading person profile..." loading />
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <Link href="/people" style={{ fontSize: "0.82rem", color: "var(--accent)", textDecoration: "none", fontWeight: 500 }}>
          &larr; Back to search
        </Link>
        <div style={{ marginTop: 20 }}>
          <EmptyState message="Person not found" />
        </div>
      </div>
    );
  }

  const stageStatus = (stage: string): "positive" | "negative" | "warning" | "neutral" | "info" => {
    const lower = (stage ?? "").toLowerCase();
    if (lower.includes("active") || lower.includes("engaged")) return "positive";
    if (lower.includes("lapsed") || lower.includes("lost")) return "negative";
    if (lower.includes("at risk") || lower.includes("declining")) return "warning";
    if (lower.includes("new") || lower.includes("prospect")) return "info";
    return "neutral";
  };

  // Group tags by tag_group
  const tagGroups: Record<string, string[]> = {};
  (data.tags ?? []).forEach((t) => {
    const group = t.tag_group || "Other";
    if (!tagGroups[group]) tagGroups[group] = [];
    tagGroups[group].push(t.tag_name);
  });

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Back link */}
      <Link href="/people" style={{ fontSize: "0.82rem", color: "var(--accent)", textDecoration: "none", fontWeight: 500 }}>
        &larr; Back to search
      </Link>

      {/* Profile Header */}
      <div style={{ marginTop: 16, marginBottom: 28 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div>
            <h1 style={{ fontSize: "1.6rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
              {data.display_name}
            </h1>
            <div style={{ display: "flex", alignItems: "center", gap: 10, marginTop: 6 }}>
              {data.email && (
                <span style={{ fontSize: "0.84rem", color: "var(--text-muted)" }}>{data.email}</span>
              )}
              {data.lifecycle_stage && (
                <BadgeStatus
                  status={stageStatus(data.lifecycle_stage)}
                  label={data.lifecycle_stage}
                />
              )}
            </div>
          </div>
        </div>
      </div>

      {/* KPI Row */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 20 }}>
        <StatCardEnhanced
          label="Total Given"
          value={`$${(data.total_given ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
        />
        <StatCardEnhanced
          label="Avg Gift"
          value={`$${(data.avg_gift ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
        />
        <StatCardEnhanced
          label="Gift Count"
          value={(data.gift_count ?? 0).toLocaleString()}
        />
        <StatCardEnhanced
          label="Days Since Last"
          value={(data.days_since_last ?? 0).toLocaleString()}
          color={data.days_since_last > 365 ? "var(--red)" : data.days_since_last > 180 ? "var(--orange)" : undefined}
        />
      </div>

      {/* Risk Score */}
      {data.risk_score !== undefined && data.risk_score !== null && (
        <div style={{ display: "flex", alignItems: "center", gap: 20, marginBottom: 28 }}>
          <HealthRing score={Math.max(0, 100 - data.risk_score * 100)} size="md" label="Health" />
          <div>
            <div style={{ fontSize: "0.84rem", color: "var(--text-secondary)" }}>
              Risk Score: <strong>{data.risk_score.toFixed(2)}</strong>
            </div>
            <div style={{ fontSize: "0.78rem", color: "var(--text-muted)", marginTop: 2 }}>
              {data.risk_score >= 0.7 ? "High risk — may be lapsing" :
               data.risk_score >= 0.4 ? "Moderate risk — monitor closely" :
               "Low risk — healthy engagement"}
            </div>
          </div>
        </div>
      )}

      {/* Giving Timeline */}
      {(data.giving_timeline ?? []).length > 0 && (
        <>
          <SectionHeader title="Giving Timeline" />
          <TrendChart
            data={data.giving_timeline!}
            color="var(--green)"
            valueFormatter={(v) => `$${v.toLocaleString()}`}
          />
        </>
      )}

      {/* Tags & Events two-column */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
        {/* Tags & Signals */}
        <div>
          <SectionHeader title="Tags & Signals" />
          {Object.keys(tagGroups).length > 0 ? (
            <div className="card-base" style={{ padding: "16px 20px" }}>
              {Object.entries(tagGroups).map(([group, tags]) => (
                <div key={group} style={{ marginBottom: 14 }}>
                  <div style={{ fontSize: "0.72rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 6 }}>
                    {group}
                  </div>
                  <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
                    {tags.map((tag) => (
                      <span key={tag} style={{
                        fontSize: "0.74rem",
                        fontWeight: 500,
                        padding: "4px 10px",
                        borderRadius: 20,
                        background: "var(--accent-light)",
                        color: "var(--accent)",
                      }}>
                        {tag}
                      </span>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState message="No tags found" />
          )}
        </div>

        {/* Events Attended */}
        <div>
          <SectionHeader title="Events Attended" />
          {(data.events ?? []).length > 0 ? (
            <div className="card-base" style={{ padding: "16px 20px" }}>
              {data.events!.map((evt, i) => (
                <div key={i} style={{
                  display: "flex",
                  justifyContent: "space-between",
                  padding: "8px 0",
                  borderBottom: i < data.events!.length - 1 ? "1px solid var(--surface-border)" : "none",
                }}>
                  <span style={{ fontSize: "0.84rem", color: "var(--text-secondary)" }}>
                    {evt.event_name}
                  </span>
                  <span style={{ fontSize: "0.78rem", color: "var(--text-muted)" }}>
                    {evt.event_date ? new Date(evt.event_date).toLocaleDateString() : ""}
                  </span>
                </div>
              ))}
            </div>
          ) : (
            <EmptyState message="No events attended" />
          )}
        </div>
      </div>

      {/* Subscriptions */}
      {(data.subscriptions ?? []).length > 0 && (
        <>
          <SectionHeader title="Subscriptions" />
          <div className="card-base" style={{ overflow: "hidden" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
              <thead>
                <tr style={{ borderBottom: "1px solid var(--surface-border)" }}>
                  <th style={{ textAlign: "left", padding: "10px 14px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Plan</th>
                  <th style={{ textAlign: "left", padding: "10px 14px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Status</th>
                  <th style={{ textAlign: "right", padding: "10px 14px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Amount</th>
                  <th style={{ textAlign: "right", padding: "10px 14px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Start Date</th>
                </tr>
              </thead>
              <tbody>
                {data.subscriptions!.map((sub, i) => (
                  <tr key={i} style={{ borderBottom: "1px solid var(--surface-border)" }}>
                    <td style={{ padding: "10px 14px", fontWeight: 500, color: "var(--text-primary)" }}>{sub.plan_name}</td>
                    <td style={{ padding: "10px 14px" }}>
                      <BadgeStatus
                        status={sub.status === "active" ? "positive" : sub.status === "cancelled" ? "negative" : "neutral"}
                        label={sub.status}
                      />
                    </td>
                    <td style={{ padding: "10px 14px", textAlign: "right", fontWeight: 600, color: "var(--text-primary)" }}>
                      ${(sub.amount ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}
                    </td>
                    <td style={{ padding: "10px 14px", textAlign: "right", color: "var(--text-muted)", fontSize: "0.78rem" }}>
                      {sub.start_date ? new Date(sub.start_date).toLocaleDateString() : "-"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}

      {/* Wealth Screening */}
      {data.wealth && (data.wealth.giving_capacity_label || data.wealth.estimated_capacity) && (
        <>
          <SectionHeader title="Wealth Screening" />
          <div className="card-base" style={{ padding: "20px 24px" }}>
            <div style={{ display: "flex", alignItems: "baseline", gap: 16 }}>
              {data.wealth.giving_capacity_label && (
                <div>
                  <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 4 }}>
                    Capacity Label
                  </div>
                  <div style={{ fontSize: "1.1rem", fontWeight: 700, color: "var(--text-primary)" }}>
                    {data.wealth.giving_capacity_label}
                  </div>
                </div>
              )}
              {data.wealth.estimated_capacity !== undefined && (
                <div>
                  <div style={{ fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 4 }}>
                    Estimated Capacity
                  </div>
                  <div style={{ fontSize: "1.1rem", fontWeight: 700, color: "var(--accent)" }}>
                    ${data.wealth.estimated_capacity.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                  </div>
                </div>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  );
}
