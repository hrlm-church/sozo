"use client";

import { useEffect, useState } from "react";
import { StatCardEnhanced } from "@/components/dashboard/StatCardEnhanced";
import { SectionHeader } from "@/components/dashboard/SectionHeader";
import { DataTable } from "@/components/dashboard/DataTable";
import { DonutChart } from "@/components/dashboard/DonutChart";
import { HealthRing } from "@/components/dashboard/HealthRing";
import { EmptyState } from "@/components/dashboard/EmptyState";

interface EventsData {
  summary?: {
    total_events?: number;
    total_attendees?: number;
    total_revenue?: number;
    avg_ticket_price?: number;
    overall_checkin_rate?: number;
  };
  leaderboard?: Record<string, unknown>[];
  ticket_types?: { name: string; value: number; color: string }[];
  top_states?: Record<string, unknown>[];
}

export default function EventsDashboardPage() {
  const [data, setData] = useState<EventsData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboards/events")
      .then((r) => r.json())
      .then(setData)
      .catch(() => setData(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Loading events data..." loading />
      </div>
    );
  }

  if (!data) {
    return (
      <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
        <EmptyState message="Unable to load events data" />
      </div>
    );
  }

  const s = data.summary ?? {};
  const checkinRate = s.overall_checkin_rate ?? 0;

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Events &amp; Tours
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Event performance and attendance analytics
        </p>
      </div>

      {/* KPI Cards */}
      <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 12, marginBottom: 28 }}>
        <StatCardEnhanced
          label="Total Events"
          value={(s.total_events ?? 0).toLocaleString()}
        />
        <StatCardEnhanced
          label="Total Attendees"
          value={(s.total_attendees ?? 0).toLocaleString()}
        />
        <StatCardEnhanced
          label="Total Revenue"
          value={`$${(s.total_revenue ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}`}
        />
        <StatCardEnhanced
          label="Avg Ticket Price"
          value={`$${(s.avg_ticket_price ?? 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`}
        />
      </div>

      {/* Event Leaderboard */}
      <SectionHeader title="Event Leaderboard" />
      <DataTable
        columns={[
          { key: "event_name", label: "Event" },
          { key: "attendee_count", label: "Attendees", align: "right", format: "number" },
          { key: "total_revenue", label: "Revenue", align: "right", format: "currency" },
          { key: "avg_price", label: "Avg Price", align: "right", format: "currency" },
          { key: "checkin_rate", label: "Check-in Rate", align: "right", format: "percent" },
        ]}
        data={data.leaderboard ?? []}
      />

      {/* Ticket Types + Top States */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginTop: 28 }}>
        <div>
          <SectionHeader title="Ticket Types" />
          <div className="card-base" style={{ padding: 20 }}>
            <DonutChart data={data.ticket_types ?? []} />
          </div>
        </div>
        <div>
          <SectionHeader title="Top States by Attendance" />
          <DataTable
            columns={[
              { key: "state", label: "State" },
              { key: "attendee_count", label: "Attendees", align: "right", format: "number" },
            ]}
            data={data.top_states ?? []}
            maxRows={10}
          />
        </div>
      </div>

      {/* Check-in Rate */}
      <SectionHeader title="Check-in Rate" />
      <div className="card-base" style={{ padding: "32px 40px", display: "flex", alignItems: "center", gap: 32 }}>
        <HealthRing score={checkinRate} size="lg" label="Check-in %" />
        <div>
          <div style={{ fontSize: "2rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.02em" }}>
            {checkinRate.toFixed(1)}%
          </div>
          <div style={{ fontSize: "0.84rem", color: "var(--text-muted)", marginTop: 4 }}>
            Overall check-in rate across all events
          </div>
        </div>
      </div>
    </div>
  );
}
