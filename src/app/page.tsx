"use client";

import { useState } from "react";
import { AppShell } from "@/components/layout/AppShell";
import { DashboardHeader } from "@/components/dashboard/DashboardHeader";
import { PerformanceCard } from "@/components/dashboard/PerformanceCard";
import { UtilizationCard } from "@/components/dashboard/UtilizationCard";
import { RecentPaymentsCard } from "@/components/dashboard/RecentPaymentsCard";
import { ScheduleRail } from "@/components/dashboard/ScheduleRail";
import { StatCard } from "@/components/ui/StatCard";
import {
  datePresets,
  kpiCards,
  navItems,
  payments,
  performanceBars,
  specialists,
} from "@/lib/mock/dashboard";
import { DashboardMetricTab, TimeRangePreset } from "@/types/dashboard";

export default function Home() {
  const [activeNav, setActiveNav] = useState("dashboard");
  const [searchQuery, setSearchQuery] = useState("");
  const [metricTab, setMetricTab] = useState<DashboardMetricTab>("specialists");
  const [selectedPreset, setSelectedPreset] =
    useState<TimeRangePreset>("last_week");
  const [period, setPeriod] = useState<"weekly" | "monthly">("weekly");

  return (
    <AppShell
      navItems={navItems}
      activeNav={activeNav}
      onNavChange={setActiveNav}
      searchQuery={searchQuery}
      onSearchQueryChange={setSearchQuery}
    >
      <DashboardHeader
        datePresets={datePresets}
        selectedPreset={selectedPreset}
        onPresetChange={setSelectedPreset}
        period={period}
        onPeriodChange={setPeriod}
      />

      <div className="grid grid-cols-1 gap-6 xl:grid-cols-[1.7fr_1fr]">
        <section className="space-y-6">
          <PerformanceCard
            metricTab={metricTab}
            onMetricTabChange={setMetricTab}
            bars={performanceBars}
          />

          <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
            {kpiCards.map((card) => (
              <StatCard key={card.label} data={card} />
            ))}
          </div>

          <div className="grid grid-cols-1 gap-6 lg:grid-cols-2">
            <UtilizationCard />
            <RecentPaymentsCard items={payments} />
          </div>
        </section>

        <ScheduleRail specialists={specialists} />
      </div>
    </AppShell>
  );
}
