"use client";

import Link from "next/link";

const DASHBOARDS = [
  { href: "/dashboards/briefing", label: "Executive Briefing", sub: "Morning overview with AI insights", color: "var(--accent)" },
  { href: "/dashboards/donor-health", label: "Donor Health", sub: "Lifecycle, retention & risk analysis", color: "var(--green)" },
  { href: "/dashboards/revenue", label: "Revenue Intelligence", sub: "Revenue streams, trends & funds", color: "var(--accent-secondary)" },
  { href: "/dashboards/subscriptions", label: "Subscriptions & MRR", sub: "Recurring revenue health", color: "var(--orange)" },
  { href: "/dashboards/events", label: "Events & Tours", sub: "Attendance, revenue & geography", color: "#E3A23C" },
  { href: "/dashboards/engagement", label: "Engagement & Signals", sub: "Tags, communications & signals", color: "#2E5FA7" },
  { href: "/dashboards/wealth", label: "Wealth & Capacity", sub: "Giving gap & upgrade opportunities", color: "#5BAE6A" },
  { href: "/dashboards/person", label: "Person 360", sub: "Individual donor profiles", color: "#C94C4C" },
  { href: "/dashboards/programs", label: "Program Comparison", sub: "True Girl, B2BB & more", color: "#F26522" },
  { href: "/dashboards/report", label: "Ministry Report", sub: "Monthly health score & grade", color: "#355E9D" },
];

export default function DashboardsHub() {
  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      <div style={{ marginBottom: 28 }}>
        <h1 style={{ fontSize: "1.5rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Dashboards
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Pre-built intelligence dashboards powered by your data
        </p>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill, minmax(280px, 1fr))", gap: 14 }}>
        {DASHBOARDS.map((d) => (
          <Link key={d.href} href={d.href} style={{ textDecoration: "none" }}>
            <div className="card-base" style={{ padding: "20px 22px", cursor: "pointer", borderTop: `3px solid ${d.color}`, minHeight: 90 }}>
              <div style={{ fontSize: "0.88rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: 4 }}>
                {d.label}
              </div>
              <div style={{ fontSize: "0.76rem", color: "var(--text-muted)", lineHeight: 1.4 }}>
                {d.sub}
              </div>
            </div>
          </Link>
        ))}
      </div>
    </div>
  );
}
