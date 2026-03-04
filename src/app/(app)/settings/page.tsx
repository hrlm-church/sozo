"use client";

import { useSession } from "next-auth/react";
import Link from "next/link";

export default function SettingsPage() {
  const { data: session } = useSession();

  return (
    <div style={{ padding: "32px 40px", maxWidth: 800 }}>
      <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: "0 0 4px" }}>
        Settings
      </h1>
      <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "0 0 32px" }}>
        Manage your workspace preferences
      </p>

      {/* Account section */}
      <Section title="Account">
        <div className="card-base" style={{ padding: "20px 24px" }}>
          <Row label="Name" value={session?.user?.name ?? "N/A"} />
          <Row label="Email" value={session?.user?.email ?? "N/A"} />
        </div>
      </Section>

      {/* Navigation to sub-pages */}
      <Section title="Organization">
        <Link href="/settings/users" style={{ textDecoration: "none", display: "block" }}>
          <div className="card-base" style={{ padding: "16px 24px", display: "flex", alignItems: "center", justifyContent: "space-between", cursor: "pointer" }}>
            <div>
              <div style={{ fontSize: "0.88rem", fontWeight: 600, color: "var(--text-primary)" }}>
                Team Members
              </div>
              <div style={{ fontSize: "0.76rem", color: "var(--text-muted)", marginTop: 2 }}>
                Invite members, manage roles and access
              </div>
            </div>
            <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
              <path d="M6 4L10 8L6 12" stroke="var(--text-muted)" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          </div>
        </Link>
      </Section>

      {/* About */}
      <Section title="About">
        <div className="card-base" style={{ padding: "20px 24px" }}>
          <Row label="Platform" value="Sozo Intelligence Platform" />
          <Row label="Organization" value="Pure Freedom Ministries" last />
        </div>
      </Section>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <section style={{ marginBottom: 28 }}>
      <h2 style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.05em", marginBottom: 10 }}>
        {title}
      </h2>
      {children}
    </section>
  );
}

function Row({ label, value, last }: { label: string; value: string; last?: boolean }) {
  return (
    <div style={{
      display: "flex",
      justifyContent: "space-between",
      alignItems: "center",
      padding: "10px 0",
      borderBottom: last ? "none" : "1px solid var(--surface-border)",
    }}>
      <span style={{ fontSize: "0.84rem", color: "var(--text-secondary)" }}>{label}</span>
      <span style={{ fontSize: "0.84rem", color: "var(--text-primary)", fontWeight: 500 }}>{value}</span>
    </div>
  );
}
