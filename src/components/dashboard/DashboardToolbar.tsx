"use client";

import { useDashboardStore } from "@/lib/stores/dashboard-store";
import { useSession, signOut } from "next-auth/react";
import { useState } from "react";

export function DashboardToolbar() {
  const { data: session } = useSession();
  const name = useDashboardStore((s) => s.name);
  const setName = useDashboardStore((s) => s.setName);
  const dirty = useDashboardStore((s) => s.dirty);
  const widgets = useDashboardStore((s) => s.widgets);
  const clearDashboard = useDashboardStore((s) => s.clearDashboard);
  const [saving, setSaving] = useState(false);
  const dashboardId = useDashboardStore((s) => s.dashboardId);
  const markSaved = useDashboardStore((s) => s.markSaved);
  const layouts = useDashboardStore((s) => s.layouts);

  const handleSave = async () => {
    setSaving(true);
    try {
      const resp = await fetch("/api/dashboard/save", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: dashboardId, name, widgets, layouts }),
      });
      if (resp.ok) {
        const data = await resp.json();
        markSaved(data.id);
      }
    } finally {
      setSaving(false);
    }
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: 14,
        padding: "12px 24px",
        borderBottom: "1px solid var(--surface-border)",
        background: "var(--surface)",
        flexShrink: 0,
      }}
    >
      <input
        value={name}
        onChange={(e) => setName(e.target.value)}
        style={{
          background: "transparent",
          border: "none",
          fontSize: "1rem",
          fontWeight: 600,
          color: "var(--text-primary)",
          outline: "none",
          flex: 1,
          minWidth: 0,
          letterSpacing: "-0.02em",
        }}
      />

      {dirty && (
        <span style={{
          fontSize: "0.68rem", color: "var(--text-muted)", fontWeight: 500,
        }}>
          Edited
        </span>
      )}

      <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
        {widgets.length} widget{widgets.length !== 1 ? "s" : ""}
      </span>

      <button
        onClick={handleSave}
        disabled={saving || widgets.length === 0}
        className="gradient-btn"
        style={{ padding: "7px 18px", fontSize: "0.82rem" }}
      >
        {saving ? "Saving..." : "Save"}
      </button>

      <button onClick={clearDashboard} className="btn-secondary">
        Clear
      </button>

      <div style={{ width: 1, height: 18, background: "var(--surface-border-strong)", flexShrink: 0 }} />

      {session?.user && (
        <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", whiteSpace: "nowrap" }}>
          {session.user.name ?? session.user.email}
        </span>
      )}
      <button onClick={() => signOut()} className="btn-secondary" style={{ color: "var(--text-muted)" }}>
        Sign out
      </button>
    </div>
  );
}
