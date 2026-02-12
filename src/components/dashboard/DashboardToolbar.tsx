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
        body: JSON.stringify({
          id: dashboardId,
          name,
          widgets,
          layouts,
        }),
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
        gap: 12,
        padding: "12px 20px",
        borderBottom: "1px solid var(--surface-border)",
        background: "var(--surface-strong)",
        flexShrink: 0,
      }}
    >
      <input
        value={name}
        onChange={(e) => setName(e.target.value)}
        style={{
          background: "transparent",
          border: "none",
          fontSize: "1.05rem",
          fontWeight: 650,
          color: "var(--text-primary)",
          outline: "none",
          flex: 1,
          minWidth: 0,
        }}
      />

      {dirty && (
        <span style={{ fontSize: "0.7rem", color: "var(--text-muted)" }}>
          unsaved
        </span>
      )}

      <span style={{ fontSize: "0.75rem", color: "var(--text-muted)" }}>
        {widgets.length} widget{widgets.length !== 1 ? "s" : ""}
      </span>

      <button
        onClick={handleSave}
        disabled={saving || widgets.length === 0}
        className="gradient-btn"
        style={{ padding: "6px 16px", fontSize: "0.8rem" }}
      >
        {saving ? "Saving..." : "Save"}
      </button>

      <button
        onClick={clearDashboard}
        style={{
          background: "none",
          border: "1px solid var(--surface-border)",
          borderRadius: "var(--r-sm)",
          padding: "6px 12px",
          fontSize: "0.8rem",
          color: "var(--text-muted)",
          cursor: "pointer",
        }}
      >
        Clear
      </button>

      {/* Spacer */}
      <div style={{ flex: "0 0 1px", height: 20, background: "var(--surface-border)", margin: "0 4px" }} />

      {/* User info + sign out */}
      {session?.user && (
        <span style={{ fontSize: "0.75rem", color: "var(--text-muted)", whiteSpace: "nowrap" }}>
          {session.user.name ?? session.user.email}
        </span>
      )}
      <button
        onClick={() => signOut()}
        style={{
          background: "none",
          border: "1px solid var(--surface-border)",
          borderRadius: "var(--r-sm)",
          padding: "6px 12px",
          fontSize: "0.8rem",
          color: "var(--text-muted)",
          cursor: "pointer",
          whiteSpace: "nowrap",
        }}
      >
        Sign out
      </button>
    </div>
  );
}
