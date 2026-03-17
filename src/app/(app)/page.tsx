"use client";

import { useState } from "react";
import { ChatPanel } from "@/components/chat/ChatPanel";
import { DashboardCanvas } from "@/components/dashboard/DashboardCanvas";
import { DashboardToolbar } from "@/components/dashboard/DashboardToolbar";
import { useDashboardStore } from "@/lib/stores/dashboard-store";

export default function Home() {
  const [chatOpen, setChatOpen] = useState(true);
  const widgets = useDashboardStore((s) => s.widgets);
  const hasWidgets = widgets.length > 0;

  return (
    <div style={{ display: "flex", height: "100%", overflow: "hidden" }}>
      {chatOpen && (
        <div style={{ flex: hasWidgets ? "0 0 660px" : 1, display: "flex", flexDirection: "column", height: "100%", minWidth: 0, transition: "flex 200ms ease" }}>
          <ChatPanel />
        </div>
      )}

      {(hasWidgets || !chatOpen) && (
        <div style={{ flex: 1, display: "flex", flexDirection: "column", height: "100%", minWidth: 0 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 0 }}>
            <button
              onClick={() => setChatOpen(!chatOpen)}
              title={chatOpen ? "Hide chat" : "Show chat"}
              style={{
                background: "var(--surface)",
                border: "none",
                borderBottom: "1px solid var(--surface-border)",
                borderRight: "1px solid var(--surface-border)",
                padding: "13px 16px",
                cursor: "pointer",
                fontSize: "0.9rem",
                color: "var(--text-muted)",
                flexShrink: 0,
                transition: "color 150ms ease",
              }}
              onMouseEnter={(e) => { e.currentTarget.style.color = "#0693e3"; }}
              onMouseLeave={(e) => { e.currentTarget.style.color = "var(--text-muted)"; }}
            >
              {chatOpen ? "\u2190" : "\u2192"}
            </button>
            <div style={{ flex: 1 }}>
              <DashboardToolbar />
            </div>
          </div>

          <div style={{ flex: 1, overflow: "auto", padding: 20 }}>
            <DashboardCanvas />
          </div>
        </div>
      )}
    </div>
  );
}
