"use client";

import { useState } from "react";
import { ChatPanel } from "@/components/chat/ChatPanel";
import { DashboardCanvas } from "@/components/dashboard/DashboardCanvas";
import { DashboardToolbar } from "@/components/dashboard/DashboardToolbar";

export default function Home() {
  const [chatOpen, setChatOpen] = useState(true);

  return (
    <div style={{ display: "flex", height: "100vh", overflow: "hidden", background: "var(--app-bg)" }}>
      {chatOpen && (
        <div style={{ width: 660, flexShrink: 0, display: "flex", flexDirection: "column", height: "100%" }}>
          <ChatPanel />
        </div>
      )}

      <div style={{ flex: 1, display: "flex", flexDirection: "column", height: "100%", minWidth: 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 0 }}>
          <button
            onClick={() => setChatOpen(!chatOpen)}
            title={chatOpen ? "Hide chat" : "Show chat"}
            style={{
              background: "#fff",
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
    </div>
  );
}
