"use client";

import { useState } from "react";
import { ChatPanel } from "@/components/chat/ChatPanel";
import { DashboardCanvas } from "@/components/dashboard/DashboardCanvas";
import { DashboardToolbar } from "@/components/dashboard/DashboardToolbar";

export default function Home() {
  const [chatOpen, setChatOpen] = useState(true);

  return (
    <div style={{ display: "flex", height: "100vh", overflow: "hidden" }}>
      {/* Chat panel — collapsible */}
      {chatOpen && (
        <div
          style={{
            width: 420,
            flexShrink: 0,
            display: "flex",
            flexDirection: "column",
            height: "100%",
          }}
        >
          <ChatPanel />
        </div>
      )}

      {/* Dashboard area */}
      <div
        style={{
          flex: 1,
          display: "flex",
          flexDirection: "column",
          height: "100%",
          background: "var(--app-bg)",
          minWidth: 0,
        }}
      >
        {/* Top bar with toggle + toolbar */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 0,
          }}
        >
          <button
            onClick={() => setChatOpen(!chatOpen)}
            title={chatOpen ? "Hide chat" : "Show chat"}
            style={{
              background: "var(--surface-strong)",
              border: "none",
              borderBottom: "1px solid var(--surface-border)",
              borderRight: "1px solid var(--surface-border)",
              padding: "12px 14px",
              cursor: "pointer",
              fontSize: "1rem",
              color: "var(--text-muted)",
              flexShrink: 0,
            }}
          >
            {chatOpen ? "\u2190" : "\u2192"}
          </button>
          <div style={{ flex: 1 }}>
            <DashboardToolbar />
          </div>
        </div>

        {/* Canvas */}
        <div
          style={{
            flex: 1,
            overflow: "auto",
            padding: 16,
          }}
        >
          <DashboardCanvas />
        </div>
      </div>
    </div>
  );
}
