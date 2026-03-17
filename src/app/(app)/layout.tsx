"use client";

import { Sidebar } from "@/components/layout/Sidebar";
import { ConversationSidebar } from "@/components/chat/ConversationSidebar";
import { AlertBell } from "@/components/layout/AlertBell";

export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ display: "flex", height: "100vh", overflow: "hidden", background: "var(--app-bg)" }}>
      <Sidebar />
      <ConversationSidebar />
      <div style={{ flex: 1, display: "flex", flexDirection: "column", minWidth: 0 }}>
        {/* Top bar with alerts */}
        <div
          style={{
            display: "flex",
            justifyContent: "flex-end",
            alignItems: "center",
            padding: "8px 20px",
            borderBottom: "1px solid var(--surface-border)",
            background: "var(--surface)",
            flexShrink: 0,
            gap: 8,
          }}
        >
          <AlertBell />
        </div>
        <main style={{ flex: 1, overflow: "auto", minWidth: 0 }}>
          {children}
        </main>
      </div>
    </div>
  );
}
