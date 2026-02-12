"use client";

import { useState, type ReactNode } from "react";
import type { Widget } from "@/types/widget";

interface WidgetCardProps {
  widget: Widget;
  children: ReactNode;
  onPin?: () => void;
  onRemove?: () => void;
  isPinned?: boolean;
}

export function WidgetCard({ widget, children, onPin, onRemove, isPinned }: WidgetCardProps) {
  const [showSql, setShowSql] = useState(false);

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        background: "#fff",
        borderRadius: "var(--r-xl)",
        boxShadow: "var(--shadow-sm)",
        overflow: "hidden",
        transition: "box-shadow 200ms ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.boxShadow = "var(--shadow-md)"; }}
      onMouseLeave={(e) => { e.currentTarget.style.boxShadow = "var(--shadow-sm)"; }}
    >
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "14px 20px 8px" }}>
        <h3 style={{
          margin: 0, fontSize: "0.85rem", fontWeight: 600, letterSpacing: "-0.01em",
          color: "var(--text-primary)", lineHeight: 1.3,
        }}>
          {widget.title}
        </h3>
        <div style={{ display: "flex", gap: 2, flexShrink: 0 }}>
          {widget.sql && (
            <WidgetBtn onClick={() => setShowSql(!showSql)} title="View SQL" active={showSql}>
              SQL
            </WidgetBtn>
          )}
          {onPin && !isPinned && (
            <WidgetBtn onClick={onPin} title="Pin to dashboard">+</WidgetBtn>
          )}
          {onRemove && (
            <WidgetBtn onClick={onRemove} title="Remove">&times;</WidgetBtn>
          )}
        </div>
      </div>

      {showSql && widget.sql && (
        <div style={{ padding: "0 20px 8px" }}>
          <pre style={{
            fontSize: "0.68rem", lineHeight: 1.5,
            background: "var(--app-bg)", padding: "8px 12px", borderRadius: "var(--r-sm)",
            overflow: "auto", maxHeight: 100,
            color: "var(--text-secondary)", fontFamily: "var(--font-mono)",
          }}>
            {widget.sql}
          </pre>
        </div>
      )}

      <div style={{ flex: 1, minHeight: 0, padding: "0 20px 20px" }}>
        {children}
      </div>
    </div>
  );
}

function WidgetBtn({ onClick, title, active, children }: {
  onClick: () => void; title: string; active?: boolean; children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      title={title}
      style={{
        background: active ? "var(--accent-light)" : "none",
        border: "none", cursor: "pointer",
        fontSize: "0.68rem", fontWeight: 600,
        padding: "3px 8px", borderRadius: 6,
        color: active ? "var(--accent)" : "var(--text-muted)",
        fontFamily: "var(--font-mono)",
        transition: "all 120ms ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.background = "var(--accent-light)"; e.currentTarget.style.color = "var(--accent)"; }}
      onMouseLeave={(e) => { e.currentTarget.style.background = active ? "var(--accent-light)" : "none"; e.currentTarget.style.color = active ? "var(--accent)" : "var(--text-muted)"; }}
    >
      {children}
    </button>
  );
}
