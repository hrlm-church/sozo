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

const ACCENT_COLORS: Record<string, string> = {
  kpi: "#6f43ea",
  stat_grid: "#2f7ff6",
  bar_chart: "#6f43ea",
  line_chart: "#2f7ff6",
  area_chart: "#17c6b8",
  donut_chart: "#f59e0b",
  table: "#2f7ff6",
  drill_down_table: "#6f43ea",
  funnel: "#f43f5e",
  text: "#7f8ba8",
};

export function WidgetCard({ widget, children, onPin, onRemove, isPinned }: WidgetCardProps) {
  const [showSql, setShowSql] = useState(false);
  const accent = ACCENT_COLORS[widget.type] ?? "#6f43ea";

  return (
    <div
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        background: "var(--surface-strong)",
        borderRadius: "var(--r-xl)",
        border: "1px solid var(--surface-border)",
        boxShadow: "var(--shadow-soft)",
        overflow: "hidden",
        transition: "box-shadow 200ms ease, transform 200ms ease",
        position: "relative",
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.boxShadow = "0 4px 0 rgba(17,24,39,0.03), 0 16px 40px rgba(27,32,56,0.10)";
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.boxShadow = "var(--shadow-soft)";
      }}
    >
      {/* Accent top bar */}
      <div style={{ height: 3, background: accent, flexShrink: 0 }} />

      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "14px 20px 8px" }}>
        <h3 style={{
          margin: 0, fontSize: "0.85rem", fontWeight: 650, letterSpacing: "-0.01em",
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

      {/* SQL preview */}
      {showSql && widget.sql && (
        <div style={{ padding: "0 20px 8px" }}>
          <pre style={{
            fontSize: "0.68rem", lineHeight: 1.5,
            background: "#f1f3f9", padding: "8px 12px", borderRadius: 8,
            overflow: "auto", maxHeight: 100,
            color: "#5a6078", fontFamily: "var(--font-mono)",
            border: "1px solid #e2e6ef",
          }}>
            {widget.sql}
          </pre>
        </div>
      )}

      {/* Content */}
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
        background: active ? "#eef0f8" : "none",
        border: "none", cursor: "pointer",
        fontSize: "0.68rem", fontWeight: 600,
        padding: "3px 8px", borderRadius: 6,
        color: active ? "var(--accent-purple)" : "var(--text-muted)",
        fontFamily: "var(--font-mono)",
        transition: "all 120ms ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.background = "#eef0f8"; }}
      onMouseLeave={(e) => { e.currentTarget.style.background = active ? "#eef0f8" : "none"; }}
    >
      {children}
    </button>
  );
}
