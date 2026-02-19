"use client";

import { useState, useRef, type ReactNode } from "react";
import type { Widget } from "@/types/widget";
import { exportCSV, exportXLSX, exportPDF } from "@/lib/export";

interface WidgetCardProps {
  widget: Widget;
  children: ReactNode;
  onPin?: () => void;
  onRemove?: () => void;
  isPinned?: boolean;
}

export function WidgetCard({ widget, children, onPin, onRemove, isPinned }: WidgetCardProps) {
  const [showSql, setShowSql] = useState(false);
  const [showExport, setShowExport] = useState(false);
  const cardRef = useRef<HTMLDivElement>(null);

  const hasData = widget.data && widget.data.length > 0;

  const handleExportCSV = () => { exportCSV(widget.data, widget.title); setShowExport(false); };
  const handleExportXLSX = () => { exportXLSX(widget.data, widget.title); setShowExport(false); };
  const handleExportPDF = async () => {
    if (cardRef.current) {
      await exportPDF(cardRef.current, widget.title);
    }
    setShowExport(false);
  };

  return (
    <div
      ref={cardRef}
      style={{
        height: "100%",
        display: "flex",
        flexDirection: "column",
        background: "var(--surface-strong)",
        borderRadius: "var(--r-xl)",
        border: "1px solid var(--surface-border)",
        boxShadow: "var(--shadow-sm)",
        overflow: "hidden",
        transition: "box-shadow 200ms ease",
        position: "relative",
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
          {hasData && (
            <div style={{ position: "relative" }}>
              <WidgetBtn onClick={() => setShowExport(!showExport)} title="Export data" active={showExport}>
                Export
              </WidgetBtn>
              {showExport && (
                <div
                  style={{
                    position: "absolute",
                    top: "100%",
                    right: 0,
                    marginTop: 4,
                    background: "var(--surface-elevated)",
                    borderRadius: 8,
                    boxShadow: "0 4px 16px rgba(0,0,0,0.4), 0 1px 4px rgba(0,0,0,0.3)",
                    zIndex: 50,
                    minWidth: 120,
                    padding: "4px 0",
                    border: "1px solid var(--surface-border)",
                  }}
                >
                  <ExportMenuItem onClick={handleExportCSV} label="CSV" desc="Spreadsheet-compatible" />
                  <ExportMenuItem onClick={handleExportXLSX} label="Excel (.xlsx)" desc="Formatted workbook" />
                  <ExportMenuItem onClick={handleExportPDF} label="PDF" desc="Print-ready snapshot" />
                </div>
              )}
            </div>
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

      {/* Click-away overlay for export menu */}
      {showExport && (
        <div
          style={{ position: "fixed", inset: 0, zIndex: 40 }}
          onClick={() => setShowExport(false)}
        />
      )}
    </div>
  );
}

function ExportMenuItem({ onClick, label, desc }: { onClick: () => void; label: string; desc: string }) {
  return (
    <button
      onClick={onClick}
      style={{
        display: "block",
        width: "100%",
        padding: "8px 14px",
        background: "none",
        border: "none",
        cursor: "pointer",
        textAlign: "left",
        transition: "background 100ms ease",
      }}
      onMouseEnter={(e) => { e.currentTarget.style.background = "var(--accent-light, #f0f0ff)"; }}
      onMouseLeave={(e) => { e.currentTarget.style.background = "none"; }}
    >
      <div style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-primary)" }}>{label}</div>
      <div style={{ fontSize: "0.65rem", color: "var(--text-muted)", marginTop: 1 }}>{desc}</div>
    </button>
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
