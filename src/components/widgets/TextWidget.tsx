"use client";

import type { Widget } from "@/types/widget";

/** Escape HTML entities to prevent XSS before applying markdown transforms */
function escapeHtml(str: string): string {
  return str
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

export function TextWidget({ widget }: { widget: Widget }) {
  const markdown = widget.config.markdown ?? "";

  // Escape first, then apply markdown → HTML (only our transforms produce real HTML)
  const html = escapeHtml(markdown)
    .replace(/^### (.+)$/gm, '<h4 style="margin:8px 0 4px;font-size:0.95rem;font-weight:600">$1</h4>')
    .replace(/^## (.+)$/gm, '<h3 style="margin:8px 0 4px;font-size:1.05rem;font-weight:650">$1</h3>')
    .replace(/^# (.+)$/gm, '<h2 style="margin:8px 0 4px;font-size:1.15rem;font-weight:700">$1</h2>')
    .replace(/\*\*(.+?)\*\*/g, "<strong>$1</strong>")
    .replace(/\*(.+?)\*/g, "<em>$1</em>")
    .replace(/^- (.+)$/gm, '<li style="margin-left:16px">$1</li>')
    .replace(/\n/g, "<br/>");

  return (
    <div
      style={{
        fontSize: "0.85rem",
        lineHeight: 1.6,
        color: "var(--text-primary)",
        padding: "4px 0",
        overflow: "auto",
        height: "100%",
      }}
      dangerouslySetInnerHTML={{ __html: html }}
    />
  );
}
