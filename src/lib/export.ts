import * as XLSX from "xlsx";
import type { Widget } from "@/types/widget";

/**
 * Export widget data as CSV and trigger download.
 */
export function exportCSV(data: Record<string, unknown>[], title: string) {
  if (!data.length) return;
  const keys = Object.keys(data[0]);
  const header = keys.map(k => `"${k}"`).join(",");
  const rows = data.map(row =>
    keys.map(k => {
      const v = row[k];
      if (v == null) return "";
      const s = String(v);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    }).join(",")
  );
  const csv = [header, ...rows].join("\n");
  download(csv, `${sanitize(title)}.csv`, "text/csv;charset=utf-8;");
}

/**
 * Export widget data as XLSX and trigger download.
 */
export function exportXLSX(data: Record<string, unknown>[], title: string) {
  if (!data.length) return;
  const ws = XLSX.utils.json_to_sheet(data);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, "Data");
  const buf = XLSX.write(wb, { bookType: "xlsx", type: "array" });
  const blob = new Blob([buf], { type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" });
  downloadBlob(blob, `${sanitize(title)}.xlsx`);
}

// ─── Formatting helpers ─────────────────────────────────────────────────────

const BRAND_COLOR: [number, number, number] = [0, 113, 227];
const HEADER_BG: [number, number, number] = [15, 23, 42];     // slate-900
const ROW_ALT: [number, number, number] = [248, 250, 252];    // slate-50
const TEXT_PRIMARY: [number, number, number] = [15, 23, 42];
const TEXT_MUTED: [number, number, number] = [100, 116, 139]; // slate-500

function fmtVal(v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "number") {
    if (Math.abs(v) >= 1000) return v.toLocaleString("en-US", { maximumFractionDigits: 2 });
    return String(Math.round(v * 100) / 100);
  }
  return String(v);
}

function isCurrencyCol(key: string): boolean {
  const k = key.toLowerCase();
  return /amount|total|given|revenue|price|gift|commerce|capacity|avg/.test(k) && !/count|order|ticket|donor/.test(k);
}

function fmtCell(key: string, v: unknown): string {
  if (v == null) return "—";
  if (typeof v === "number" && isCurrencyCol(key)) {
    return "$" + v.toLocaleString("en-US", { minimumFractionDigits: 0, maximumFractionDigits: 0 });
  }
  return fmtVal(v);
}

// ─── Native PDF export ──────────────────────────────────────────────────────

/**
 * Export a single widget as a professionally formatted PDF.
 * Uses native jsPDF drawing for crisp text — no screenshots.
 */
export async function exportPDF(element: HTMLElement, title: string, widget?: Widget) {
  const { jsPDF } = await import("jspdf");
  const autoTable = (await import("jspdf-autotable")).default;

  // A4 landscape for wide data, portrait for text-heavy
  const isTable = widget?.type === "table" || widget?.type === "drill_down_table";
  const orientation = isTable ? "l" : "p";
  const pdf = new jsPDF({ orientation, unit: "mm", format: "a4" });
  const pageW = pdf.internal.pageSize.getWidth();
  const pageH = pdf.internal.pageSize.getHeight();
  const margin = 15;
  const contentW = pageW - margin * 2;
  let y = margin;

  // ── Header bar ──
  pdf.setFillColor(...BRAND_COLOR);
  pdf.rect(0, 0, pageW, 18, "F");
  pdf.setFont("helvetica", "bold");
  pdf.setFontSize(13);
  pdf.setTextColor(255, 255, 255);
  pdf.text(title, margin, 12);
  pdf.setFontSize(8);
  pdf.setFont("helvetica", "normal");
  pdf.text(`Sozo — Pure Freedom Ministries  |  ${new Date().toLocaleDateString("en-US", { year: "numeric", month: "long", day: "numeric" })}`, pageW - margin, 12, { align: "right" });
  y = 24;

  if (!widget) {
    // Fallback: screenshot approach for unknown contexts
    await screenshotFallback(element, pdf, title, margin, y, contentW, pageH);
    pdf.save(`${sanitize(title)}.pdf`);
    return;
  }

  const type = widget.type;
  const data = widget.data;
  const config = widget.config;

  // ── KPI ──
  if (type === "kpi") {
    const val = config.value != null ? String(config.value) : "—";
    const display = config.unit === "$" ? `$${fmtVal(config.value)}` : fmtVal(config.value);
    y += 15;
    pdf.setFont("helvetica", "bold");
    pdf.setFontSize(42);
    pdf.setTextColor(...BRAND_COLOR);
    pdf.text(display, pageW / 2, y, { align: "center" });
    y += 10;
    if (config.delta != null) {
      pdf.setFontSize(14);
      pdf.setTextColor(...TEXT_MUTED);
      const arrow = config.trend === "up" ? "▲" : config.trend === "down" ? "▼" : "—";
      pdf.text(`${arrow} ${config.delta}`, pageW / 2, y, { align: "center" });
    }
  }

  // ── Stat Grid ──
  else if (type === "stat_grid" && config.stats) {
    const stats = config.stats;
    const cols = Math.min(stats.length, 4);
    const cellW = contentW / cols;
    const cellH = 28;

    for (let i = 0; i < stats.length; i++) {
      const col = i % cols;
      const row = Math.floor(i / cols);
      const x = margin + col * cellW;
      const cy = y + row * (cellH + 6);

      // Card background
      pdf.setFillColor(248, 250, 252);
      pdf.roundedRect(x, cy, cellW - 4, cellH, 3, 3, "F");

      // Value
      const display = stats[i].unit === "$" ? `$${fmtVal(stats[i].value)}` : fmtVal(stats[i].value);
      pdf.setFont("helvetica", "bold");
      pdf.setFontSize(18);
      pdf.setTextColor(...TEXT_PRIMARY);
      pdf.text(display, x + (cellW - 4) / 2, cy + 13, { align: "center" });

      // Label
      pdf.setFont("helvetica", "normal");
      pdf.setFontSize(8);
      pdf.setTextColor(...TEXT_MUTED);
      pdf.text(stats[i].label, x + (cellW - 4) / 2, cy + 22, { align: "center" });
    }
    y += Math.ceil(stats.length / cols) * (cellH + 6) + 4;
  }

  // ── Table / Drill-down Table ──
  else if ((type === "table" || type === "drill_down_table") && data.length > 0) {
    const keys = Object.keys(data[0]);
    const head = [keys];
    const body = data.map(row => keys.map(k => fmtCell(k, row[k])));

    autoTable(pdf, {
      startY: y,
      head,
      body,
      margin: { left: margin, right: margin },
      styles: {
        fontSize: 7,
        cellPadding: 2.5,
        textColor: TEXT_PRIMARY,
        lineColor: [226, 232, 240],
        lineWidth: 0.1,
      },
      headStyles: {
        fillColor: HEADER_BG,
        textColor: [255, 255, 255],
        fontStyle: "bold",
        fontSize: 7.5,
      },
      alternateRowStyles: {
        fillColor: ROW_ALT,
      },
      columnStyles: keys.reduce<Record<number, object>>((acc, key, i) => {
        if (isCurrencyCol(key)) {
          acc[i] = { halign: "right" as const };
        }
        return acc;
      }, {}),
      didDrawPage: (hookData: { pageNumber: number }) => {
        // Footer on each page
        pdf.setFontSize(7);
        pdf.setTextColor(...TEXT_MUTED);
        pdf.text(`Page ${hookData.pageNumber}`, pageW - margin, pageH - 8, { align: "right" });
        pdf.text("Sozo — Pure Freedom Ministries", margin, pageH - 8);
      },
    });
  }

  // ── Text widget ──
  else if (type === "text" && config.markdown) {
    const lines = config.markdown
      .replace(/\*\*(.*?)\*\*/g, "$1")  // strip bold markdown
      .replace(/\*(.*?)\*/g, "$1")       // strip italic
      .split("\n")
      .filter(l => l.trim());

    pdf.setFont("helvetica", "normal");
    pdf.setFontSize(10);
    pdf.setTextColor(...TEXT_PRIMARY);

    for (const line of lines) {
      const trimmed = line.trim();
      const isBullet = trimmed.startsWith("- ") || trimmed.startsWith("• ");
      const isHeader = trimmed.startsWith("### ") || trimmed.startsWith("## ");

      if (isHeader) {
        y += 3;
        pdf.setFont("helvetica", "bold");
        pdf.setFontSize(11);
        const headerText = trimmed.replace(/^#+\s*/, "");
        const wrapped = pdf.splitTextToSize(headerText, contentW);
        pdf.text(wrapped, margin, y);
        y += wrapped.length * 5 + 2;
        pdf.setFont("helvetica", "normal");
        pdf.setFontSize(10);
      } else {
        const prefix = isBullet ? "  • " : "";
        const text = isBullet ? trimmed.slice(2) : trimmed;
        const wrapped = pdf.splitTextToSize(prefix + text, contentW - (isBullet ? 4 : 0));
        if (y + wrapped.length * 4.5 > pageH - 20) {
          pdf.addPage();
          y = margin;
        }
        pdf.text(wrapped, margin + (isBullet ? 2 : 0), y);
        y += wrapped.length * 4.5 + 1;
      }
    }
  }

  // ── Funnel ──
  else if (type === "funnel" && data.length > 0) {
    const catKey = config.categoryKey || Object.keys(data[0])[0];
    const valKey = config.valueKeys?.[0] || Object.keys(data[0])[1];
    const maxVal = Math.max(...data.map(r => Number(r[valKey]) || 0));
    const barH = 16;
    const gap = 6;

    for (let i = 0; i < data.length; i++) {
      const row = data[i];
      const label = String(row[catKey] ?? "");
      const val = Number(row[valKey]) || 0;
      const barW = maxVal > 0 ? (val / maxVal) * (contentW - 50) : 0;
      const cy = y + i * (barH + gap);

      if (cy + barH > pageH - 20) {
        pdf.addPage();
        y = margin - i * (barH + gap);
      }

      // Bar
      const alpha = 1 - (i / data.length) * 0.5;
      pdf.setFillColor(
        Math.round(BRAND_COLOR[0] + (255 - BRAND_COLOR[0]) * (1 - alpha)),
        Math.round(BRAND_COLOR[1] + (255 - BRAND_COLOR[1]) * (1 - alpha)),
        Math.round(BRAND_COLOR[2] + (255 - BRAND_COLOR[2]) * (1 - alpha)),
      );
      const xOffset = (contentW - 50 - barW) / 2;
      pdf.roundedRect(margin + xOffset, cy, barW, barH, 2, 2, "F");

      // Label
      pdf.setFont("helvetica", "bold");
      pdf.setFontSize(8);
      pdf.setTextColor(...TEXT_PRIMARY);
      pdf.text(label, margin + (contentW - 50) / 2, cy + barH / 2 + 1, { align: "center" });

      // Value
      pdf.setFont("helvetica", "normal");
      pdf.setTextColor(...TEXT_MUTED);
      pdf.text(fmtVal(val), margin + contentW - 45 + xOffset, cy + barH / 2 + 1);
    }
  }

  // ── Charts (bar, line, area, donut) — capture SVG as image ──
  else if (["bar_chart", "line_chart", "area_chart", "donut_chart"].includes(type)) {
    // For charts, capture the rendered SVG/canvas as an image — this is the right approach for charts
    await screenshotFallback(element, pdf, title, margin, y, contentW, pageH);
  }

  // ── Fallback ──
  else {
    await screenshotFallback(element, pdf, title, margin, y, contentW, pageH);
  }

  // ── Footer on last page (if not handled by autoTable) ──
  if (type !== "table" && type !== "drill_down_table") {
    pdf.setFontSize(7);
    pdf.setTextColor(...TEXT_MUTED);
    pdf.text("Sozo — Pure Freedom Ministries", margin, pageH - 8);
    pdf.text(new Date().toLocaleDateString("en-US"), pageW - margin, pageH - 8, { align: "right" });
  }

  pdf.save(`${sanitize(title)}.pdf`);
}

/**
 * Fallback: screenshot element for chart widgets (charts are SVG/canvas — screenshot is appropriate).
 */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function screenshotFallback(
  element: HTMLElement,
  pdf: any,
  _title: string,
  margin: number,
  y: number,
  contentW: number,
  pageH: number,
) {
  const { default: html2canvas } = await import("html2canvas");
  const canvas = await html2canvas(element, {
    scale: 2,
    useCORS: true,
    backgroundColor: "#ffffff",
  });
  const imgData = canvas.toDataURL("image/png");
  const imgW = canvas.width;
  const imgH = canvas.height;
  const ratio = Math.min(contentW / imgW, (pageH - y - 20) / imgH);
  const w = imgW * ratio;
  const h = imgH * ratio;
  pdf.addImage(imgData, "PNG", margin, y, w, h);
}

function sanitize(name: string): string {
  return name.replace(/[^a-zA-Z0-9_\- ]/g, "").substring(0, 60).trim() || "export";
}

function download(content: string, filename: string, mime: string) {
  const blob = new Blob([content], { type: mime });
  downloadBlob(blob, filename);
}

function downloadBlob(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
