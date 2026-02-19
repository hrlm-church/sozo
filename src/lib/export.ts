import * as XLSX from "xlsx";

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

/**
 * Export a DOM element as PDF via html2canvas + jsPDF.
 * The element ref must be passed in.
 */
export async function exportPDF(element: HTMLElement, title: string) {
  const { default: html2canvas } = await import("html2canvas");
  const { jsPDF } = await import("jspdf");

  const canvas = await html2canvas(element, {
    scale: 2,
    useCORS: true,
    backgroundColor: "#ffffff",
  });

  const imgData = canvas.toDataURL("image/png");
  const imgW = canvas.width;
  const imgH = canvas.height;

  // A4 landscape for wider charts, portrait for tables
  const isWide = imgW > imgH * 1.2;
  const orientation = isWide ? "l" : "p";
  const pdf = new jsPDF({ orientation, unit: "mm", format: "a4" });

  const pageW = pdf.internal.pageSize.getWidth() - 20; // 10mm margins
  const pageH = pdf.internal.pageSize.getHeight() - 20;
  const ratio = Math.min(pageW / imgW, pageH / imgH);
  const w = imgW * ratio;
  const h = imgH * ratio;

  pdf.setFontSize(12);
  pdf.text(title, 10, 12);
  pdf.addImage(imgData, "PNG", 10, 18, w, h);
  pdf.save(`${sanitize(title)}.pdf`);
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
