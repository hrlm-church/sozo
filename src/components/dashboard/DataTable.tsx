"use client";

interface Column {
  key: string;
  label: string;
  align?: string;
  format?: string;
}

interface DataTableProps {
  columns: Column[];
  data: Record<string, unknown>[];
  maxRows?: number;
}

function formatCell(value: unknown, format?: string): string {
  if (value === null || value === undefined) return "-";
  if (format === "currency") return `$${Number(value).toLocaleString(undefined, { maximumFractionDigits: 0 })}`;
  if (format === "percent") return `${Number(value).toFixed(1)}%`;
  if (format === "number") return Number(value).toLocaleString();
  if (format === "date") {
    const d = new Date(String(value));
    return isNaN(d.getTime()) ? String(value) : d.toLocaleDateString();
  }
  return String(value);
}

export function DataTable({ columns, data, maxRows }: DataTableProps) {
  const rows = maxRows ? data.slice(0, maxRows) : data;

  if (!rows || rows.length === 0) {
    return (
      <div style={{ padding: "24px 20px", textAlign: "center", color: "var(--text-muted)", fontSize: "0.82rem" }}>
        No data available
      </div>
    );
  }

  return (
    <div className="card-base" style={{ overflow: "hidden" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
        <thead>
          <tr style={{ borderBottom: "1px solid var(--surface-border)" }}>
            {columns.map((col) => (
              <th
                key={col.key}
                style={{
                  textAlign: (col.align as React.CSSProperties["textAlign"]) ?? "left",
                  padding: "10px 14px",
                  color: "var(--text-muted)",
                  fontWeight: 500,
                  fontSize: "0.72rem",
                  textTransform: "uppercase",
                  letterSpacing: "0.04em",
                }}
              >
                {col.label}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, ri) => (
            <tr key={ri} style={{ borderBottom: "1px solid var(--surface-border)" }}>
              {columns.map((col) => (
                <td
                  key={col.key}
                  style={{
                    padding: "10px 14px",
                    textAlign: (col.align as React.CSSProperties["textAlign"]) ?? "left",
                    color: "var(--text-secondary)",
                    fontWeight: col.align === "right" || col.format === "currency" ? 600 : 400,
                  }}
                >
                  {formatCell(row[col.key], col.format)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
