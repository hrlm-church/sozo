"use client";

interface DateRangeValue {
  preset: string;
  start?: string;
  end?: string;
}

interface DateRangePickerProps {
  value: DateRangeValue;
  onChange: (value: DateRangeValue) => void;
}

const PRESETS = [
  { value: "7d", label: "Last 7 days" },
  { value: "30d", label: "Last 30 days" },
  { value: "90d", label: "Last 90 days" },
  { value: "this_month", label: "This month" },
  { value: "last_month", label: "Last month" },
  { value: "quarter", label: "This quarter" },
  { value: "ytd", label: "Year to date" },
  { value: "1y", label: "Last 12 months" },
  { value: "all", label: "All time" },
  { value: "custom", label: "Custom range" },
];

export function DateRangePicker({ value, onChange }: DateRangePickerProps) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
      <select
        value={value.preset}
        onChange={(e) => onChange({ preset: e.target.value, start: value.start, end: value.end })}
        style={{
          padding: "6px 12px",
          borderRadius: 8,
          border: "1px solid var(--surface-border-strong)",
          background: "var(--surface)",
          color: "var(--text-secondary)",
          fontSize: "0.78rem",
          fontWeight: 500,
          cursor: "pointer",
          outline: "none",
        }}
      >
        {PRESETS.map((p) => (
          <option key={p.value} value={p.value}>{p.label}</option>
        ))}
      </select>

      {value.preset === "custom" && (
        <>
          <input
            type="date"
            value={value.start ?? ""}
            onChange={(e) => onChange({ ...value, start: e.target.value })}
            style={{
              padding: "5px 10px",
              borderRadius: 8,
              border: "1px solid var(--surface-border-strong)",
              background: "var(--surface)",
              color: "var(--text-secondary)",
              fontSize: "0.78rem",
            }}
          />
          <span style={{ color: "var(--text-muted)", fontSize: "0.78rem" }}>to</span>
          <input
            type="date"
            value={value.end ?? ""}
            onChange={(e) => onChange({ ...value, end: e.target.value })}
            style={{
              padding: "5px 10px",
              borderRadius: 8,
              border: "1px solid var(--surface-border-strong)",
              background: "var(--surface)",
              color: "var(--text-secondary)",
              fontSize: "0.78rem",
            }}
          />
        </>
      )}

      {value.preset !== "all" && (
        <button
          onClick={() => onChange({ preset: "all" })}
          style={{
            padding: "4px 10px",
            borderRadius: 8,
            border: "1px solid var(--surface-border)",
            background: "transparent",
            color: "var(--text-muted)",
            fontSize: "0.72rem",
            cursor: "pointer",
          }}
        >
          Clear
        </button>
      )}
    </div>
  );
}
