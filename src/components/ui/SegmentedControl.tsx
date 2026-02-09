interface SegmentItem<T extends string> {
  id: T;
  label: string;
}

interface SegmentedControlProps<T extends string> {
  items: SegmentItem<T>[];
  value: T;
  onChange: (value: T) => void;
}

export function SegmentedControl<T extends string>({
  items,
  value,
  onChange,
}: SegmentedControlProps<T>) {
  return (
    <div className="inline-flex items-center gap-1 rounded-[var(--r-xl)] border border-[var(--surface-border)] bg-[var(--surface)] p-1">
      {items.map((item) => (
        <button
          key={item.id}
          type="button"
          onClick={() => onChange(item.id)}
          className={`rounded-[var(--r-lg)] px-4 py-2 text-sm transition ${
            value === item.id
              ? "bg-[var(--chip-active-bg)] text-[var(--text-primary)]"
              : "text-[var(--text-muted)] hover:text-[var(--text-primary)]"
          }`}
        >
          {item.label}
        </button>
      ))}
    </div>
  );
}
