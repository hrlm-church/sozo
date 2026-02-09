interface SearchInputProps {
  value: string;
  onChange: (value: string) => void;
}

export function SearchInput({ value, onChange }: SearchInputProps) {
  return (
    <label className="flex h-14 w-full min-w-0 items-center gap-3 rounded-[var(--r-xl)] border border-[var(--surface-border)] bg-[var(--surface)] px-4 shadow-[var(--shadow-soft)]">
      <span aria-hidden="true" className="text-[var(--text-muted)]">
        🔍
      </span>
      <input
        value={value}
        onChange={(event) => onChange(event.target.value)}
        placeholder="Start searching here..."
        className="w-full bg-transparent text-[15px] text-[var(--text-primary)] outline-none placeholder:text-[var(--text-muted)]"
      />
    </label>
  );
}
