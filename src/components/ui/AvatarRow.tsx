interface AvatarRowProps {
  name: string;
  subtitle: string;
}

export function AvatarRow({ name, subtitle }: AvatarRowProps) {
  return (
    <div className="flex items-center gap-3">
      <span className="inline-flex size-11 items-center justify-center rounded-full bg-[var(--chip-active-bg)] text-sm font-semibold text-[var(--text-muted)]">
        {name
          .split(" ")
          .map((chunk) => chunk[0])
          .join("")
          .slice(0, 2)
          .toUpperCase()}
      </span>
      <div>
        <p className="text-lg font-medium text-[var(--text-primary)]">{name}</p>
        <p className="text-sm text-[var(--text-muted)]">{subtitle}</p>
      </div>
    </div>
  );
}
