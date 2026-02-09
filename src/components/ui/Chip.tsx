interface ChipProps {
  label: string;
  active?: boolean;
  onClick?: () => void;
}

export function Chip({ label, active, onClick }: ChipProps) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={`chip-base ${active ? "chip-active" : "chip-muted"}`}
    >
      {label}
    </button>
  );
}
