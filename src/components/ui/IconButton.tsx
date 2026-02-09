import { ReactNode } from "react";

interface IconButtonProps {
  label: string;
  icon: ReactNode;
  active?: boolean;
}

export function IconButton({ label, icon, active }: IconButtonProps) {
  return (
    <button
      type="button"
      aria-label={label}
      className={`icon-btn-base ${active ? "icon-btn-active" : "icon-btn-muted"}`}
    >
      <span aria-hidden="true" className="text-lg leading-none">
        {icon}
      </span>
    </button>
  );
}
