interface GradientButtonProps {
  label: string;
}

export function GradientButton({ label }: GradientButtonProps) {
  return (
    <button type="button" className="gradient-btn">
      {label}
    </button>
  );
}
