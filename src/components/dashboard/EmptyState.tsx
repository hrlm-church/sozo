"use client";

interface EmptyStateProps {
  message: string;
  loading?: boolean;
}

export function EmptyState({ message, loading }: EmptyStateProps) {
  if (loading) {
    return (
      <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--text-muted)", fontSize: "0.84rem", padding: "60px 0", justifyContent: "center" }}>
        <span className="loading-pulse" style={{ width: 8, height: 8, borderRadius: "50%", background: "var(--accent)" }} />
        {message}
      </div>
    );
  }

  return (
    <div style={{ padding: "48px 20px", textAlign: "center", color: "var(--text-muted)", fontSize: "0.84rem" }}>
      {message}
    </div>
  );
}
