"use client";

import { useEffect, useState, useCallback } from "react";

interface Action {
  id: string;
  title: string;
  description: string | null;
  action_type: string;
  priority_score: number;
  person_name: string | null;
  status: string;
  source: string;
  due_date: string | null;
  outcome: string | null;
  outcome_value: number | null;
  outcome_date: string | null;
  created_at: string;
  updated_at: string;
}

const STATUS_FILTERS = ["all", "pending", "in_progress", "completed", "dismissed"] as const;
const TYPE_LABELS: Record<string, string> = {
  call: "Call",
  email: "Email",
  thank: "Thank",
  reengage: "Re-engage",
  review: "Review",
  general: "General",
};

export default function ActionsPage() {
  const [actions, setActions] = useState<Action[]>([]);
  const [loading, setLoading] = useState(true);
  const [statusFilter, setStatusFilter] = useState<string>("all");

  const fetchActions = useCallback(async () => {
    try {
      const params = new URLSearchParams({ limit: "100" });
      if (statusFilter !== "all") params.set("status", statusFilter);
      const res = await fetch(`/api/actions?${params}`);
      if (!res.ok) return;
      const data = await res.json();
      setActions(data.actions ?? []);
    } catch {
      // Silent fail
    } finally {
      setLoading(false);
    }
  }, [statusFilter]);

  useEffect(() => {
    setLoading(true);
    fetchActions();
  }, [fetchActions]);

  const updateStatus = async (id: string, status: string) => {
    try {
      await fetch("/api/actions", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id, status }),
      });
      setActions((prev) =>
        prev.map((a) => (a.id === id ? { ...a, status, updated_at: new Date().toISOString() } : a)),
      );
    } catch {
      // Silent fail
    }
  };

  const deleteAction = async (id: string) => {
    try {
      await fetch(`/api/actions?id=${id}`, { method: "DELETE" });
      setActions((prev) => prev.filter((a) => a.id !== id));
    } catch {
      // Silent fail
    }
  };

  const typeStyle = (type: string) => {
    switch (type) {
      case "call": return { bg: "rgba(0, 113, 227, 0.08)", color: "var(--accent)" };
      case "email": return { bg: "rgba(88, 86, 214, 0.08)", color: "var(--accent-secondary)" };
      case "thank": return { bg: "rgba(52, 199, 89, 0.08)", color: "var(--green)" };
      case "reengage": return { bg: "rgba(255, 149, 0, 0.08)", color: "var(--orange)" };
      case "review": return { bg: "rgba(255, 59, 48, 0.08)", color: "var(--red)" };
      default: return { bg: "rgba(0, 0, 0, 0.04)", color: "var(--text-muted)" };
    }
  };

  const priorityBar = (score: number) => {
    const color = score >= 80 ? "var(--red)" : score >= 60 ? "var(--orange)" : score >= 40 ? "var(--accent)" : "var(--text-muted)";
    return (
      <div style={{ width: 40, height: 4, borderRadius: 2, background: "rgba(0,0,0,0.06)" }}>
        <div style={{ width: `${score}%`, height: "100%", borderRadius: 2, background: color }} />
      </div>
    );
  };

  const statusBadge = (status: string) => {
    const styles: Record<string, { bg: string; color: string }> = {
      pending: { bg: "rgba(255, 149, 0, 0.1)", color: "var(--orange)" },
      in_progress: { bg: "rgba(0, 113, 227, 0.1)", color: "var(--accent)" },
      completed: { bg: "rgba(52, 199, 89, 0.1)", color: "var(--green)" },
      dismissed: { bg: "rgba(0, 0, 0, 0.04)", color: "var(--text-muted)" },
    };
    const s = styles[status] ?? styles.pending;
    return (
      <span style={{
        padding: "3px 8px",
        borderRadius: 12,
        fontSize: "0.68rem",
        fontWeight: 600,
        background: s.bg,
        color: s.color,
      }}>
        {status.replace("_", " ")}
      </span>
    );
  };

  const pendingCount = actions.filter((a) => a.status === "pending" || a.status === "in_progress").length;

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1000 }}>
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Actions
          {pendingCount > 0 && (
            <span style={{
              fontSize: "0.78rem",
              fontWeight: 600,
              color: "var(--accent)",
              marginLeft: 10,
              background: "var(--accent-light)",
              padding: "2px 10px",
              borderRadius: 12,
              verticalAlign: "middle",
            }}>
              {pendingCount} pending
            </span>
          )}
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          AI-generated action items sorted by priority
        </p>
      </div>

      {/* Status filter */}
      <div style={{ display: "flex", gap: 6, marginBottom: 20 }}>
        {STATUS_FILTERS.map((s) => (
          <button
            key={s}
            onClick={() => setStatusFilter(s)}
            className={statusFilter === s ? "chip-active" : "chip-muted"}
            style={{ cursor: "pointer" }}
          >
            {s === "all" ? "All" : s.replace("_", " ")}
          </button>
        ))}
      </div>

      {loading && (
        <div style={{ textAlign: "center", padding: 60, color: "var(--text-muted)", fontSize: "0.84rem" }}>
          Loading actions...
        </div>
      )}

      {!loading && actions.length === 0 && (
        <div className="card-base" style={{ padding: "60px 40px", textAlign: "center" }}>
          <div style={{ fontSize: "0.92rem", fontWeight: 600, color: "var(--text-secondary)", marginBottom: 8 }}>
            No actions found
          </div>
          <div style={{ fontSize: "0.82rem", color: "var(--text-muted)", maxWidth: 400, margin: "0 auto", lineHeight: 1.6 }}>
            Actions are created by the AI when analyzing your data, or from daily briefings.
            Ask Sozo to analyze donor churn, giving trends, or engagement to generate action items.
          </div>
        </div>
      )}

      {!loading && actions.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {actions.map((action) => {
            const ts = typeStyle(action.action_type);
            return (
              <div key={action.id} className="card-base" style={{ padding: "16px 20px" }}>
                <div style={{ display: "flex", alignItems: "flex-start", gap: 12 }}>
                  {/* Priority + type */}
                  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 6, paddingTop: 2 }}>
                    {priorityBar(action.priority_score)}
                    <span style={{
                      padding: "2px 8px",
                      borderRadius: 10,
                      fontSize: "0.62rem",
                      fontWeight: 600,
                      background: ts.bg,
                      color: ts.color,
                      textTransform: "uppercase",
                      letterSpacing: "0.02em",
                    }}>
                      {TYPE_LABELS[action.action_type] ?? action.action_type}
                    </span>
                  </div>

                  {/* Content */}
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                      <span style={{
                        fontSize: "0.88rem",
                        fontWeight: 600,
                        color: action.status === "completed" || action.status === "dismissed" ? "var(--text-muted)" : "var(--text-primary)",
                        textDecoration: action.status === "completed" ? "line-through" : "none",
                      }}>
                        {action.title}
                      </span>
                      {statusBadge(action.status)}
                    </div>
                    {action.description && (
                      <div style={{ fontSize: "0.78rem", color: "var(--text-muted)", lineHeight: 1.5, marginBottom: 6 }}>
                        {action.description.slice(0, 200)}{action.description.length > 200 ? "..." : ""}
                      </div>
                    )}
                    <div style={{ display: "flex", alignItems: "center", gap: 12, fontSize: "0.72rem", color: "var(--text-muted)" }}>
                      {action.person_name && <span>{action.person_name}</span>}
                      <span>{action.source}</span>
                      {action.due_date && <span>Due: {new Date(action.due_date).toLocaleDateString()}</span>}
                      {action.outcome && <span style={{ color: "var(--green)" }}>Outcome: {action.outcome}</span>}
                    </div>
                  </div>

                  {/* Actions */}
                  <div style={{ display: "flex", gap: 4, flexShrink: 0 }}>
                    {action.status === "pending" && (
                      <button
                        onClick={() => updateStatus(action.id, "in_progress")}
                        title="Start"
                        style={{
                          background: "var(--accent-light)",
                          border: "none",
                          borderRadius: 6,
                          padding: "4px 10px",
                          cursor: "pointer",
                          fontSize: "0.72rem",
                          fontWeight: 500,
                          color: "var(--accent)",
                        }}
                      >
                        Start
                      </button>
                    )}
                    {(action.status === "pending" || action.status === "in_progress") && (
                      <>
                        <button
                          onClick={() => updateStatus(action.id, "completed")}
                          title="Complete"
                          style={{
                            background: "rgba(52, 199, 89, 0.1)",
                            border: "none",
                            borderRadius: 6,
                            padding: "4px 10px",
                            cursor: "pointer",
                            fontSize: "0.72rem",
                            fontWeight: 500,
                            color: "var(--green)",
                          }}
                        >
                          Done
                        </button>
                        <button
                          onClick={() => updateStatus(action.id, "dismissed")}
                          title="Dismiss"
                          style={{
                            background: "rgba(0, 0, 0, 0.04)",
                            border: "none",
                            borderRadius: 6,
                            padding: "4px 10px",
                            cursor: "pointer",
                            fontSize: "0.72rem",
                            fontWeight: 500,
                            color: "var(--text-muted)",
                          }}
                        >
                          Dismiss
                        </button>
                      </>
                    )}
                    <button
                      onClick={() => deleteAction(action.id)}
                      title="Delete"
                      style={{
                        background: "transparent",
                        border: "none",
                        borderRadius: 6,
                        padding: "4px 6px",
                        cursor: "pointer",
                        fontSize: "0.72rem",
                        color: "var(--text-muted)",
                        opacity: 0.5,
                      }}
                    >
                      x
                    </button>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
