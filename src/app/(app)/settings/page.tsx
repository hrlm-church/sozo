"use client";

import { useSession } from "next-auth/react";
import { useEffect, useState, useCallback } from "react";

interface Goal {
  id: string;
  title: string;
  goal_type: string;
  target_value: number;
  current_value: number;
  unit: string | null;
  target_date: string | null;
  status: string;
}

export default function SettingsPage() {
  const { data: session } = useSession();
  const [goals, setGoals] = useState<Goal[]>([]);
  const [showGoalForm, setShowGoalForm] = useState(false);
  const [newGoal, setNewGoal] = useState({ title: "", goal_type: "custom", target_value: "", unit: "$", target_date: "" });

  const fetchGoals = useCallback(async () => {
    try {
      const res = await fetch("/api/goals");
      if (!res.ok) return;
      const data = await res.json();
      setGoals(data.goals ?? []);
    } catch {
      // Silent fail
    }
  }, []);

  useEffect(() => {
    fetchGoals();
  }, [fetchGoals]);

  const createGoal = async () => {
    if (!newGoal.title || !newGoal.target_value) return;
    try {
      await fetch("/api/goals", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          title: newGoal.title,
          goal_type: newGoal.goal_type,
          target_value: Number(newGoal.target_value),
          unit: newGoal.unit || null,
          target_date: newGoal.target_date || null,
        }),
      });
      setNewGoal({ title: "", goal_type: "custom", target_value: "", unit: "$", target_date: "" });
      setShowGoalForm(false);
      fetchGoals();
    } catch {
      // Silent fail
    }
  };

  const deleteGoal = async (id: string) => {
    try {
      await fetch(`/api/goals?id=${id}`, { method: "DELETE" });
      setGoals((prev) => prev.filter((g) => g.id !== id));
    } catch {
      // Silent fail
    }
  };

  const goalProgress = (goal: Goal) => {
    if (goal.target_value <= 0) return 0;
    return Math.min(100, Math.round((goal.current_value / goal.target_value) * 100));
  };

  return (
    <div style={{ padding: "32px 40px", maxWidth: 800 }}>
      <div style={{ marginBottom: 32 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          Settings
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Manage your preferences, goals, and account
        </p>
      </div>

      {/* Profile */}
      <div className="card-base" style={{ padding: "24px 28px", marginBottom: 16 }}>
        <div style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 16 }}>
          Profile
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <div
            style={{
              width: 48,
              height: 48,
              borderRadius: 12,
              background: "var(--accent-gradient)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              color: "#fff",
              fontSize: "1.1rem",
              fontWeight: 700,
              flexShrink: 0,
            }}
          >
            {(session?.user?.name ?? "U")[0].toUpperCase()}
          </div>
          <div>
            <div style={{ fontSize: "0.92rem", fontWeight: 600, color: "var(--text-primary)" }}>
              {session?.user?.name ?? "User"}
            </div>
            <div style={{ fontSize: "0.78rem", color: "var(--text-muted)" }}>
              {session?.user?.email ?? ""}
            </div>
          </div>
        </div>
      </div>

      {/* Goals */}
      <div className="card-base" style={{ padding: "24px 28px", marginBottom: 16 }}>
        <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 16 }}>
          <div style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em" }}>
            Goals
          </div>
          <button
            onClick={() => setShowGoalForm(!showGoalForm)}
            className="btn-secondary"
            style={{ fontSize: "0.76rem" }}
          >
            {showGoalForm ? "Cancel" : "+ Add Goal"}
          </button>
        </div>

        {showGoalForm && (
          <div style={{ marginBottom: 20, padding: 16, borderRadius: 12, background: "var(--app-bg)" }}>
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12, marginBottom: 12 }}>
              <div>
                <label style={{ display: "block", fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, marginBottom: 4 }}>Title</label>
                <input
                  value={newGoal.title}
                  onChange={(e) => setNewGoal((g) => ({ ...g, title: e.target.value }))}
                  placeholder="e.g., Reach 500 active donors"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    border: "1px solid var(--surface-border-strong)",
                    borderRadius: 8,
                    fontSize: "0.82rem",
                    background: "var(--surface)",
                    color: "var(--text-primary)",
                    outline: "none",
                  }}
                />
              </div>
              <div>
                <label style={{ display: "block", fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, marginBottom: 4 }}>Type</label>
                <select
                  value={newGoal.goal_type}
                  onChange={(e) => setNewGoal((g) => ({ ...g, goal_type: e.target.value }))}
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    border: "1px solid var(--surface-border-strong)",
                    borderRadius: 8,
                    fontSize: "0.82rem",
                    background: "var(--surface)",
                    color: "var(--text-primary)",
                    outline: "none",
                  }}
                >
                  <option value="donors">Donors</option>
                  <option value="revenue">Revenue</option>
                  <option value="retention">Retention</option>
                  <option value="engagement">Engagement</option>
                  <option value="custom">Custom</option>
                </select>
              </div>
              <div>
                <label style={{ display: "block", fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, marginBottom: 4 }}>Target Value</label>
                <input
                  type="number"
                  value={newGoal.target_value}
                  onChange={(e) => setNewGoal((g) => ({ ...g, target_value: e.target.value }))}
                  placeholder="500"
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    border: "1px solid var(--surface-border-strong)",
                    borderRadius: 8,
                    fontSize: "0.82rem",
                    background: "var(--surface)",
                    color: "var(--text-primary)",
                    outline: "none",
                  }}
                />
              </div>
              <div>
                <label style={{ display: "block", fontSize: "0.72rem", color: "var(--text-muted)", fontWeight: 500, marginBottom: 4 }}>Target Date</label>
                <input
                  type="date"
                  value={newGoal.target_date}
                  onChange={(e) => setNewGoal((g) => ({ ...g, target_date: e.target.value }))}
                  style={{
                    width: "100%",
                    padding: "8px 12px",
                    border: "1px solid var(--surface-border-strong)",
                    borderRadius: 8,
                    fontSize: "0.82rem",
                    background: "var(--surface)",
                    color: "var(--text-primary)",
                    outline: "none",
                  }}
                />
              </div>
            </div>
            <button onClick={createGoal} className="btn-primary" style={{ fontSize: "0.82rem" }}>
              Create Goal
            </button>
          </div>
        )}

        {goals.length === 0 && !showGoalForm && (
          <div style={{ padding: "20px 0", textAlign: "center", fontSize: "0.82rem", color: "var(--text-muted)" }}>
            No goals set yet. Add one to start tracking.
          </div>
        )}

        {goals.map((goal) => {
          const pct = goalProgress(goal);
          const progressColor = pct >= 100 ? "var(--green)" : pct >= 50 ? "var(--accent)" : "var(--orange)";
          return (
            <div key={goal.id} style={{ padding: "14px 0", borderBottom: "1px solid var(--surface-border)" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 8 }}>
                <div>
                  <span style={{ fontSize: "0.88rem", fontWeight: 500, color: "var(--text-primary)" }}>
                    {goal.title}
                  </span>
                  <span style={{
                    marginLeft: 8,
                    padding: "2px 8px",
                    borderRadius: 10,
                    fontSize: "0.62rem",
                    fontWeight: 600,
                    background: "rgba(0,0,0,0.04)",
                    color: "var(--text-muted)",
                    textTransform: "uppercase",
                  }}>
                    {goal.goal_type}
                  </span>
                </div>
                <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
                  <span style={{ fontSize: "0.82rem", fontWeight: 600, color: progressColor }}>
                    {goal.unit === "$" ? `$${goal.current_value.toLocaleString()}` : goal.current_value.toLocaleString()}
                    {" / "}
                    {goal.unit === "$" ? `$${goal.target_value.toLocaleString()}` : goal.target_value.toLocaleString()}
                  </span>
                  <button
                    onClick={() => deleteGoal(goal.id)}
                    style={{
                      background: "none",
                      border: "none",
                      cursor: "pointer",
                      fontSize: "0.72rem",
                      color: "var(--text-muted)",
                      opacity: 0.5,
                      padding: "2px 4px",
                    }}
                  >
                    x
                  </button>
                </div>
              </div>
              <div style={{ height: 6, borderRadius: 3, background: "rgba(0,0,0,0.06)", overflow: "hidden" }}>
                <div
                  style={{
                    height: "100%",
                    width: `${pct}%`,
                    borderRadius: 3,
                    background: progressColor,
                    transition: "width 300ms ease",
                  }}
                />
              </div>
              {goal.target_date && (
                <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", marginTop: 4 }}>
                  Target: {new Date(goal.target_date).toLocaleDateString()}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Preferences */}
      <div className="card-base" style={{ padding: "24px 28px", marginBottom: 16 }}>
        <div style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 16 }}>
          Preferences
        </div>
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <div style={{ fontSize: "0.88rem", fontWeight: 500, color: "var(--text-primary)" }}>
                Daily Briefing
              </div>
              <div style={{ fontSize: "0.78rem", color: "var(--text-muted)", marginTop: 2 }}>
                Receive an AI-generated morning briefing at 6am
              </div>
            </div>
            <div style={{
              padding: "4px 12px",
              borderRadius: 20,
              fontSize: "0.72rem",
              fontWeight: 500,
              background: "rgba(52, 199, 89, 0.1)",
              color: "var(--green)",
            }}>
              Active
            </div>
          </div>

          <div style={{ borderTop: "1px solid var(--surface-border)" }} />

          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <div>
              <div style={{ fontSize: "0.88rem", fontWeight: 500, color: "var(--text-primary)" }}>
                Alert Notifications
              </div>
              <div style={{ fontSize: "0.78rem", color: "var(--text-muted)", marginTop: 2 }}>
                Get notified about churn risks and milestones
              </div>
            </div>
            <div style={{
              padding: "4px 12px",
              borderRadius: 20,
              fontSize: "0.72rem",
              fontWeight: 500,
              background: "rgba(52, 199, 89, 0.1)",
              color: "var(--green)",
            }}>
              Active
            </div>
          </div>
        </div>
      </div>

      {/* About */}
      <div className="card-base" style={{ padding: "24px 28px" }}>
        <div style={{ fontSize: "0.78rem", fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 12 }}>
          About
        </div>
        <div style={{ fontSize: "0.84rem", color: "var(--text-secondary)", lineHeight: 1.6 }}>
          Sozo Intelligence Platform v2.0
        </div>
        <div style={{ fontSize: "0.78rem", color: "var(--text-muted)", marginTop: 4 }}>
          AI-powered ministry intelligence for Pure Freedom Ministries
        </div>
      </div>
    </div>
  );
}
