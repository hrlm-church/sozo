"use client";

import { useEffect, useState, useCallback } from "react";

interface Member {
  id: string;
  email: string;
  role: string;
  is_active: boolean;
  created_at: string;
  invited_by: string | null;
}

export default function UsersPage() {
  const [members, setMembers] = useState<Member[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showInvite, setShowInvite] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState("viewer");
  const [inviting, setInviting] = useState(false);

  const loadMembers = useCallback(async () => {
    try {
      const res = await fetch("/api/admin/users");
      if (!res.ok) {
        const data = await res.json();
        setError(data.error ?? "Failed to load members");
        return;
      }
      const data = await res.json();
      setMembers(data.members ?? []);
    } catch {
      setError("Failed to load team members");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { loadMembers(); }, [loadMembers]);

  const handleInvite = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!inviteEmail.trim()) return;
    setInviting(true);
    try {
      const res = await fetch("/api/admin/users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email: inviteEmail.trim(), role: inviteRole }),
      });
      if (res.ok) {
        setInviteEmail("");
        setShowInvite(false);
        loadMembers();
      } else {
        const data = await res.json();
        setError(data.error ?? "Failed to invite user");
      }
    } finally {
      setInviting(false);
    }
  };

  const handleUpdateRole = async (memberId: string, role: string) => {
    await fetch("/api/admin/users", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ memberId, role }),
    });
    loadMembers();
  };

  const handleToggleActive = async (memberId: string, isActive: boolean) => {
    await fetch("/api/admin/users", {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ memberId, isActive }),
    });
    loadMembers();
  };

  return (
    <div style={{ padding: "32px 40px", maxWidth: 900 }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 24 }}>
        <div>
          <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
            Team Members
          </h1>
          <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
            Manage who has access to your workspace
          </p>
        </div>
        <button
          onClick={() => setShowInvite(!showInvite)}
          className="gradient-btn"
          style={{ padding: "9px 20px", fontSize: "0.82rem" }}
        >
          Invite Member
        </button>
      </div>

      {/* Error */}
      {error && (
        <div style={{
          padding: "12px 16px",
          background: "rgba(255, 59, 48, 0.08)",
          borderRadius: 10,
          color: "var(--red)",
          fontSize: "0.82rem",
          marginBottom: 16,
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
        }}>
          <span>{error}</span>
          <button
            onClick={() => setError(null)}
            style={{ background: "none", border: "none", color: "var(--red)", cursor: "pointer", fontSize: "1rem" }}
          >
            &times;
          </button>
        </div>
      )}

      {/* Invite form */}
      {showInvite && (
        <div className="card-base" style={{ padding: "20px 24px", marginBottom: 20 }}>
          <form onSubmit={handleInvite} style={{ display: "flex", gap: 12, alignItems: "flex-end" }}>
            <div style={{ flex: 1 }}>
              <label style={{ display: "block", fontSize: "0.72rem", fontWeight: 500, color: "var(--text-muted)", marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                Email
              </label>
              <input
                type="email"
                value={inviteEmail}
                onChange={(e) => setInviteEmail(e.target.value)}
                placeholder="colleague@example.com"
                required
                style={{
                  width: "100%",
                  padding: "10px 14px",
                  fontSize: "0.84rem",
                  borderRadius: 10,
                  border: "1px solid var(--surface-border-strong)",
                  background: "var(--app-bg)",
                  outline: "none",
                  color: "var(--text-primary)",
                }}
              />
            </div>
            <div>
              <label style={{ display: "block", fontSize: "0.72rem", fontWeight: 500, color: "var(--text-muted)", marginBottom: 6, textTransform: "uppercase", letterSpacing: "0.04em" }}>
                Role
              </label>
              <select
                value={inviteRole}
                onChange={(e) => setInviteRole(e.target.value)}
                style={{
                  padding: "10px 14px",
                  fontSize: "0.84rem",
                  borderRadius: 10,
                  border: "1px solid var(--surface-border-strong)",
                  background: "var(--app-bg)",
                  outline: "none",
                  color: "var(--text-primary)",
                  cursor: "pointer",
                }}
              >
                <option value="viewer">Viewer</option>
                <option value="analyst">Analyst</option>
                <option value="admin">Admin</option>
              </select>
            </div>
            <button
              type="submit"
              disabled={inviting || !inviteEmail.trim()}
              className="gradient-btn"
              style={{ padding: "10px 20px", fontSize: "0.82rem" }}
            >
              {inviting ? "Inviting..." : "Send Invite"}
            </button>
          </form>
        </div>
      )}

      {/* Members list */}
      {loading ? (
        <div style={{ display: "flex", alignItems: "center", gap: 8, color: "var(--text-muted)", fontSize: "0.84rem", padding: "40px 0" }}>
          <span className="loading-pulse" style={{ width: 8, height: 8, borderRadius: "50%", background: "var(--accent)" }} />
          Loading team members...
        </div>
      ) : members.length === 0 ? (
        <div style={{ padding: "48px 20px", textAlign: "center", color: "var(--text-muted)", fontSize: "0.84rem" }}>
          No team members found. Invite someone to get started.
        </div>
      ) : (
        <div className="card-base" style={{ overflow: "hidden" }}>
          <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.82rem" }}>
            <thead>
              <tr style={{ borderBottom: "1px solid var(--surface-border)" }}>
                <th style={{ textAlign: "left", padding: "10px 16px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Email</th>
                <th style={{ textAlign: "left", padding: "10px 16px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Role</th>
                <th style={{ textAlign: "left", padding: "10px 16px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Status</th>
                <th style={{ textAlign: "left", padding: "10px 16px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Joined</th>
                <th style={{ textAlign: "right", padding: "10px 16px", color: "var(--text-muted)", fontWeight: 500, fontSize: "0.72rem", textTransform: "uppercase", letterSpacing: "0.04em" }}>Actions</th>
              </tr>
            </thead>
            <tbody>
              {members.map((m) => (
                <tr key={m.id} style={{ borderBottom: "1px solid var(--surface-border)" }}>
                  <td style={{ padding: "12px 16px", fontWeight: 500, color: "var(--text-primary)" }}>
                    {m.email}
                  </td>
                  <td style={{ padding: "12px 16px" }}>
                    <select
                      value={m.role}
                      onChange={(e) => handleUpdateRole(m.id, e.target.value)}
                      style={{
                        padding: "4px 10px",
                        fontSize: "0.78rem",
                        borderRadius: 8,
                        border: "1px solid var(--surface-border)",
                        background: "var(--surface)",
                        color: "var(--text-primary)",
                        cursor: "pointer",
                        outline: "none",
                      }}
                    >
                      <option value="viewer">Viewer</option>
                      <option value="analyst">Analyst</option>
                      <option value="admin">Admin</option>
                    </select>
                  </td>
                  <td style={{ padding: "12px 16px" }}>
                    <span style={{
                      fontSize: "0.72rem",
                      fontWeight: 600,
                      padding: "2px 10px",
                      borderRadius: 20,
                      color: m.is_active ? "var(--green)" : "var(--text-muted)",
                      background: m.is_active ? "rgba(52, 199, 89, 0.1)" : "var(--surface-border)",
                    }}>
                      {m.is_active ? "Active" : "Inactive"}
                    </span>
                  </td>
                  <td style={{ padding: "12px 16px", color: "var(--text-muted)", fontSize: "0.78rem" }}>
                    {new Date(m.created_at).toLocaleDateString()}
                  </td>
                  <td style={{ padding: "12px 16px", textAlign: "right" }}>
                    <button
                      onClick={() => handleToggleActive(m.id, !m.is_active)}
                      style={{
                        padding: "4px 12px",
                        fontSize: "0.76rem",
                        borderRadius: 8,
                        border: "1px solid var(--surface-border)",
                        background: "var(--surface)",
                        color: m.is_active ? "var(--red)" : "var(--green)",
                        cursor: "pointer",
                        transition: "all 150ms ease",
                      }}
                    >
                      {m.is_active ? "Deactivate" : "Activate"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
