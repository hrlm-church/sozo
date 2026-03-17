"use client";

import { useEffect, useState, useRef, useCallback } from "react";

interface Alert {
  id: string;
  alert_type: string;
  severity: string;
  title: string;
  body: string | null;
  person_name: string | null;
  is_read: boolean;
  created_at: string;
}

export function AlertBell() {
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const fetchAlerts = useCallback(async () => {
    try {
      const res = await fetch("/api/alerts?limit=10");
      if (!res.ok) return;
      const data = await res.json();
      setAlerts(data.alerts ?? []);
      setUnreadCount(data.unread_count ?? 0);
    } catch {
      // Silent fail
    }
  }, []);

  useEffect(() => {
    fetchAlerts();
    const interval = setInterval(fetchAlerts, 60_000); // Poll every minute
    return () => clearInterval(interval);
  }, [fetchAlerts]);

  // Close dropdown on click outside
  useEffect(() => {
    if (!open) return;
    const handleClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClick);
    return () => document.removeEventListener("mousedown", handleClick);
  }, [open]);

  const markAllRead = async () => {
    try {
      await fetch("/api/alerts", {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ mark_all_read: true }),
      });
      setUnreadCount(0);
      setAlerts((prev) => prev.map((a) => ({ ...a, is_read: true })));
    } catch {
      // Silent fail
    }
  };

  const severityColor = (severity: string) => {
    switch (severity) {
      case "critical": return "var(--red)";
      case "warning": return "var(--orange)";
      default: return "var(--accent)";
    }
  };

  const formatTime = (dateStr: string) => {
    const d = new Date(dateStr);
    const now = new Date();
    const diffMs = now.getTime() - d.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    if (diffMins < 1) return "Just now";
    if (diffMins < 60) return `${diffMins}m ago`;
    const diffHours = Math.floor(diffMins / 60);
    if (diffHours < 24) return `${diffHours}h ago`;
    const diffDays = Math.floor(diffHours / 24);
    return `${diffDays}d ago`;
  };

  return (
    <div ref={ref} style={{ position: "relative" }}>
      <button
        onClick={() => setOpen(!open)}
        style={{
          background: "none",
          border: "none",
          cursor: "pointer",
          padding: "6px",
          borderRadius: 8,
          position: "relative",
          color: "var(--text-muted)",
          transition: "color 150ms ease",
        }}
        title="Alerts"
      >
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <path
            d="M10 2C7.23858 2 5 4.23858 5 7V10L3 13H17L15 10V7C15 4.23858 12.7614 2 10 2Z"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          <path
            d="M8 13V14C8 15.1046 8.89543 16 10 16C11.1046 16 12 15.1046 12 14V13"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
        {unreadCount > 0 && (
          <span
            style={{
              position: "absolute",
              top: 2,
              right: 2,
              background: "var(--red)",
              color: "#fff",
              fontSize: "0.6rem",
              fontWeight: 700,
              borderRadius: 10,
              minWidth: 16,
              height: 16,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: "0 4px",
            }}
          >
            {unreadCount > 99 ? "99+" : unreadCount}
          </span>
        )}
      </button>

      {open && (
        <div
          style={{
            position: "absolute",
            top: "100%",
            right: 0,
            width: 320,
            maxHeight: 400,
            overflow: "auto",
            background: "var(--surface)",
            borderRadius: 12,
            boxShadow: "var(--shadow-lg)",
            border: "1px solid var(--surface-border)",
            zIndex: 100,
            marginTop: 4,
          }}
        >
          <div
            style={{
              padding: "12px 16px",
              borderBottom: "1px solid var(--surface-border)",
              display: "flex",
              justifyContent: "space-between",
              alignItems: "center",
            }}
          >
            <span style={{ fontSize: "0.84rem", fontWeight: 600, color: "var(--text-primary)" }}>
              Alerts
            </span>
            {unreadCount > 0 && (
              <button
                onClick={markAllRead}
                style={{
                  background: "none",
                  border: "none",
                  cursor: "pointer",
                  fontSize: "0.72rem",
                  color: "var(--accent)",
                  fontWeight: 500,
                }}
              >
                Mark all read
              </button>
            )}
          </div>

          {alerts.length === 0 ? (
            <div
              style={{
                padding: "24px 16px",
                textAlign: "center",
                fontSize: "0.78rem",
                color: "var(--text-muted)",
              }}
            >
              No alerts yet
            </div>
          ) : (
            alerts.map((alert) => (
              <div
                key={alert.id}
                style={{
                  padding: "10px 16px",
                  borderBottom: "1px solid var(--surface-border)",
                  background: alert.is_read ? "transparent" : "rgba(0, 113, 227, 0.03)",
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 4 }}>
                  <span
                    style={{
                      width: 6,
                      height: 6,
                      borderRadius: "50%",
                      background: alert.is_read ? "transparent" : severityColor(alert.severity),
                      flexShrink: 0,
                    }}
                  />
                  <span
                    style={{
                      fontSize: "0.78rem",
                      fontWeight: alert.is_read ? 400 : 600,
                      color: "var(--text-primary)",
                      flex: 1,
                    }}
                  >
                    {alert.title}
                  </span>
                  <span style={{ fontSize: "0.65rem", color: "var(--text-muted)", flexShrink: 0 }}>
                    {formatTime(alert.created_at)}
                  </span>
                </div>
                {alert.body && (
                  <div
                    style={{
                      fontSize: "0.72rem",
                      color: "var(--text-muted)",
                      marginLeft: 14,
                      lineHeight: 1.4,
                    }}
                  >
                    {alert.body.slice(0, 120)}
                    {alert.body.length > 120 ? "..." : ""}
                  </div>
                )}
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}
