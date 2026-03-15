"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useSession, signOut } from "next-auth/react";
import { useState } from "react";

const NAV_ITEMS = [
  { href: "/", label: "Home", icon: "house" },
  { href: "/dashboards", label: "Dashboards", icon: "dashboards" },
  { href: "/chat", label: "Chat", icon: "chat" },
  { href: "/intelligence", label: "Intelligence", icon: "intel" },
  { href: "/settings", label: "Settings", icon: "settings" },
];

function NavIcon({ type, active }: { type: string; active: boolean }) {
  const color = active ? "var(--accent)" : "var(--text-muted)";

  switch (type) {
    case "house":
      return (
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <path
            d="M3 10L10 3L17 10V17C17 17.5523 16.5523 18 16 18H4C3.44772 18 3 17.5523 3 17V10Z"
            stroke={color}
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            fill={active ? "var(--accent-light)" : "none"}
          />
          <path d="M8 18V12H12V18" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      );
    case "chat":
      return (
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <path
            d="M3 5C3 3.89543 3.89543 3 5 3H15C16.1046 3 17 3.89543 17 5V12C17 13.1046 16.1046 14 15 14H7L3 17V5Z"
            stroke={color}
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
            fill={active ? "var(--accent-light)" : "none"}
          />
        </svg>
      );
    case "intel":
      return (
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <path d="M10 3V17" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
          <path d="M6 7V17" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
          <path d="M14 5V17" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
          <path d="M2 11V17" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
          <path d="M18 9V17" stroke={color} strokeWidth="1.5" strokeLinecap="round" />
        </svg>
      );
    case "dashboards":
      return (
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <rect x="2" y="2" width="7" height="7" rx="1.5" stroke={color} strokeWidth="1.5" fill={active ? "var(--accent-light)" : "none"} />
          <rect x="11" y="2" width="7" height="4" rx="1.5" stroke={color} strokeWidth="1.5" fill={active ? "var(--accent-light)" : "none"} />
          <rect x="11" y="9" width="7" height="9" rx="1.5" stroke={color} strokeWidth="1.5" fill={active ? "var(--accent-light)" : "none"} />
          <rect x="2" y="12" width="7" height="6" rx="1.5" stroke={color} strokeWidth="1.5" fill={active ? "var(--accent-light)" : "none"} />
        </svg>
      );
    case "settings":
      return (
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <circle cx="10" cy="10" r="3" stroke={color} strokeWidth="1.5" fill={active ? "var(--accent-light)" : "none"} />
          <path
            d="M10 1.5V4M10 16V18.5M18.5 10H16M4 10H1.5M16.01 3.99L14.24 5.76M5.76 14.24L3.99 16.01M16.01 16.01L14.24 14.24M5.76 5.76L3.99 3.99"
            stroke={color}
            strokeWidth="1.5"
            strokeLinecap="round"
          />
        </svg>
      );
    default:
      return null;
  }
}

export function Sidebar() {
  const pathname = usePathname();
  const { data: session } = useSession();
  const [collapsed, setCollapsed] = useState(false);

  const isActive = (href: string) => {
    if (href === "/") return pathname === "/";
    return pathname.startsWith(href);
  };

  return (
    <aside
      style={{
        width: collapsed ? 64 : 220,
        height: "100vh",
        display: "flex",
        flexDirection: "column",
        background: "var(--surface)",
        borderRight: "1px solid var(--surface-border)",
        transition: "width 200ms ease",
        flexShrink: 0,
        overflow: "hidden",
      }}
    >
      {/* Logo */}
      <div
        style={{
          padding: collapsed ? "20px 12px" : "20px 20px",
          display: "flex",
          alignItems: "center",
          gap: 10,
        }}
      >
        <div
          style={{
            width: 36,
            height: 36,
            borderRadius: 10,
            background: "var(--accent-gradient)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            color: "#fff",
            fontSize: "0.82rem",
            fontWeight: 700,
            flexShrink: 0,
            letterSpacing: "-0.02em",
          }}
        >
          S
        </div>
        {!collapsed && (
          <div>
            <div style={{ fontSize: "0.92rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em" }}>
              Sozo
            </div>
            <div style={{ fontSize: "0.65rem", color: "var(--text-muted)", marginTop: -1 }}>
              Intelligence Platform
            </div>
          </div>
        )}
      </div>

      {/* Navigation */}
      <nav style={{ flex: 1, padding: collapsed ? "8px 8px" : "8px 12px" }}>
        {NAV_ITEMS.map((item) => {
          const active = isActive(item.href);
          return (
            <Link
              key={item.href}
              href={item.href}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                padding: collapsed ? "10px 12px" : "10px 12px",
                borderRadius: 10,
                marginBottom: 2,
                textDecoration: "none",
                color: active ? "var(--accent)" : "var(--text-secondary)",
                background: active ? "var(--accent-light)" : "transparent",
                fontWeight: active ? 600 : 400,
                fontSize: "0.84rem",
                transition: "all 150ms ease",
                justifyContent: collapsed ? "center" : "flex-start",
              }}
              title={collapsed ? item.label : undefined}
            >
              <NavIcon type={item.icon} active={active} />
              {!collapsed && <span>{item.label}</span>}
            </Link>
          );
        })}
      </nav>

      {/* Bottom section */}
      <div style={{ padding: collapsed ? "12px 8px" : "12px", borderTop: "1px solid var(--surface-border)" }}>
        {/* Collapse toggle */}
        <button
          onClick={() => setCollapsed(!collapsed)}
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: collapsed ? "center" : "flex-start",
            gap: 10,
            width: "100%",
            padding: "8px 12px",
            borderRadius: 10,
            border: "none",
            background: "transparent",
            cursor: "pointer",
            color: "var(--text-muted)",
            fontSize: "0.78rem",
            transition: "color 150ms ease",
            marginBottom: 8,
          }}
          title={collapsed ? "Expand sidebar" : "Collapse sidebar"}
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none" style={{ transform: collapsed ? "rotate(180deg)" : "none", transition: "transform 200ms ease" }}>
            <path d="M10 12L6 8L10 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          {!collapsed && <span>Collapse</span>}
        </button>

        {/* User info */}
        {session?.user && !collapsed && (
          <div style={{ padding: "4px 12px 4px", marginBottom: 4 }}>
            <div style={{ fontSize: "0.78rem", fontWeight: 500, color: "var(--text-primary)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {session.user.name ?? "User"}
            </div>
            <div style={{ fontSize: "0.68rem", color: "var(--text-muted)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
              {session.user.email ?? ""}
            </div>
          </div>
        )}

        <button
          onClick={() => signOut()}
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: collapsed ? "center" : "flex-start",
            gap: 10,
            width: "100%",
            padding: "8px 12px",
            borderRadius: 10,
            border: "none",
            background: "transparent",
            cursor: "pointer",
            color: "var(--text-muted)",
            fontSize: "0.78rem",
            transition: "color 150ms ease",
          }}
          title="Sign out"
        >
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <path d="M6 14H3C2.44772 14 2 13.5523 2 13V3C2 2.44772 2.44772 2 3 2H6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M10 11L14 8L10 5" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
            <path d="M14 8H6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          {!collapsed && <span>Sign out</span>}
        </button>
      </div>
    </aside>
  );
}
