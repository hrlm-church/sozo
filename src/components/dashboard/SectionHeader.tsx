"use client";

import Link from "next/link";

interface SectionHeaderProps {
  title: string;
  subtitle?: string;
  href?: string;
}

export function SectionHeader({ title, subtitle, href }: SectionHeaderProps) {
  return (
    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 12, marginTop: 28 }}>
      <div>
        <h2 style={{ fontSize: "0.92rem", fontWeight: 600, color: "var(--text-primary)", margin: 0, letterSpacing: "-0.01em" }}>
          {title}
        </h2>
        {subtitle && (
          <p style={{ fontSize: "0.78rem", color: "var(--text-muted)", margin: "2px 0 0" }}>{subtitle}</p>
        )}
      </div>
      {href && (
        <Link href={href} style={{ fontSize: "0.76rem", color: "var(--accent)", textDecoration: "none", fontWeight: 500 }}>
          View all &rarr;
        </Link>
      )}
    </div>
  );
}
