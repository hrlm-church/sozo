"use client";

import { useState, useCallback, useRef } from "react";
import Link from "next/link";
import { EmptyState } from "@/components/dashboard/EmptyState";
import { BadgeStatus } from "@/components/dashboard/BadgeStatus";

interface PersonResult {
  person_id: string;
  display_name: string;
  email: string;
  lifecycle_stage: string;
  total_given: number;
}

export default function PeopleSearchPage() {
  const [query, setQuery] = useState("");
  const [results, setResults] = useState<PersonResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [searched, setSearched] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const doSearch = useCallback((q: string) => {
    if (q.trim().length < 2) {
      setResults([]);
      setSearched(false);
      return;
    }
    setLoading(true);
    setSearched(true);
    fetch(`/api/people/search?q=${encodeURIComponent(q.trim())}`)
      .then((r) => r.json())
      .then((d) => setResults(d.results ?? d ?? []))
      .catch(() => setResults([]))
      .finally(() => setLoading(false));
  }, []);

  const handleChange = (value: string) => {
    setQuery(value);
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => doSearch(value), 400);
  };

  const stageStatus = (stage: string): "positive" | "negative" | "warning" | "neutral" | "info" => {
    const lower = (stage ?? "").toLowerCase();
    if (lower.includes("active") || lower.includes("engaged")) return "positive";
    if (lower.includes("lapsed") || lower.includes("lost")) return "negative";
    if (lower.includes("at risk") || lower.includes("declining")) return "warning";
    if (lower.includes("new") || lower.includes("prospect")) return "info";
    return "neutral";
  };

  return (
    <div style={{ padding: "32px 40px", maxWidth: 1200 }}>
      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: "1.4rem", fontWeight: 700, color: "var(--text-primary)", letterSpacing: "-0.03em", margin: 0 }}>
          People
        </h1>
        <p style={{ fontSize: "0.84rem", color: "var(--text-muted)", margin: "4px 0 0" }}>
          Search for a person to view their complete profile
        </p>
      </div>

      {/* Search Input */}
      <div style={{ marginBottom: 28 }}>
        <div style={{
          display: "flex",
          alignItems: "center",
          gap: 12,
          padding: "14px 20px",
          borderRadius: 16,
          border: "1px solid var(--surface-border-strong)",
          background: "var(--surface)",
          boxShadow: "var(--shadow-sm)",
        }}>
          <span style={{ color: "var(--text-muted)", fontSize: "1rem", flexShrink: 0 }}>&#128269;</span>
          <input
            value={query}
            onChange={(e) => handleChange(e.target.value)}
            placeholder="Search by name or email..."
            style={{
              flex: 1,
              border: "none",
              outline: "none",
              background: "transparent",
              fontSize: "0.92rem",
              color: "var(--text-primary)",
            }}
          />
          {query && (
            <button
              onClick={() => { setQuery(""); setResults([]); setSearched(false); }}
              style={{
                border: "none",
                background: "transparent",
                color: "var(--text-muted)",
                cursor: "pointer",
                fontSize: "0.82rem",
                padding: "4px 8px",
              }}
            >
              Clear
            </button>
          )}
        </div>
      </div>

      {/* Results */}
      {loading && <EmptyState message="Searching..." loading />}

      {!loading && searched && results.length === 0 && (
        <EmptyState message={`No results found for "${query}"`} />
      )}

      {!loading && results.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
          {results.map((person) => (
            <Link
              key={person.person_id}
              href={`/people/${person.person_id}`}
              style={{ textDecoration: "none" }}
            >
              <div className="card-base" style={{ padding: "16px 20px", cursor: "pointer" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                  <div>
                    <div style={{ fontSize: "0.9rem", fontWeight: 600, color: "var(--text-primary)", marginBottom: 4 }}>
                      {person.display_name}
                    </div>
                    <div style={{ fontSize: "0.78rem", color: "var(--text-muted)" }}>
                      {person.email || "No email"}
                    </div>
                  </div>
                  <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
                    {person.lifecycle_stage && (
                      <BadgeStatus
                        status={stageStatus(person.lifecycle_stage)}
                        label={person.lifecycle_stage}
                      />
                    )}
                    <div style={{ textAlign: "right" }}>
                      <div style={{ fontSize: "0.92rem", fontWeight: 700, color: "var(--text-primary)" }}>
                        ${(person.total_given ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}
                      </div>
                      <div style={{ fontSize: "0.68rem", color: "var(--text-muted)" }}>total given</div>
                    </div>
                  </div>
                </div>
              </div>
            </Link>
          ))}
        </div>
      )}

      {!loading && !searched && (
        <div style={{ padding: "60px 20px", textAlign: "center", color: "var(--text-muted)", fontSize: "0.84rem" }}>
          Type at least 2 characters to search
        </div>
      )}
    </div>
  );
}
