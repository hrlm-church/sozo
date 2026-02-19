"use client";

import { signIn } from "next-auth/react";
import { useState } from "react";
import { useRouter } from "next/navigation";

export default function LoginPage() {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);
  const router = useRouter();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!username.trim() || !password.trim()) return;
    setLoading(true);
    setError("");

    const res = await signIn("credentials", {
      username: username.trim(),
      password: password.trim(),
      redirect: false,
    });

    setLoading(false);
    if (res?.error) {
      setError("Invalid username or password");
    } else {
      router.push("/");
      router.refresh();
    }
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100vh",
        background: "#f5f5f7",
      }}
    >
      <div
        style={{
          width: 360,
          padding: "48px 40px 40px",
          background: "#ffffff",
          borderRadius: 20,
          boxShadow: "0 2px 20px rgba(0, 0, 0, 0.06)",
        }}
      >
        <div style={{ textAlign: "center", marginBottom: 32 }}>
          <h1
            style={{
              fontSize: "2rem",
              fontWeight: 700,
              color: "#1d1d1f",
              letterSpacing: "-0.04em",
              margin: "0 0 4px",
            }}
          >
            Sozo
          </h1>
          <p style={{ fontSize: "0.84rem", color: "#86868b", margin: 0 }}>
            Ministry Intelligence Platform
          </p>
        </div>

        <form onSubmit={handleSubmit}>
          <div style={{ marginBottom: 16 }}>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              placeholder="Username"
              autoComplete="username"
              style={{
                width: "100%",
                padding: "12px 16px",
                fontSize: "0.88rem",
                borderRadius: 12,
                border: "1px solid rgba(0, 0, 0, 0.12)",
                background: "#f5f5f7",
                outline: "none",
                color: "#1d1d1f",
                transition: "border-color 150ms ease",
              }}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "#0071e3";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(0, 0, 0, 0.12)";
              }}
            />
          </div>
          <div style={{ marginBottom: 20 }}>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              placeholder="Password"
              autoComplete="current-password"
              style={{
                width: "100%",
                padding: "12px 16px",
                fontSize: "0.88rem",
                borderRadius: 12,
                border: "1px solid rgba(0, 0, 0, 0.12)",
                background: "#f5f5f7",
                outline: "none",
                color: "#1d1d1f",
                transition: "border-color 150ms ease",
              }}
              onFocus={(e) => {
                e.currentTarget.style.borderColor = "#0071e3";
              }}
              onBlur={(e) => {
                e.currentTarget.style.borderColor = "rgba(0, 0, 0, 0.12)";
              }}
            />
          </div>

          {error && (
            <p
              style={{
                fontSize: "0.78rem",
                color: "#ff3b30",
                textAlign: "center",
                margin: "0 0 16px",
              }}
            >
              {error}
            </p>
          )}

          <button
            type="submit"
            disabled={loading || !username.trim() || !password.trim()}
            style={{
              width: "100%",
              padding: "12px 24px",
              fontSize: "0.88rem",
              fontWeight: 500,
              color: "#fff",
              background: "#0071e3",
              border: "none",
              borderRadius: 12,
              cursor: loading ? "wait" : "pointer",
              transition: "opacity 150ms ease",
              opacity: loading ? 0.6 : 1,
            }}
          >
            {loading ? "Signing in..." : "Sign In"}
          </button>
        </form>

        <div
          style={{
            margin: "24px 0 0",
            textAlign: "center",
            borderTop: "1px solid rgba(0, 0, 0, 0.06)",
            paddingTop: 20,
          }}
        >
          <button
            onClick={() => signIn("microsoft-entra-id", { callbackUrl: "/" })}
            style={{
              background: "none",
              border: "1px solid rgba(0, 0, 0, 0.12)",
              borderRadius: 12,
              padding: "10px 20px",
              fontSize: "0.82rem",
              color: "#6e6e73",
              cursor: "pointer",
              width: "100%",
              transition: "all 150ms ease",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.borderColor = "rgba(0, 0, 0, 0.2)";
              e.currentTarget.style.color = "#1d1d1f";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.borderColor = "rgba(0, 0, 0, 0.12)";
              e.currentTarget.style.color = "#6e6e73";
            }}
          >
            Sign in with Microsoft
          </button>
        </div>

        <p
          style={{
            fontSize: "0.68rem",
            color: "#86868b",
            textAlign: "center",
            marginTop: 24,
            marginBottom: 0,
          }}
        >
          Pure Freedom Ministries
        </p>
      </div>
    </div>
  );
}
