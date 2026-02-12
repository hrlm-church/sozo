import { signIn } from "@/auth";

export default function LoginPage() {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100vh",
        background: "#fff",
      }}
    >
      <div style={{ textAlign: "center", maxWidth: 340 }}>
        <h1 style={{
          fontSize: "2.5rem", fontWeight: 700,
          background: "linear-gradient(135deg, #0693e3 0%, #9b51e0 100%)",
          WebkitBackgroundClip: "text", WebkitTextFillColor: "transparent",
          letterSpacing: "-0.04em", margin: "0 0 6px",
        }}>
          Sozo
        </h1>
        <p style={{
          fontSize: "0.88rem", color: "#86868b", marginBottom: 36,
        }}>
          Ministry Intelligence Platform
        </p>

        <form
          action={async () => {
            "use server";
            await signIn("microsoft-entra-id", { redirectTo: "/" });
          }}
        >
          <button
            type="submit"
            style={{
              width: "100%",
              padding: "12px 24px",
              fontSize: "0.88rem",
              fontWeight: 500,
              color: "#fff",
              background: "linear-gradient(135deg, #0693e3 0%, #9b51e0 100%)",
              border: "none",
              borderRadius: 9999,
              cursor: "pointer",
              transition: "opacity 200ms ease",
              letterSpacing: "-0.01em",
            }}
          >
            Sign in with Microsoft
          </button>
        </form>

        <p style={{ fontSize: "0.72rem", color: "#86868b", marginTop: 28 }}>
          Pure Freedom Ministries
        </p>
      </div>
    </div>
  );
}
