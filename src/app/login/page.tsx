import { signIn } from "@/auth";

export default function LoginPage() {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100vh",
        background: "linear-gradient(135deg, #0e0b1a 0%, #1a1333 50%, #0e0b1a 100%)",
      }}
    >
      <div
        style={{
          textAlign: "center",
          maxWidth: 380,
          padding: "48px 40px",
          background: "rgba(30, 25, 50, 0.85)",
          borderRadius: 16,
          border: "1px solid rgba(111, 67, 234, 0.25)",
        }}
      >
        {/* Brand */}
        <div
          style={{
            fontSize: "2.5rem",
            fontWeight: 800,
            background: "linear-gradient(135deg, #6f43ea 0%, #2f7ff6 100%)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
            marginBottom: 8,
          }}
        >
          Sozo
        </div>
        <p
          style={{
            fontSize: "0.85rem",
            color: "rgba(255,255,255,0.55)",
            marginBottom: 32,
          }}
        >
          Donor Intelligence Platform
        </p>

        {/* Sign-in form (server action) */}
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
              fontSize: "0.9rem",
              fontWeight: 600,
              color: "#fff",
              background: "linear-gradient(135deg, #6f43ea 0%, #2f7ff6 100%)",
              border: "none",
              borderRadius: 8,
              cursor: "pointer",
              transition: "opacity 0.2s",
            }}
          >
            Sign in with Microsoft
          </button>
        </form>

        <p
          style={{
            fontSize: "0.7rem",
            color: "rgba(255,255,255,0.3)",
            marginTop: 24,
          }}
        >
          Pure Freedom Ministries
        </p>
      </div>
    </div>
  );
}
