import { signIn } from "@/auth";

export default function LoginPage() {
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        height: "100vh",
        background: "#0f0f13",
        position: "relative",
        overflow: "hidden",
      }}
    >
      {/* Ambient gradient orbs */}
      <div
        style={{
          position: "absolute",
          width: 600,
          height: 600,
          borderRadius: "50%",
          background:
            "radial-gradient(circle, rgba(6,147,227,0.08) 0%, transparent 70%)",
          top: "-200px",
          right: "-100px",
          pointerEvents: "none",
        }}
      />
      <div
        style={{
          position: "absolute",
          width: 500,
          height: 500,
          borderRadius: "50%",
          background:
            "radial-gradient(circle, rgba(155,81,224,0.06) 0%, transparent 70%)",
          bottom: "-150px",
          left: "-100px",
          pointerEvents: "none",
        }}
      />

      <div
        style={{
          textAlign: "center",
          maxWidth: 380,
          padding: "48px 36px",
          background: "rgba(255, 255, 255, 0.03)",
          borderRadius: 20,
          border: "1px solid rgba(255, 255, 255, 0.06)",
          backdropFilter: "blur(20px)",
          position: "relative",
          zIndex: 1,
        }}
      >
        <h1
          style={{
            fontSize: "2.5rem",
            fontWeight: 700,
            background: "linear-gradient(135deg, #0693e3 0%, #9b51e0 100%)",
            WebkitBackgroundClip: "text",
            WebkitTextFillColor: "transparent",
            letterSpacing: "-0.04em",
            margin: "0 0 6px",
          }}
        >
          Sozo
        </h1>
        <p style={{ fontSize: "0.88rem", color: "#6b6b78", marginBottom: 36 }}>
          Ministry Intelligence Platform
        </p>

        {/* Microsoft Sign In */}
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
              background:
                "linear-gradient(135deg, #0693e3 0%, #9b51e0 100%)",
              border: "none",
              borderRadius: 9999,
              cursor: "pointer",
              transition: "opacity 200ms ease",
              letterSpacing: "-0.01em",
              marginBottom: 12,
            }}
          >
            Sign in with Microsoft
          </button>
        </form>

        {/* Google Sign In */}
        <form
          action={async () => {
            "use server";
            await signIn("google", { redirectTo: "/" });
          }}
        >
          <button
            type="submit"
            style={{
              width: "100%",
              padding: "12px 24px",
              fontSize: "0.88rem",
              fontWeight: 500,
              color: "#e8e8ed",
              background: "rgba(255, 255, 255, 0.06)",
              border: "1px solid rgba(255, 255, 255, 0.12)",
              borderRadius: 9999,
              cursor: "pointer",
              transition: "all 200ms ease",
              letterSpacing: "-0.01em",
            }}
          >
            Sign in with Google
          </button>
        </form>

        <p style={{ fontSize: "0.72rem", color: "#6b6b78", marginTop: 28 }}>
          Pure Freedom Ministries
        </p>
      </div>
    </div>
  );
}
