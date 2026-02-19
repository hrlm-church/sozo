import NextAuth from "next-auth";
import MicrosoftEntraID from "next-auth/providers/microsoft-entra-id";
import Credentials from "next-auth/providers/credentials";

export const { handlers, auth, signIn, signOut } = NextAuth({
  providers: [
    MicrosoftEntraID({
      clientId: process.env.AUTH_MICROSOFT_ENTRA_ID_ID,
      clientSecret: process.env.AUTH_MICROSOFT_ENTRA_ID_SECRET,
      issuer: process.env.AUTH_MICROSOFT_ENTRA_ID_ISSUER,
      authorization: {
        params: { scope: "openid profile email User.Read" },
      },
      // Use state check instead of PKCE to avoid "pkceCodeVerifier could not be parsed" errors
      checks: ["state"],
    }),
    Credentials({
      credentials: {
        username: {},
        password: {},
      },
      async authorize(credentials) {
        const validUser = process.env.AUTH_USERNAME || "sozo";
        const validPass = process.env.AUTH_PASSWORD || "sozo2025";

        if (
          credentials?.username === validUser &&
          credentials?.password === validPass
        ) {
          return {
            id: "shared-user",
            name: String(credentials.username),
            email: `${credentials.username}@sozo.local`,
          };
        }
        return null;
      },
    }),
  ],
  pages: {
    signIn: "/login",
  },
  callbacks: {
    authorized({ auth: session, request: { nextUrl } }) {
      const isLoggedIn = !!session?.user;
      const isLoginPage = nextUrl.pathname === "/login";
      const isAuthApi = nextUrl.pathname.startsWith("/api/auth");
      const isHealthApi = nextUrl.pathname === "/api/health";

      // Always allow auth routes and health check
      if (isAuthApi || isHealthApi) return true;
      // TODO: Remove before production — allows unauthenticated chat API access for testing
      if (nextUrl.pathname === "/api/chat") return true;

      // Redirect logged-in users away from login page
      if (isLoginPage) {
        if (isLoggedIn) return Response.redirect(new URL("/", nextUrl));
        return true;
      }

      // Protect everything else
      return isLoggedIn;
    },
  },
});
