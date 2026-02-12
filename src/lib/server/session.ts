import { auth } from "@/auth";

/** Get the current user's email from the session, or null if not authenticated. */
export async function getSessionEmail(): Promise<string | null> {
  const session = await auth();
  return session?.user?.email ?? null;
}
