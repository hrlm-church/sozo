/**
 * User context: resolves the authenticated user's org membership and role.
 * Caches lookup for the duration of the request to avoid repeated SQL calls.
 */
import { executeSqlSafe } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";

export type UserRole = "admin" | "analyst" | "viewer";

export interface UserContext {
  email: string;
  orgId: string;
  orgName: string;
  orgSlug: string;
  role: UserRole;
}

/** Role hierarchy: higher number = more permissions */
export const ROLE_LEVEL: Record<UserRole, number> = {
  viewer: 1,
  analyst: 2,
  admin: 3,
};

/** Check if a role meets a minimum required level */
export function hasMinRole(userRole: UserRole, requiredRole: UserRole): boolean {
  return ROLE_LEVEL[userRole] >= ROLE_LEVEL[requiredRole];
}

// Per-request cache (module-level, cleared on each cold start)
// In serverless, each request gets a fresh module scope anyway.
// For long-lived servers, the cache is keyed by email.
const contextCache = new Map<string, { ctx: UserContext; ts: number }>();
const CACHE_TTL_MS = 60_000; // 1 minute

/**
 * Get the current user's org context from their session email.
 * Returns null if:
 *   - Not authenticated (no session)
 *   - Not a member of any active org
 */
export async function getUserContext(): Promise<UserContext | null> {
  const email = await getSessionEmail();
  if (!email) return null;

  // Check cache
  const cached = contextCache.get(email);
  if (cached && Date.now() - cached.ts < CACHE_TTL_MS) {
    return cached.ctx;
  }

  const result = await executeSqlSafe(
    `SELECT m.org_id, m.role, o.name AS org_name, o.slug AS org_slug
     FROM sozo.org_member m
     JOIN sozo.organization o ON o.id = m.org_id
     WHERE m.email = @email
       AND m.is_active = 1
       AND o.is_active = 1`,
    { email },
  );

  if (!result.ok || result.rows.length === 0) return null;

  const row = result.rows[0];
  const ctx: UserContext = {
    email,
    orgId: row.org_id as string,
    orgName: row.org_name as string,
    orgSlug: row.org_slug as string,
    role: row.role as UserRole,
  };

  contextCache.set(email, { ctx, ts: Date.now() });
  return ctx;
}

/** Clear the context cache (useful after role changes) */
export function clearContextCache(email?: string) {
  if (email) {
    contextCache.delete(email);
  } else {
    contextCache.clear();
  }
}
