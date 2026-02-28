/**
 * Role-based access control middleware for API routes.
 * Wraps a route handler and enforces a minimum role requirement.
 *
 * Usage:
 *   export const POST = withRoleCheck("analyst", async (request, ctx) => { ... });
 *
 * The UserContext is injected as the second argument so handlers
 * don't need to call getUserContext() themselves.
 */
import { NextResponse } from "next/server";
import { getUserContext, hasMinRole, type UserRole, type UserContext } from "@/lib/server/user-context";

type RoleGuardedHandler = (
  request: Request,
  ctx: UserContext,
  ...args: unknown[]
) => Promise<Response>;

/**
 * Wrap a route handler with role enforcement.
 * Returns 401 if not authenticated, 403 if insufficient role.
 */
export function withRoleCheck(
  minimumRole: UserRole,
  handler: RoleGuardedHandler,
): (request: Request, ...args: unknown[]) => Promise<Response> {
  return async (request: Request, ...args: unknown[]): Promise<Response> => {
    const ctx = await getUserContext();

    if (!ctx) {
      return NextResponse.json(
        { error: "Authentication required" },
        { status: 401 },
      );
    }

    if (!hasMinRole(ctx.role, minimumRole)) {
      return NextResponse.json(
        {
          error: "Insufficient permissions",
          required: minimumRole,
          current: ctx.role,
        },
        { status: 403 },
      );
    }

    return handler(request, ctx, ...args);
  };
}
