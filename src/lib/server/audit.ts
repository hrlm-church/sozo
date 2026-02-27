/**
 * Audit logging middleware for API routes.
 * Logs every request to audit.api_log with timing, status, and user context.
 * Fire-and-forget — never blocks or fails the actual request.
 */
import { NextResponse } from "next/server";
import { executeSqlSafe } from "@/lib/server/sql-client";
import { getSessionEmail } from "@/lib/server/session";
import { createLogger, generateRequestId } from "@/lib/server/logger";

interface AuditEntry {
  requestId: string;
  method: string;
  path: string;
  statusCode: number;
  durationMs: number;
  userEmail: string | null;
  ipAddress: string | null;
  userAgent: string | null;
  errorMessage: string | null;
}

/** Write an audit log entry (fire-and-forget, never throws) */
async function writeAuditLog(entry: AuditEntry): Promise<void> {
  try {
    await executeSqlSafe(
      `INSERT INTO audit.api_log
       (request_id, method, path, status_code, duration_ms, user_email, ip_address, user_agent, error_message)
       VALUES (@requestId, @method, @path, @statusCode, @durationMs, @userEmail, @ipAddress, @userAgent, @errorMessage)`,
      {
        requestId: entry.requestId,
        method: entry.method.slice(0, 10),
        path: entry.path.slice(0, 500),
        statusCode: entry.statusCode,
        durationMs: entry.durationMs,
        userEmail: entry.userEmail,
        ipAddress: entry.ipAddress,
        userAgent: entry.userAgent?.slice(0, 500) ?? null,
        errorMessage: entry.errorMessage?.slice(0, 1000) ?? null,
      },
    );
  } catch {
    // Never let audit logging break the request
  }
}

type RouteHandler = (request: Request, ...args: unknown[]) => Promise<Response>;

/**
 * Wrap a route handler with audit logging.
 * Usage: export const POST = withAuditLog("/api/conversation/save", handler);
 */
export function withAuditLog(path: string, handler: RouteHandler): RouteHandler {
  return async (request: Request, ...args: unknown[]): Promise<Response> => {
    const requestId = generateRequestId();
    const startTime = Date.now();
    const log = createLogger({ requestId });
    const method = request.method;

    let userEmail: string | null = null;
    let statusCode = 500;
    let errorMessage: string | null = null;

    try {
      userEmail = await getSessionEmail();
      const response = await handler(request, ...args);
      statusCode = response.status;

      // Extract error from 4xx/5xx responses
      if (statusCode >= 400) {
        try {
          const clone = response.clone();
          const body = await clone.json();
          errorMessage = body?.error ?? null;
        } catch {
          // Response may not be JSON
        }
      }

      return response;
    } catch (error) {
      errorMessage = error instanceof Error ? error.message : String(error);
      log.error("Unhandled route error", { path, error: errorMessage });
      return NextResponse.json(
        { error: errorMessage ?? "Internal server error" },
        { status: 500 },
      );
    } finally {
      const durationMs = Date.now() - startTime;

      // Extract client IP and user-agent
      const ipAddress =
        request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() ??
        request.headers.get("x-real-ip") ??
        null;
      const userAgent = request.headers.get("user-agent");

      // Structured log
      log.info("API request", {
        method,
        path,
        statusCode,
        durationMs,
        userId: userEmail ?? undefined,
      });

      // Fire-and-forget to SQL
      writeAuditLog({
        requestId,
        method,
        path,
        statusCode,
        durationMs,
        userEmail,
        ipAddress,
        userAgent,
        errorMessage,
      });
    }
  };
}
