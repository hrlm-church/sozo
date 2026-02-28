/**
 * Admin token usage API.
 * GET — view current month's usage and budget status
 */
import { NextResponse } from "next/server";
import { withRoleCheck } from "@/lib/server/role-guard";
import { withAuditLog } from "@/lib/server/audit";
import { getBudgetStatus } from "@/lib/server/token-budget";
import { executeSqlSafe } from "@/lib/server/sql-client";

export const dynamic = "force-dynamic";

export const GET = withAuditLog(
  "/api/admin/usage",
  withRoleCheck("admin", async (_request, ctx) => {
    const [budget, topUsers] = await Promise.all([
      getBudgetStatus(ctx.orgId),
      executeSqlSafe(
        `SELECT TOP (10)
           user_email,
           SUM(input_tokens + output_tokens) AS total_tokens,
           COUNT(*) AS request_count
         FROM sozo.token_usage
         WHERE org_id = @orgId
           AND created_at >= DATEADD(DAY, 1 - DAY(SYSUTCDATETIME()), CAST(SYSUTCDATETIME() AS DATE))
         GROUP BY user_email
         ORDER BY total_tokens DESC`,
        { orgId: ctx.orgId },
      ),
    ]);

    return NextResponse.json({
      budget: budget ?? { error: "No budget configured" },
      topUsers: topUsers.ok ? topUsers.rows : [],
    });
  }),
);
