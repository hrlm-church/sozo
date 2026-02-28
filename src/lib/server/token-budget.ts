/**
 * Token budget tracking and enforcement.
 *
 * - Tracks input/output tokens per request
 * - Enforces monthly org-level budgets
 * - Alerts at configurable threshold (default 80%)
 */
import { executeSqlSafe } from "@/lib/server/sql-client";

export interface TokenUsage {
  orgId: string;
  userEmail: string;
  inputTokens: number;
  outputTokens: number;
  modelName?: string;
  requestId?: string;
}

export interface BudgetStatus {
  orgId: string;
  monthlyLimit: number;
  usedTokens: number;
  remainingTokens: number;
  usagePercent: number;
  isEnforced: boolean;
  alertThreshold: number;
  isOverBudget: boolean;
  isNearBudget: boolean;
}

/**
 * Record token usage for a chat request (fire-and-forget).
 */
export async function recordTokenUsage(usage: TokenUsage): Promise<void> {
  try {
    await executeSqlSafe(
      `INSERT INTO sozo.token_usage (org_id, user_email, input_tokens, output_tokens, model_name, request_id)
       VALUES (@orgId, @userEmail, @inputTokens, @outputTokens, @modelName, @requestId)`,
      {
        orgId: usage.orgId,
        userEmail: usage.userEmail,
        inputTokens: usage.inputTokens,
        outputTokens: usage.outputTokens,
        modelName: usage.modelName ?? null,
        requestId: usage.requestId ?? null,
      },
    );
  } catch {
    // Fire-and-forget — don't block the response
  }
}

/**
 * Check the current month's budget status for an org.
 */
export async function getBudgetStatus(orgId: string): Promise<BudgetStatus | null> {
  const result = await executeSqlSafe(
    `SELECT
       b.monthly_token_limit,
       b.alert_threshold,
       b.is_enforced,
       ISNULL(u.total_tokens, 0) AS used_tokens
     FROM sozo.org_budget b
     LEFT JOIN (
       SELECT org_id,
              SUM(input_tokens + output_tokens) AS total_tokens
       FROM sozo.token_usage
       WHERE org_id = @orgId
         AND created_at >= DATEADD(DAY, 1 - DAY(SYSUTCDATETIME()), CAST(SYSUTCDATETIME() AS DATE))
       GROUP BY org_id
     ) u ON u.org_id = b.org_id
     WHERE b.org_id = @orgId`,
    { orgId },
  );

  if (!result.ok || result.rows.length === 0) return null;

  const row = result.rows[0];
  const monthlyLimit = row.monthly_token_limit as number;
  const usedTokens = row.used_tokens as number;
  const alertThreshold = row.alert_threshold as number;
  const isEnforced = row.is_enforced as boolean;
  const remainingTokens = Math.max(0, monthlyLimit - usedTokens);
  const usagePercent = monthlyLimit > 0 ? usedTokens / monthlyLimit : 0;

  return {
    orgId,
    monthlyLimit,
    usedTokens,
    remainingTokens,
    usagePercent,
    isEnforced,
    alertThreshold,
    isOverBudget: isEnforced && usedTokens >= monthlyLimit,
    isNearBudget: usagePercent >= alertThreshold,
  };
}

/**
 * Quick check: is the org over budget? Returns false if budget not found or not enforced.
 */
export async function isOverBudget(orgId: string): Promise<boolean> {
  const status = await getBudgetStatus(orgId);
  return status?.isOverBudget ?? false;
}
