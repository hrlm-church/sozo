/**
 * Admin user management API.
 * GET  — list org members
 * POST — invite a new user
 * All operations require admin role.
 */
import { NextResponse } from "next/server";
import { executeSqlSafe } from "@/lib/server/sql-client";
import { withRoleCheck } from "@/lib/server/role-guard";
import { withAuditLog } from "@/lib/server/audit";
import { clearContextCache } from "@/lib/server/user-context";
import { z } from "zod";

export const dynamic = "force-dynamic";

/** List all org members */
export const GET = withAuditLog(
  "/api/admin/users",
  withRoleCheck("admin", async (_request, ctx) => {
    const result = await executeSqlSafe(
      `SELECT m.id, m.email, m.role, m.is_active, m.created_at, m.invited_by
       FROM sozo.org_member m
       WHERE m.org_id = @orgId
       ORDER BY m.created_at ASC`,
      { orgId: ctx.orgId },
    );

    if (!result.ok) {
      return NextResponse.json({ error: result.reason }, { status: 500 });
    }

    return NextResponse.json({ members: result.rows });
  }),
);

const InviteSchema = z.object({
  email: z.string().email().max(256),
  role: z.enum(["admin", "analyst", "viewer"]),
});

/** Invite a new user to the org */
export const POST = withAuditLog(
  "/api/admin/users",
  withRoleCheck("admin", async (request, ctx) => {
    const body = await request.json();
    const parsed = InviteSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: "Valid email and role (admin/analyst/viewer) required", details: parsed.error.issues },
        { status: 400 },
      );
    }

    const { email, role } = parsed.data;

    // Check if already a member
    const existing = await executeSqlSafe(
      `SELECT id, is_active FROM sozo.org_member
       WHERE org_id = @orgId AND email = @email`,
      { orgId: ctx.orgId, email },
    );

    if (existing.ok && existing.rows.length > 0) {
      const member = existing.rows[0];
      if (member.is_active) {
        return NextResponse.json(
          { error: "User is already a member of this organization" },
          { status: 409 },
        );
      }
      // Reactivate inactive member
      await executeSqlSafe(
        `UPDATE sozo.org_member
         SET is_active = 1, role = @role, updated_at = SYSUTCDATETIME()
         WHERE id = @id`,
        { id: member.id as string, role },
      );
      return NextResponse.json({ id: member.id, reactivated: true });
    }

    const id = crypto.randomUUID();
    await executeSqlSafe(
      `INSERT INTO sozo.org_member (id, org_id, email, role, invited_by)
       VALUES (@id, @orgId, @email, @role, @invitedBy)`,
      { id, orgId: ctx.orgId, email, role, invitedBy: ctx.email },
    );

    return NextResponse.json({ id, invited: true });
  }),
);

const UpdateSchema = z.object({
  memberId: z.string().min(1).max(36),
  role: z.enum(["admin", "analyst", "viewer"]).optional(),
  isActive: z.boolean().optional(),
});

/** Update a member's role or deactivate them */
export const PATCH = withAuditLog(
  "/api/admin/users",
  withRoleCheck("admin", async (request, ctx) => {
    const body = await request.json();
    const parsed = UpdateSchema.safeParse(body);
    if (!parsed.success) {
      return NextResponse.json(
        { error: "memberId required, plus role or isActive", details: parsed.error.issues },
        { status: 400 },
      );
    }

    const { memberId, role, isActive } = parsed.data;

    // Verify member belongs to same org
    const member = await executeSqlSafe(
      `SELECT email FROM sozo.org_member WHERE id = @id AND org_id = @orgId`,
      { id: memberId, orgId: ctx.orgId },
    );

    if (!member.ok || member.rows.length === 0) {
      return NextResponse.json({ error: "Member not found" }, { status: 404 });
    }

    // Prevent admins from demoting themselves (must have at least 1 admin)
    const memberEmail = member.rows[0].email as string;
    if (memberEmail === ctx.email && role && role !== "admin") {
      return NextResponse.json(
        { error: "Cannot demote yourself. Ask another admin to change your role." },
        { status: 400 },
      );
    }

    const updates: string[] = [];
    const params: Record<string, string | number | boolean | null> = { id: memberId };

    if (role !== undefined) {
      updates.push("role = @role");
      params.role = role;
    }
    if (isActive !== undefined) {
      updates.push("is_active = @isActive");
      params.isActive = isActive;
    }

    if (updates.length === 0) {
      return NextResponse.json({ error: "No updates provided" }, { status: 400 });
    }

    updates.push("updated_at = SYSUTCDATETIME()");

    await executeSqlSafe(
      `UPDATE sozo.org_member SET ${updates.join(", ")} WHERE id = @id`,
      params,
    );

    // Clear context cache for the affected user
    clearContextCache(memberEmail);

    return NextResponse.json({ updated: true });
  }),
);
