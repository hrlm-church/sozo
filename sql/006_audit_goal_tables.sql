-- =============================================================
-- Sozo v2 Audit & Goal Tables — Phase 3-4
-- Run against the Sozo Azure SQL database
-- =============================================================

-- ─── Audit Log (write-back tracking) ─────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'audit_log' AND schema_id = SCHEMA_ID('sozo'))
BEGIN
  CREATE TABLE sozo.audit_log (
    id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    owner_email     NVARCHAR(256)    NOT NULL,
    action          NVARCHAR(100)    NOT NULL,  -- tag_applied, note_created, email_drafted
    target_system   NVARCHAR(50)     NOT NULL,  -- keap, sozo, manual
    target_id       NVARCHAR(256)    NULL,       -- external system ID
    person_id       INT              NULL,
    person_name     NVARCHAR(256)    NULL,
    payload_json    NVARCHAR(MAX)    NULL,       -- request payload
    response_json   NVARCHAR(MAX)    NULL,       -- response from target system
    status          NVARCHAR(20)     NOT NULL DEFAULT 'pending',  -- pending, success, failed
    error_message   NVARCHAR(MAX)    NULL,
    created_at      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
  );

  CREATE INDEX IX_audit_log_owner ON sozo.audit_log (owner_email, created_at DESC);
  CREATE INDEX IX_audit_log_status ON sozo.audit_log (status, created_at DESC);
END;

-- ─── Goal Tracking ───────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'goal' AND schema_id = SCHEMA_ID('sozo'))
BEGIN
  CREATE TABLE sozo.goal (
    id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    owner_email     NVARCHAR(256)    NOT NULL,
    title           NVARCHAR(500)    NOT NULL,
    goal_type       NVARCHAR(50)     NOT NULL DEFAULT 'custom',  -- donors, revenue, retention, engagement, custom
    target_value    DECIMAL(12,2)    NOT NULL,
    current_value   DECIMAL(12,2)    NOT NULL DEFAULT 0,
    unit            NVARCHAR(20)     NULL,       -- $, %, count, etc.
    metric_query    NVARCHAR(MAX)    NULL,       -- SQL query to auto-update current_value
    target_date     DATE             NULL,
    status          NVARCHAR(20)     NOT NULL DEFAULT 'active',  -- active, completed, paused
    created_at      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
  );

  CREATE INDEX IX_goal_owner_status ON sozo.goal (owner_email, status);
END;

-- ─── User Role (simple role-based access) ────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'user_role' AND schema_id = SCHEMA_ID('sozo'))
BEGIN
  CREATE TABLE sozo.user_role (
    email           NVARCHAR(256)    NOT NULL PRIMARY KEY,
    role            NVARCHAR(20)     NOT NULL DEFAULT 'viewer',  -- admin, viewer
    created_at      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
  );
END;

PRINT 'Phase 3-4 audit, goal, and role tables created successfully.';
GO
