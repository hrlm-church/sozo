-- =============================================================
-- Sozo v2 Intelligence Tables — Phase 2
-- Run against the Sozo Azure SQL database
-- =============================================================

-- ─── Daily Briefing ──────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'briefing' AND schema_id = SCHEMA_ID('sozo'))
BEGIN
  CREATE TABLE sozo.briefing (
    id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    owner_email     NVARCHAR(256)    NOT NULL,
    briefing_date   DATE             NOT NULL DEFAULT CAST(SYSUTCDATETIME() AS DATE),
    content_json    NVARCHAR(MAX)    NOT NULL,  -- AI-generated narrative + sections
    metrics_json    NVARCHAR(MAX)    NULL,       -- Raw metric snapshots
    action_count    INT              NOT NULL DEFAULT 0,
    created_at      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
  );

  CREATE INDEX IX_briefing_owner_date ON sozo.briefing (owner_email, briefing_date DESC);
END;

-- ─── Action Queue ────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'action' AND schema_id = SCHEMA_ID('sozo'))
BEGIN
  CREATE TABLE sozo.action (
    id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    owner_email     NVARCHAR(256)    NOT NULL,
    title           NVARCHAR(500)    NOT NULL,
    description     NVARCHAR(MAX)    NULL,
    action_type     NVARCHAR(50)     NOT NULL DEFAULT 'general',  -- call, email, thank, reengage, review, general
    priority_score  FLOAT            NOT NULL DEFAULT 50,          -- 0-100, higher = more urgent
    person_id       INT              NULL,                          -- optional link to person
    person_name     NVARCHAR(256)    NULL,
    status          NVARCHAR(20)     NOT NULL DEFAULT 'pending',   -- pending, in_progress, completed, dismissed
    source          NVARCHAR(50)     NOT NULL DEFAULT 'ai',        -- ai, briefing, manual
    due_date        DATE             NULL,
    outcome         NVARCHAR(500)    NULL,
    outcome_value   DECIMAL(12,2)    NULL,
    outcome_date    DATE             NULL,
    created_at      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME(),
    updated_at      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
  );

  CREATE INDEX IX_action_owner_status_priority ON sozo.action (owner_email, status, priority_score DESC);
  CREATE INDEX IX_action_person ON sozo.action (person_id) WHERE person_id IS NOT NULL;
END;

-- ─── Alerts ──────────────────────────────────────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'alert' AND schema_id = SCHEMA_ID('sozo'))
BEGIN
  CREATE TABLE sozo.alert (
    id              UNIQUEIDENTIFIER NOT NULL DEFAULT NEWID() PRIMARY KEY,
    owner_email     NVARCHAR(256)    NOT NULL,
    alert_type      NVARCHAR(50)     NOT NULL,  -- churn_risk, milestone, anomaly, giving_drop, new_donor
    severity        NVARCHAR(20)     NOT NULL DEFAULT 'info',  -- info, warning, critical
    title           NVARCHAR(500)    NOT NULL,
    body            NVARCHAR(MAX)    NULL,
    person_id       INT              NULL,
    person_name     NVARCHAR(256)    NULL,
    is_read         BIT              NOT NULL DEFAULT 0,
    created_at      DATETIME2        NOT NULL DEFAULT SYSUTCDATETIME()
  );

  CREATE INDEX IX_alert_owner_unread ON sozo.alert (owner_email, is_read, created_at DESC);
END;

-- ─── Tag Summary (materialized aggregation) ──────────────────
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'tag_summary' AND schema_id = SCHEMA_ID('serving'))
BEGIN
  CREATE TABLE serving.tag_summary (
    person_id       INT              NOT NULL,
    display_name    NVARCHAR(256)    NOT NULL,
    tag_group       NVARCHAR(256)    NOT NULL,
    tag_count       INT              NOT NULL DEFAULT 0,
    distinct_tags   INT              NOT NULL DEFAULT 0,
    most_recent_tag NVARCHAR(512)    NULL,
    most_recent_at  DATETIME2        NULL,
    PRIMARY KEY (person_id, tag_group)
  );

  CREATE INDEX IX_tag_summary_group ON serving.tag_summary (tag_group, tag_count DESC);
END;

PRINT 'Phase 2 intelligence tables created successfully.';
GO
