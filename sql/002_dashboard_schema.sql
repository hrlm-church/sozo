-- Dashboard persistence schema
-- Run via: node scripts/setup/02_dashboard_schema.js

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'dashboard')
  EXEC('CREATE SCHEMA dashboard');
GO

-- Saved dashboards
IF OBJECT_ID('dashboard.saved_dashboard', 'U') IS NULL
CREATE TABLE dashboard.saved_dashboard (
  id            NVARCHAR(36)   NOT NULL PRIMARY KEY,
  name          NVARCHAR(200)  NOT NULL,
  owner_email   NVARCHAR(254)  NOT NULL,
  created_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  updated_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_dashboard_owner')
  CREATE INDEX IX_dashboard_owner ON dashboard.saved_dashboard (owner_email);
GO

-- Individual widgets within a dashboard
IF OBJECT_ID('dashboard.widget', 'U') IS NULL
CREATE TABLE dashboard.widget (
  id            NVARCHAR(36)   NOT NULL PRIMARY KEY,
  dashboard_id  NVARCHAR(36)   NOT NULL
    REFERENCES dashboard.saved_dashboard(id) ON DELETE CASCADE,
  type          NVARCHAR(30)   NOT NULL,
  title         NVARCHAR(200)  NOT NULL,
  sql_query     NVARCHAR(MAX)  NULL,
  config_json   NVARCHAR(MAX)  NOT NULL,
  data_json     NVARCHAR(MAX)  NULL,
  layout_x      INT            NOT NULL DEFAULT 0,
  layout_y      INT            NOT NULL DEFAULT 0,
  layout_w      INT            NOT NULL DEFAULT 6,
  layout_h      INT            NOT NULL DEFAULT 4,
  created_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

-- Chat sessions (optional, for replaying conversations)
IF OBJECT_ID('dashboard.chat_session', 'U') IS NULL
CREATE TABLE dashboard.chat_session (
  id            NVARCHAR(36)   NOT NULL PRIMARY KEY,
  owner_email   NVARCHAR(254)  NOT NULL,
  title         NVARCHAR(200)  NULL,
  messages_json NVARCHAR(MAX)  NOT NULL,
  created_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
  updated_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME()
);
GO
