-- Sozo Data Platform — Fresh Schema
-- Drops all existing pipeline schemas and creates clean domain-oriented tables.
-- Run via: node scripts/setup/01_create_schema.js

-- ============================================================
-- Drop existing schemas (clean slate)
-- ============================================================
IF SCHEMA_ID('serving') IS NOT NULL BEGIN
  DECLARE @s1 NVARCHAR(MAX) = '';
  SELECT @s1 = @s1 + 'DROP TABLE IF EXISTS [serving].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'serving';
  EXEC sp_executesql @s1;
END;

IF SCHEMA_ID('intel') IS NOT NULL BEGIN
  DECLARE @s2 NVARCHAR(MAX) = '';
  SELECT @s2 = @s2 + 'DROP TABLE IF EXISTS [intel].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'intel';
  EXEC sp_executesql @s2;
END;

IF SCHEMA_ID('engagement') IS NOT NULL BEGIN
  DECLARE @s3 NVARCHAR(MAX) = '';
  SELECT @s3 = @s3 + 'DROP TABLE IF EXISTS [engagement].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'engagement';
  EXEC sp_executesql @s3;
END;

IF SCHEMA_ID('event') IS NOT NULL BEGIN
  DECLARE @s4 NVARCHAR(MAX) = '';
  SELECT @s4 = @s4 + 'DROP TABLE IF EXISTS [event].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'event';
  EXEC sp_executesql @s4;
END;

IF SCHEMA_ID('commerce') IS NOT NULL BEGIN
  DECLARE @s5 NVARCHAR(MAX) = '';
  SELECT @s5 = @s5 + 'DROP TABLE IF EXISTS [commerce].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'commerce';
  EXEC sp_executesql @s5;
END;

IF SCHEMA_ID('giving') IS NOT NULL BEGIN
  DECLARE @s6 NVARCHAR(MAX) = '';
  SELECT @s6 = @s6 + 'DROP TABLE IF EXISTS [giving].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'giving';
  EXEC sp_executesql @s6;
END;

IF SCHEMA_ID('household') IS NOT NULL BEGIN
  DECLARE @s7 NVARCHAR(MAX) = '';
  SELECT @s7 = @s7 + 'DROP TABLE IF EXISTS [household].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'household';
  EXEC sp_executesql @s7;
END;

IF SCHEMA_ID('person') IS NOT NULL BEGIN
  DECLARE @s8 NVARCHAR(MAX) = '';
  SELECT @s8 = @s8 + 'DROP TABLE IF EXISTS [person].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'person';
  EXEC sp_executesql @s8;
END;

IF SCHEMA_ID('raw') IS NOT NULL BEGIN
  DECLARE @s9 NVARCHAR(MAX) = '';
  SELECT @s9 = @s9 + 'DROP TABLE IF EXISTS [raw].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'raw';
  EXEC sp_executesql @s9;
END;

IF SCHEMA_ID('staging') IS NOT NULL BEGIN
  DECLARE @s10 NVARCHAR(MAX) = '';
  SELECT @s10 = @s10 + 'DROP TABLE IF EXISTS [staging].[' + t.name + ']; '
  FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = 'staging';
  EXEC sp_executesql @s10;
END;

-- Drop old pipeline schemas
DECLARE @oldSchemas TABLE (name NVARCHAR(64));
INSERT INTO @oldSchemas VALUES ('gold_intel'),('gold'),('silver'),('bronze'),('meta');
DECLARE @oname NVARCHAR(64);
DECLARE old_cur CURSOR FOR SELECT name FROM @oldSchemas;
OPEN old_cur;
FETCH NEXT FROM old_cur INTO @oname;
WHILE @@FETCH_STATUS = 0 BEGIN
  IF SCHEMA_ID(@oname) IS NOT NULL BEGIN
    DECLARE @od NVARCHAR(MAX) = '';
    SELECT @od = @od + 'DROP TABLE IF EXISTS [' + @oname + '].[' + t.name + ']; '
    FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE s.name = @oname;
    IF LEN(@od) > 0 EXEC sp_executesql @od;
  END;
  FETCH NEXT FROM old_cur INTO @oname;
END;
CLOSE old_cur;
DEALLOCATE old_cur;
