-- Safe regeneration template for DW.Dim_Principals
-- Generated at 2026-03-04 14:57:17

-- Drop old backup if it exists (prevents rename conflict)
IF OBJECT_ID('DW.Dim_Principals_backup_20260304_145717', 'U') IS NOT NULL
    DROP TABLE [DW].[Dim_Principals_backup_20260304_145717];
GO

-- Preserve data and rename old table if it exists
IF OBJECT_ID('DW.Dim_Principals', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes safely
    IF EXISTS (SELECT * FROM sys.key_constraints 
               WHERE name = 'PK_Dim_Principals' 
               AND parent_object_id = OBJECT_ID('DW.Dim_Principals'))
        ALTER TABLE [DW].[Dim_Principals] DROP CONSTRAINT PK_Dim_Principals;
    
    IF EXISTS (SELECT * FROM sys.indexes 
               WHERE name = 'UIX_NK_Dim_Principals_Active' 
               AND object_id = OBJECT_ID('DW.Dim_Principals'))
        DROP INDEX [UIX_NK_Dim_Principals_Active] ON [DW].[Dim_Principals];
    
    -- Rename old table to backup (preserves data)
    EXEC sp_rename 'DW.Dim_Principals', 'Dim_Principals_backup_20260304_145717';
END
GO

-- Create new table
CREATE TABLE [DW].[Dim_Principals] (
    [Principals_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Principals] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Row_Change_Reason] VARCHAR(50) NULL
    , [Principal_Code] VARCHAR(500) NOT NULL
, [Principal_Name] VARCHAR(500) NOT NULL
, [Principal_Trading_As_Name] VARCHAR(500) NOT NULL
, [Principal_Address] VARCHAR(500) NOT NULL
, [Principal_City] VARCHAR(500) NOT NULL
, [Principal_Province] VARCHAR(500) NOT NULL
, [Principal_Country] VARCHAR(500) NOT NULL
);
GO

-- Restore data with IDENTITY_INSERT to preserve SKs
IF OBJECT_ID('DW.Dim_Principals_backup_20260304_145717', 'U') IS NOT NULL
BEGIN
    SET IDENTITY_INSERT [DW].[Dim_Principals] ON;
    
    INSERT INTO [DW].[Dim_Principals] ([Principals_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country])
    SELECT [Principals_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country]
    FROM [DW].[Dim_Principals_backup_20260304_145717];
    
    SET IDENTITY_INSERT [DW].[Dim_Principals] OFF;
END
GO

-- Add unique index on active natural keys
CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Principals_Active] ON [DW].[Dim_Principals] ([Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country]) WHERE [Is_Deleted] = 0;
GO