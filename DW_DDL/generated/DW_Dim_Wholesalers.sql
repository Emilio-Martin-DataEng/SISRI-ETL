-- Safe regeneration template for DW.Dim_Wholesalers
-- Generated at 2026-03-05 09:45:40

-- Drop old backup if it exists (prevents rename conflict)
IF OBJECT_ID('DW.Dim_Wholesalers_backup_20260305_094540', 'U') IS NOT NULL
    DROP TABLE [DW].[Dim_Wholesalers_backup_20260305_094540];
GO

-- Preserve data and rename old table if it exists
IF OBJECT_ID('DW.Dim_Wholesalers', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes safely
    IF EXISTS (SELECT * FROM sys.key_constraints 
               WHERE name = 'PK_Dim_Wholesalers' 
               AND parent_object_id = OBJECT_ID('DW.Dim_Wholesalers'))
        ALTER TABLE [DW].[Dim_Wholesalers] DROP CONSTRAINT PK_Dim_Wholesalers;
    
    IF EXISTS (SELECT * FROM sys.indexes 
               WHERE name = 'UIX_NK_Dim_Wholesalers_Active' 
               AND object_id = OBJECT_ID('DW.Dim_Wholesalers'))
        DROP INDEX [UIX_NK_Dim_Wholesalers_Active] ON [DW].[Dim_Wholesalers];
    
    -- Rename old table to backup (preserves data)
    EXEC sp_rename 'DW.Dim_Wholesalers', 'Dim_Wholesalers_backup_20260305_094540';
END
GO

-- Create new table
CREATE TABLE [DW].[Dim_Wholesalers] (
    [Wholesalers_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Wholesalers] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Row_Change_Reason] VARCHAR(50) NULL
    , [Wholesaler_Code] VARCHAR(255) NOT NULL
, [Wholesaler_Name] VARCHAR(255) NOT NULL
, [Wholesaler_Address] VARCHAR(255) NOT NULL
, [Wholesaler_City] VARCHAR(255) NOT NULL
, [Wholesaler_Province] VARCHAR(255) NOT NULL
, [Wholesaler_Country] VARCHAR(255) NOT NULL
);
GO

-- Restore data with IDENTITY_INSERT to preserve SKs
IF OBJECT_ID('DW.Dim_Wholesalers_backup_20260305_094540', 'U') IS NOT NULL
BEGIN
    SET IDENTITY_INSERT [DW].[Dim_Wholesalers] ON;
    
    INSERT INTO [DW].[Dim_Wholesalers] ([Wholesalers_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Wholesaler_Code], [Wholesaler_Name], [Wholesaler_Address], [Wholesaler_City], [Wholesaler_Province], [Wholesaler_Country])
    SELECT [Wholesalers_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Wholesaler_Code], [Wholesaler_Name], [Wholesaler_Address], [Wholesaler_City], [Wholesaler_Province], [Wholesaler_Country]
    FROM [DW].[Dim_Wholesalers_backup_20260305_094540];
    
    SET IDENTITY_INSERT [DW].[Dim_Wholesalers] OFF;
END
GO

-- Add unique index on active natural keys
CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Wholesalers_Active] ON [DW].[Dim_Wholesalers] ([Wholesaler_Code], [Wholesaler_Name], [Wholesaler_Address], [Wholesaler_City], [Wholesaler_Province], [Wholesaler_Country]) WHERE [Is_Deleted] = 0;
GO