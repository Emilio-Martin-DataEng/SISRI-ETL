-- Safe regeneration template for DW.Dim_Brands
-- Generated at 2026-03-04 14:57:18

-- Drop old backup if it exists (prevents rename conflict)
IF OBJECT_ID('DW.Dim_Brands_backup_20260304_145717', 'U') IS NOT NULL
    DROP TABLE [DW].[Dim_Brands_backup_20260304_145717];
GO

-- Preserve data and rename old table if it exists
IF OBJECT_ID('DW.Dim_Brands', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes safely
    IF EXISTS (SELECT * FROM sys.key_constraints 
               WHERE name = 'PK_Dim_Brands' 
               AND parent_object_id = OBJECT_ID('DW.Dim_Brands'))
        ALTER TABLE [DW].[Dim_Brands] DROP CONSTRAINT PK_Dim_Brands;
    
    IF EXISTS (SELECT * FROM sys.indexes 
               WHERE name = 'UIX_NK_Dim_Brands_Active' 
               AND object_id = OBJECT_ID('DW.Dim_Brands'))
        DROP INDEX [UIX_NK_Dim_Brands_Active] ON [DW].[Dim_Brands];
    
    -- Rename old table to backup (preserves data)
    EXEC sp_rename 'DW.Dim_Brands', 'Dim_Brands_backup_20260304_145717';
END
GO

-- Create new table
CREATE TABLE [DW].[Dim_Brands] (
    [Brands_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Brands] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Row_Change_Reason] VARCHAR(50) NULL
    , [Principal_Code] VARCHAR(500) NOT NULL
, [Brand_Code] VARCHAR(500) NOT NULL
, [Brand_Name] VARCHAR(500) NOT NULL
);
GO

-- Restore data with IDENTITY_INSERT to preserve SKs
IF OBJECT_ID('DW.Dim_Brands_backup_20260304_145717', 'U') IS NOT NULL
BEGIN
    SET IDENTITY_INSERT [DW].[Dim_Brands] ON;
    
    INSERT INTO [DW].[Dim_Brands] ([Brands_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Principal_Code], [Brand_Code], [Brand_Name])
    SELECT [Brands_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Principal_Code], [Brand_Code], [Brand_Name]
    FROM [DW].[Dim_Brands_backup_20260304_145717];
    
    SET IDENTITY_INSERT [DW].[Dim_Brands] OFF;
END
GO

-- Add unique index on active natural keys
CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Brands_Active] ON [DW].[Dim_Brands] ([Principal_Code], [Brand_Code], [Brand_Name]) WHERE [Is_Deleted] = 0;
GO