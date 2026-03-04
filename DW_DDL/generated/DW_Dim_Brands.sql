-- Safe regeneration for DW.Dim_Brands with SK preservation

-- Clean up any leftover temp table
IF OBJECT_ID('tempdb..#Temp_Dim_Brands') IS NOT NULL
    DROP TABLE #Temp_Dim_Brands;
GO

IF OBJECT_ID('DW.Dim_Brands', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes
    IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'PK_Dim_Brands' AND parent_object_id = OBJECT_ID('DW.Dim_Brands'))
        ALTER TABLE [DW].[Dim_Brands] DROP CONSTRAINT PK_Dim_Brands;
    
    IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'UIX_NK_Dim_Brands_Active' AND object_id = OBJECT_ID('DW.Dim_Brands'))
        DROP INDEX [UIX_NK_Dim_Brands_Active] ON [DW].[Dim_Brands];
    
    -- Rename old table to backup
    EXEC sp_rename 'DW.Dim_Brands', 'Dim_Brands_backup_20260304_120426';
END
GO

-- Create new table
CREATE TABLE [DW].[Dim_Brands] (
    [Brands_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Brands] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Brand_Code] VARCHAR(500) NOT NULL,
    [Brand_Name] VARCHAR(500) NOT NULL
);
GO

-- Restore data with IDENTITY_INSERT ON to preserve SKs
IF OBJECT_ID('DW.Dim_Brands_backup_20260304_120426', 'U') IS NOT NULL
BEGIN
    SET IDENTITY_INSERT [DW].[Dim_Brands] ON;
    
    INSERT INTO [DW].[Dim_Brands] ([Brands_SK], [Principal_Code], [Brand_Code], [Brand_Name], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted])
    SELECT [Brands_SK], [Principal_Code], [Brand_Code], [Brand_Name], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted] FROM [DW].[Dim_Brands_backup_20260304_120426];
    
    SET IDENTITY_INSERT [DW].[Dim_Brands] OFF;
    
    -- Optional: drop backup after success (comment out if you want to keep it)
    -- DROP TABLE [DW].[Dim_Brands_backup_20260304_120426];
END
GO

CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Brands_Active] ON [DW].[Dim_Brands] ([Principal_Code], [Brand_Code]) WHERE [Is_Deleted] = 0;
GO