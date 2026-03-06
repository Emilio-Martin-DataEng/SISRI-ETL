-- Safe regeneration template for DW.Dim_Products (SCD Type 1)
-- Generated at 2026-03-05 15:02:49

-- Drop old backup if it exists (prevents rename conflict)
IF OBJECT_ID('DW.Dim_Products_backup_20260305_150248', 'U') IS NOT NULL
    DROP TABLE [DW].[Dim_Products_backup_20260305_150248];
GO

-- Preserve data and rename old table if it exists
IF OBJECT_ID('DW.Dim_Products', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes safely
    IF EXISTS (SELECT * FROM sys.key_constraints 
               WHERE name = 'PK_Dim_Products' 
               AND parent_object_id = OBJECT_ID('DW.Dim_Products'))
        ALTER TABLE [DW].[Dim_Products] DROP CONSTRAINT PK_Dim_Products;
    
    IF EXISTS (SELECT * FROM sys.indexes 
               WHERE name = 'UIX_NK_Dim_Products_Active' 
               AND object_id = OBJECT_ID('DW.Dim_Products'))
        DROP INDEX [UIX_NK_Dim_Products_Active] ON [DW].[Dim_Products];
    
    -- Rename old table to backup (preserves data)
    EXEC sp_rename 'DW.Dim_Products', 'Dim_Products_backup_20260305_150248';
END
GO

-- Create new table
CREATE TABLE [DW].[Dim_Products] (
    [Products_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Products] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Row_Change_Reason] VARCHAR(50) NULL
    , [Product_Code] VARCHAR(255) NOT NULL
, [Product_Name] VARCHAR(255) NOT NULL
, [Product_Barcode] VARCHAR(255) NOT NULL
, [Principal_Code] VARCHAR(255) NOT NULL
, [Brand_Code] VARCHAR(255) NOT NULL
, [Product_Department] VARCHAR(255) NULL
, [Product_Category] VARCHAR(255) NULL
, [Product_Segment] VARCHAR(255) NULL
, [Product_Sub_Segment] VARCHAR(255) NULL
, [Product_Packaging_Type] VARCHAR(255) NULL
, [Unit_Of_Measure] VARCHAR(255) NULL
, [Product_Volume] VARCHAR(255) NULL
, [Product_Variant] VARCHAR(255) NULL
, [Product_Case_Size] VARCHAR(255) NULL
, [Product_Nappi_Code] VARCHAR(255) NULL
, [Product_Schedule] VARCHAR(255) NULL
);
GO

-- Restore data with IDENTITY_INSERT to preserve SKs
IF OBJECT_ID('DW.Dim_Products_backup_20260305_150248', 'U') IS NOT NULL
BEGIN
    SET IDENTITY_INSERT [DW].[Dim_Products] ON;
    
    INSERT INTO [DW].[Dim_Products] ([Products_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Product_Code], [Product_Name], [Product_Barcode], [Principal_Code], [Brand_Code], [Product_Department], [Product_Category], [Product_Segment], [Product_Sub_Segment], [Product_Packaging_Type], [Unit_Of_Measure], [Product_Volume], [Product_Variant], [Product_Case_Size], [Product_Nappi_Code], [Product_Schedule])
    SELECT [Products_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Product_Code], [Product_Name], [Product_Barcode], [Principal_Code], [Brand_Code], [Product_Department], [Product_Category], [Product_Segment], [Product_Sub_Segment], [Product_Packaging_Type], [Unit_Of_Measure], [Product_Volume], [Product_Variant], [Product_Case_Size], [Product_Nappi_Code], [Product_Schedule]
    FROM [DW].[Dim_Products_backup_20260305_150248];
    
    SET IDENTITY_INSERT [DW].[Dim_Products] OFF;
END
GO

-- Add unique index on active natural keys
CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Products_Active] ON [DW].[Dim_Products] ([Product_Code]) WHERE [Is_Deleted] = 0;
GO