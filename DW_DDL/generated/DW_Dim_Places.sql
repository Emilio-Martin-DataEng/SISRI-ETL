-- Safe regeneration template for DW.Dim_Places
-- Generated at 2026-03-05 13:21:40

-- Drop old backup if it exists (prevents rename conflict)
IF OBJECT_ID('DW.Dim_Places_backup_20260305_132140', 'U') IS NOT NULL
    DROP TABLE [DW].[Dim_Places_backup_20260305_132140];
GO

-- Preserve data and rename old table if it exists
IF OBJECT_ID('DW.Dim_Places', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes safely
    IF EXISTS (SELECT * FROM sys.key_constraints 
               WHERE name = 'PK_Dim_Places' 
               AND parent_object_id = OBJECT_ID('DW.Dim_Places'))
        ALTER TABLE [DW].[Dim_Places] DROP CONSTRAINT PK_Dim_Places;
    
    IF EXISTS (SELECT * FROM sys.indexes 
               WHERE name = 'UIX_NK_Dim_Places_Active' 
               AND object_id = OBJECT_ID('DW.Dim_Places'))
        DROP INDEX [UIX_NK_Dim_Places_Active] ON [DW].[Dim_Places];
    
    -- Rename old table to backup (preserves data)
    EXEC sp_rename 'DW.Dim_Places', 'Dim_Places_backup_20260305_132140';
END
GO

-- Create new table
CREATE TABLE [DW].[Dim_Places] (
    [Places_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Places] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Row_Change_Reason] VARCHAR(50) NULL
        [Row_Is_Current] BIT NOT NULL DEFAULT 1,
    [Row_Effective_Datetime] DATETIME NOT NULL DEFAULT GETDATE(),
    [Row_Expiry_Datetime] DATETIME NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Row_Change_Reason] VARCHAR(50) NULL
, [Place_Code] VARCHAR(255) NOT NULL
, [Place_Name] VARCHAR(255) NOT NULL
, [Place_Category] VARCHAR(255) NOT NULL
, [Place_Sub_Category] VARCHAR(255) NOT NULL
, [Place_Chain] VARCHAR(255) NOT NULL
, [Place_Address] VARCHAR(255) NOT NULL
, [Place_City] VARCHAR(255) NOT NULL
, [Place_Province] VARCHAR(255) NOT NULL
, [Place_Country] VARCHAR(255) NOT NULL
, [Place_Latitude] VARCHAR(255) NOT NULL
, [Place_Longitude] VARCHAR(255) NOT NULL
, [Place_Contact_Number] VARCHAR(255) NOT NULL
, [Place_Contact_Name] VARCHAR(255) NOT NULL
, [Place_Contact_Title] VARCHAR(255) NOT NULL
, [Place_Contact_Email] VARCHAR(255) NOT NULL
, [Place_Website] VARCHAR(255) NOT NULL
);
GO

-- Restore data with IDENTITY_INSERT to preserve SKs
IF OBJECT_ID('DW.Dim_Places_backup_20260305_132140', 'U') IS NOT NULL
BEGIN
    SET IDENTITY_INSERT [DW].[Dim_Places] ON;
    
    INSERT INTO [DW].[Dim_Places] ([Places_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Place_Code], [Place_Name], [Place_Category], [Place_Sub_Category], [Place_Chain], [Place_Address], [Place_City], [Place_Province], [Place_Country], [Place_Latitude], [Place_Longitude], [Place_Contact_Number], [Place_Contact_Name], [Place_Contact_Title], [Place_Contact_Email], [Place_Website])
    SELECT [Places_SK], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], [Place_Code], [Place_Name], [Place_Category], [Place_Sub_Category], [Place_Chain], [Place_Address], [Place_City], [Place_Province], [Place_Country], [Place_Latitude], [Place_Longitude], [Place_Contact_Number], [Place_Contact_Name], [Place_Contact_Title], [Place_Contact_Email], [Place_Website]
    FROM [DW].[Dim_Places_backup_20260305_132140];
    
    SET IDENTITY_INSERT [DW].[Dim_Places] OFF;
END
GO

-- Add unique index on active natural keys
CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Places_Active] ON [DW].[Dim_Places] ([Place_Code], [Place_Name], [Place_Category], [Place_Sub_Category], [Place_Chain], [Place_Address], [Place_City], [Place_Province], [Place_Country], [Place_Latitude], [Place_Longitude], [Place_Contact_Number], [Place_Contact_Name], [Place_Contact_Title], [Place_Contact_Email], [Place_Website]) WHERE [Row_Is_Current] = 1 AND [Is_Deleted] = 0;
GO