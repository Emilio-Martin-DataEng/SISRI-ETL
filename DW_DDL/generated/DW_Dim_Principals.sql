-- Safe regeneration for DW.Dim_Principals with SK preservation

-- Clean up any leftover temp table
IF OBJECT_ID('tempdb..#Temp_Dim_Principals') IS NOT NULL
    DROP TABLE #Temp_Dim_Principals;
GO

IF OBJECT_ID('DW.Dim_Principals', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes
    IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'PK_Dim_Principals' AND parent_object_id = OBJECT_ID('DW.Dim_Principals'))
        ALTER TABLE [DW].[Dim_Principals] DROP CONSTRAINT PK_Dim_Principals;
    
    IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'UIX_NK_Dim_Principals_Active' AND object_id = OBJECT_ID('DW.Dim_Principals'))
        DROP INDEX [UIX_NK_Dim_Principals_Active] ON [DW].[Dim_Principals];
    
    -- Rename old table to backup
    EXEC sp_rename 'DW.Dim_Principals', 'Dim_Principals_backup_20260304_120426';
END
GO

-- Create new table
CREATE TABLE [DW].[Dim_Principals] (
    [Principals_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Principals] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Principal_Name] VARCHAR(500) NOT NULL,
    [Principal_Trading_As_Name] VARCHAR(500) NULL,
    [Principal_Address] VARCHAR(500) NULL,
    [Principal_City] VARCHAR(500) NULL,
    [Principal_Province] VARCHAR(500) NULL,
    [Principal_Country] VARCHAR(500) NULL
);
GO

-- Restore data with IDENTITY_INSERT ON to preserve SKs
IF OBJECT_ID('DW.Dim_Principals_backup_20260304_120426', 'U') IS NOT NULL
BEGIN
    SET IDENTITY_INSERT [DW].[Dim_Principals] ON;
    
    INSERT INTO [DW].[Dim_Principals] ([Principals_SK], [Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted])
    SELECT [Principals_SK], [Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted] FROM [DW].[Dim_Principals_backup_20260304_120426];
    
    SET IDENTITY_INSERT [DW].[Dim_Principals] OFF;
    
    -- Optional: drop backup after success (comment out if you want to keep it)
    -- DROP TABLE [DW].[Dim_Principals_backup_20260304_120426];
END
GO

CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Principals_Active] ON [DW].[Dim_Principals] ([Principal_Code]) WHERE [Is_Deleted] = 0;
GO