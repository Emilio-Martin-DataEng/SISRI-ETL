IF OBJECT_ID('DW.Dim_Principals', 'U') IS NOT NULL
BEGIN
    IF OBJECT_ID('PK_Dim_Principals', 'PK') IS NOT NULL ALTER TABLE [DW].[Dim_Principals] DROP CONSTRAINT PK_Dim_Principals;
    EXEC sp_rename 'DW.Dim_Principals', 'Dim_Principals_backup_20260303_160210';
    CREATE TABLE [DW].[Dim_Principals] (
    [Principals_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Principals] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Principal_Name] VARCHAR(500) NOT NULL,
    [Principal_Trading_As_Name] VARCHAR(500) NOT NULL,
    [Principal_Address] VARCHAR(500) NOT NULL,
    [Principal_City] VARCHAR(500) NOT NULL,
    [Principal_Province] VARCHAR(500) NOT NULL,
    [Principal_Country] VARCHAR(500) NOT NULL
    );
    INSERT INTO [DW].[Dim_Principals] ([Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted])
    SELECT [Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted] FROM [DW].[Dim_Principals_backup_20260303_160210];
END
ELSE
BEGIN
    CREATE TABLE [DW].[Dim_Principals] (
    [Principals_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Principals] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Principal_Name] VARCHAR(500) NOT NULL,
    [Principal_Trading_As_Name] VARCHAR(500) NOT NULL,
    [Principal_Address] VARCHAR(500) NOT NULL,
    [Principal_City] VARCHAR(500) NOT NULL,
    [Principal_Province] VARCHAR(500) NOT NULL,
    [Principal_Country] VARCHAR(500) NOT NULL
    );
END

CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Principals_Active] ON [DW].[Dim_Principals] ([Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country]) WHERE [Row_Is_Current] = 1 AND [Is_Deleted] = 0;