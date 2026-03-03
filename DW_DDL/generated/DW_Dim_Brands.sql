IF OBJECT_ID('DW.Dim_Brands', 'U') IS NOT NULL
BEGIN
    IF OBJECT_ID('PK_Dim_Brands', 'PK') IS NOT NULL ALTER TABLE [DW].[Dim_Brands] DROP CONSTRAINT PK_Dim_Brands;
    EXEC sp_rename 'DW.Dim_Brands', 'Dim_Brands_backup_20260303_160210';
    CREATE TABLE [DW].[Dim_Brands] (
    [Brands_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Brands] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Brand_Code] VARCHAR(500) NOT NULL,
    [Brand_Name] VARCHAR(500) NOT NULL
    );
    INSERT INTO [DW].[Dim_Brands] ([Principal_Code], [Brand_Code], [Brand_Name], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted])
    SELECT [Principal_Code], [Brand_Code], [Brand_Name], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted] FROM [DW].[Dim_Brands_backup_20260303_160210];
END
ELSE
BEGIN
    CREATE TABLE [DW].[Dim_Brands] (
    [Brands_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Brands] PRIMARY KEY,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Brand_Code] VARCHAR(500) NOT NULL,
    [Brand_Name] VARCHAR(500) NOT NULL
    );
END

CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_Dim_Brands_Active] ON [DW].[Dim_Brands] ([Principal_Code], [Brand_Code], [Brand_Name]) WHERE [Row_Is_Current] = 1 AND [Is_Deleted] = 0;