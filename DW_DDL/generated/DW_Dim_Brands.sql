IF OBJECT_ID('DW.Dim_Brands', 'U') IS NOT NULL
BEGIN
    EXEC sp_rename 'DW.Dim_Brands', 'Dim_Brands_backup_20260302_175923';
    -- Create new table
    CREATE TABLE [DW].[Dim_Brands] (
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Brand_Code] VARCHAR(500) NOT NULL,
    [Brand_Name] VARCHAR(500) NOT NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    -- Insert backed up data
    INSERT INTO [DW].[Dim_Brands] SELECT * FROM [DW].[Dim_Brands_backup_20260302_175923];
END
ELSE
BEGIN
    CREATE TABLE [DW].[Dim_Brands] (
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Brand_Code] VARCHAR(500) NOT NULL,
    [Brand_Name] VARCHAR(500) NOT NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
    );
END