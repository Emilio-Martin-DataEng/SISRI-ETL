IF OBJECT_ID('DW.Dim_Principals', 'U') IS NOT NULL
BEGIN
    EXEC sp_rename 'DW.Dim_Principals', 'Dim_Principals_backup_20260302_175923';
    -- Create new table
    CREATE TABLE [DW].[Dim_Principals] (
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Principal_Name] VARCHAR(500) NOT NULL,
    [Principal_Trading_As_Name] VARCHAR(500) NOT NULL,
    [Principal_Address ] VARCHAR(500) NOT NULL,
    [Principal_City    ] VARCHAR(500) NOT NULL,
    [Principal_Province] VARCHAR(500) NOT NULL,
    [Principal_Country ] VARCHAR(500) NOT NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    -- Insert backed up data
    INSERT INTO [DW].[Dim_Principals] SELECT * FROM [DW].[Dim_Principals_backup_20260302_175923];
END
ELSE
BEGIN
    CREATE TABLE [DW].[Dim_Principals] (
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Principal_Name] VARCHAR(500) NOT NULL,
    [Principal_Trading_As_Name] VARCHAR(500) NOT NULL,
    [Principal_Address ] VARCHAR(500) NOT NULL,
    [Principal_City    ] VARCHAR(500) NOT NULL,
    [Principal_Province] VARCHAR(500) NOT NULL,
    [Principal_Country ] VARCHAR(500) NOT NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
    );
END