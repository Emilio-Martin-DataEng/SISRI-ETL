IF OBJECT_ID('ODS.Principals', 'U') IS NOT NULL
DROP TABLE [ODS].[Principals];
GO

CREATE TABLE [ODS].[Principals] (
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Principal_Name] VARCHAR(500) NOT NULL,
    [Principal_Trading_As_Name] VARCHAR(500) NULL,
    [Principal_Address] VARCHAR(500) NULL,
    [Principal_City] VARCHAR(500) NULL,
    [Principal_Province] VARCHAR(500) NULL,
    [Principal_Country] VARCHAR(500) NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
, CONSTRAINT PK_Principals PRIMARY KEY ([Principal_Code])
);
GO