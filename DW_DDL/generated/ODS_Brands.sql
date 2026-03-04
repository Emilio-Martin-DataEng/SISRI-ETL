IF OBJECT_ID('ODS.Brands', 'U') IS NOT NULL
DROP TABLE [ODS].[Brands];
GO

CREATE TABLE [ODS].[Brands] (
    [Principal_Code] VARCHAR(500) NOT NULL,
    [Brand_Code] VARCHAR(500) NOT NULL,
    [Brand_Name] VARCHAR(500) NOT NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
, CONSTRAINT PK_Brands PRIMARY KEY ([Principal_Code], [Brand_Code]) 
);
GO