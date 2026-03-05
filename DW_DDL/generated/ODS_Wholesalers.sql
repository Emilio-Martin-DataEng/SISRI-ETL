IF OBJECT_ID('ODS.Wholesalers', 'U') IS NOT NULL
DROP TABLE [ODS].[Wholesalers];
GO

CREATE TABLE [ODS].[Wholesalers] (
    [Wholesaler_Code] VARCHAR(255) NOT NULL,
    [Wholesaler_Name] VARCHAR(255) NOT NULL,
    [Wholesaler_Address] VARCHAR(255) NULL,
    [Wholesaler_City] VARCHAR(255) NULL,
    [Wholesaler_Province] VARCHAR(255) NULL,
    [Wholesaler_Country] VARCHAR(255) NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
, CONSTRAINT PK_Wholesalers PRIMARY KEY ([Wholesaler_Code])
);
GO