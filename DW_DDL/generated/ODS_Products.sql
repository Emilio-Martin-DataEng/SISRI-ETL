IF OBJECT_ID('ODS.Products', 'U') IS NOT NULL
DROP TABLE [ODS].[Products];
GO

CREATE TABLE [ODS].[Products] (
    [Product_Code] VARCHAR(255) NOT NULL,
    [Product_Name] VARCHAR(255) NOT NULL,
    [Product_Barcode] VARCHAR(255) NOT NULL,
    [Principal_Code] VARCHAR(255) NOT NULL,
    [Brand_Code] VARCHAR(255) NOT NULL,
    [Product_Department] VARCHAR(255) NULL,
    [Product_Category] VARCHAR(255) NULL,
    [Product_Segment] VARCHAR(255) NULL,
    [Product_Sub_Segment] VARCHAR(255) NULL,
    [Product_Packaging_Type] VARCHAR(255) NULL,
    [Unit_Of_Measure] VARCHAR(255) NULL,
    [Product_Volume] VARCHAR(255) NULL,
    [Product_Variant] VARCHAR(255) NULL,
    [Product_Case_Size] VARCHAR(255) NULL,
    [Product_Nappi_Code] VARCHAR(255) NULL,
    [Product_Schedule] VARCHAR(255) NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
, CONSTRAINT PK_Products PRIMARY KEY ([Product_Code])
);
GO