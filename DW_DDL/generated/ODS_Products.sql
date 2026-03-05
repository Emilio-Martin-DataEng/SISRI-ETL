IF OBJECT_ID('ODS.Products', 'U') IS NOT NULL
DROP TABLE [ODS].[Products];
GO

CREATE TABLE [ODS].[Products] (
    [Product_Code] VARCHAR(255) NOT NULL,
    [Product_Name] VARCHAR(255) NOT NULL,
    [Product_Barcode] VARCHAR(255) NOT NULL,
    [Principal_Code] VARCHAR(255) NOT NULL,
    [Brand_Code] VARCHAR(255) NOT NULL,
    [Product_Department] VARCHAR(255) NOT NULL,
    [Product_Category] VARCHAR(255) NOT NULL,
    [Product_Segment] VARCHAR(255) NOT NULL,
    [Product_Sub_Segment] VARCHAR(255) NOT NULL,
    [Product_Packaging_Type] VARCHAR(255) NOT NULL,
    [Unit_Of_Measure] VARCHAR(255) NOT NULL,
    [Product_Volume] VARCHAR(255) NOT NULL,
    [Product_Variant] VARCHAR(255) NOT NULL,
    [Product_Case_Size] VARCHAR(255) NOT NULL,
    [Product_Nappi_Code] VARCHAR(255) NOT NULL,
    [Product_Schedule] VARCHAR(255) NOT NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
, CONSTRAINT PK_Products PRIMARY KEY ([Product_Code], [Product_Name], [Product_Barcode], [Principal_Code], [Brand_Code], [Product_Department], [Product_Category], [Product_Segment], [Product_Sub_Segment], [Product_Packaging_Type], [Unit_Of_Measure], [Product_Volume], [Product_Variant], [Product_Case_Size], [Product_Nappi_Code], [Product_Schedule])
);
GO