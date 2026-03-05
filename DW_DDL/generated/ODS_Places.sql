IF OBJECT_ID('ODS.Places', 'U') IS NOT NULL
DROP TABLE [ODS].[Places];
GO

CREATE TABLE [ODS].[Places] (
    [Place_Code] VARCHAR(255) NOT NULL,
    [Place_Name] VARCHAR(255) NOT NULL,
    [Place_Category] VARCHAR(255) NULL,
    [Place_Sub_Category] VARCHAR(255) NULL,
    [Place_Chain] VARCHAR(255) NULL,
    [Place_Address] VARCHAR(255) NULL,
    [Place_City] VARCHAR(255) NULL,
    [Place_Province] VARCHAR(255) NULL,
    [Place_Country] VARCHAR(255) NULL,
    [Place_Latitude] VARCHAR(255) NULL,
    [Place_Longitude] VARCHAR(255) NULL,
    [Place_Contact_Number] VARCHAR(255) NULL,
    [Place_Contact_Name] VARCHAR(255) NULL,
    [Place_Contact_Title] VARCHAR(255) NULL,
    [Place_Contact_Email] VARCHAR(255) NULL,
    [Place_Website] VARCHAR(255) NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
, CONSTRAINT PK_Places PRIMARY KEY ([Place_Code])
);
GO