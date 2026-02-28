-- DDL for Principals -> DW.Dim_Principals
-- Generated at 2026-03-01 01:00:41

USE [SISRI];
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'DW')
    EXEC ('CREATE SCHEMA [DW] AUTHORIZATION dbo;');
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'DW' AND t.name = 'Dim_Principals')
BEGIN
    CREATE TABLE [DW].[Dim_Principals] (
    [Principal_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Principals] PRIMARY KEY,,
    [Principal_Code] VARCHAR(500) NULL,
    [Principal_Name] VARCHAR(500) NULL,
    [Principal_Trading_As_Name] VARCHAR(500) NULL,
    [Principal_Address] VARCHAR(500) NULL,
    [Principal_City] VARCHAR(500) NULL,
    [Principal_Province] VARCHAR(500) NULL,
    [Principal_Country] VARCHAR(500) NULL,
    [Inserted_Datetime] DATETIME NOT NULL CONSTRAINT [DF_Dim_Principals_Inserted] DEFAULT (GETDATE()),,
    [Updated_Datetime] DATETIME NULL
    );
END
ELSE
BEGIN
    -- ADD missing columns (order per File_Mapping_SK)
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'DW' AND t.name = 'Dim_Principals' AND c.name = 'Principal_Name')
        ALTER TABLE [DW].[Dim_Principals] ADD [Principal_Name] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'DW' AND t.name = 'Dim_Principals' AND c.name = 'Principal_Trading_As_Name')
        ALTER TABLE [DW].[Dim_Principals] ADD [Principal_Trading_As_Name] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'DW' AND t.name = 'Dim_Principals' AND c.name = 'Principal_Address')
        ALTER TABLE [DW].[Dim_Principals] ADD [Principal_Address] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'DW' AND t.name = 'Dim_Principals' AND c.name = 'Principal_City')
        ALTER TABLE [DW].[Dim_Principals] ADD [Principal_City] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'DW' AND t.name = 'Dim_Principals' AND c.name = 'Principal_Province')
        ALTER TABLE [DW].[Dim_Principals] ADD [Principal_Province] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'DW' AND t.name = 'Dim_Principals' AND c.name = 'Principal_Country')
        ALTER TABLE [DW].[Dim_Principals] ADD [Principal_Country] VARCHAR(500) NULL;
END
GO


-- Merge proc for Principals -> DW.Dim_Principals
-- Generated at 2026-03-01 01:00:41

CREATE OR ALTER PROCEDURE [DW].[SP_Merge_Dim_Principals]
    @Source_Import_SK       INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ProcName SYSNAME = N'DW.SP_Merge_Dim_Principals';

    BEGIN TRY

        -- Type 1: UPDATE changed attributes
        UPDATE d SET
            d.[Principal_Name] = o.[Principal_Name], d.[Principal_Trading_As_Name] = o.[Principal_Trading_As_Name], d.[Principal_Address] = o.[Principal_Address], d.[Principal_City] = o.[Principal_City], d.[Principal_Province] = o.[Principal_Province], d.[Principal_Country] = o.[Principal_Country],
            d.Updated_Datetime = GETDATE()
        FROM [DW].[Dim_Principals] d
        INNER JOIN [ODS].[Principals] o ON d.[Principal_Code] = o.[Principal_Code]
        WHERE (COALESCE(d.[Principal_Name], '') <> COALESCE(o.[Principal_Name], '') OR COALESCE(d.[Principal_Trading_As_Name], '') <> COALESCE(o.[Principal_Trading_As_Name], '') OR COALESCE(d.[Principal_Address], '') <> COALESCE(o.[Principal_Address], '') OR COALESCE(d.[Principal_City], '') <> COALESCE(o.[Principal_City], '') OR COALESCE(d.[Principal_Province], '') <> COALESCE(o.[Principal_Province], '') OR COALESCE(d.[Principal_Country], '') <> COALESCE(o.[Principal_Country], ''));

        -- INSERT new dimension rows
        INSERT INTO [DW].[Dim_Principals] ([Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address], [Principal_City], [Principal_Province], [Principal_Country], [Inserted_Datetime], [Updated_Datetime])
        SELECT o.[Principal_Code], o.[Principal_Name], o.[Principal_Trading_As_Name], o.[Principal_Address], o.[Principal_City], o.[Principal_Province], o.[Principal_Country], GETDATE(), NULL
        FROM [ODS].[Principals] o
        WHERE NOT EXISTS (SELECT 1 FROM [DW].[Dim_Principals] d WHERE d.[Principal_Code] = o.[Principal_Code]);

    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(MAX) = ERROR_MESSAGE(), @ErrNum INT = ERROR_NUMBER(),
                @ErrState INT = ERROR_STATE(), @ErrLine INT = ERROR_LINE(), @ErrSev INT = ERROR_SEVERITY();
        EXEC ETL.SP_Log_ETL_Error @Procedure_Name = @ProcName, @Error_Message = @ErrMsg,
            @Error_Number = @ErrNum, @Error_State = @ErrState, @Error_Line = @ErrLine, @Error_Severity = @ErrSev,
            @Source_Import_SK = @Source_Import_SK, @Audit_Source_Import_SK = @Audit_Source_Import_SK;
        THROW;
    END CATCH
END;
GO