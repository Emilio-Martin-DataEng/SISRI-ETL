-- DDL for Source_Imports -> ETL.Dim_Source_Imports
-- Generated at 2026-03-01 01:00:41

USE [SISRI];
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'ETL')
    EXEC ('CREATE SCHEMA [ETL] AUTHORIZATION dbo;');
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports')
BEGIN
    CREATE TABLE [ETL].[Dim_Source_Imports] (
    [Source_Import_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Source_Imports] PRIMARY KEY,,
    [Source_Name] VARCHAR(500) NULL,
    [Rel_Path] VARCHAR(500) NULL,
    [Pattern] VARCHAR(500) NULL,
    [Sheet_Name] VARCHAR(500) NULL,
    [Staging_Table] VARCHAR(500) NULL,
    [Processing_Order] VARCHAR(500) NULL,
    [Is_Active] bit NULL,
    [Description] VARCHAR(500) NULL,
    [Inserted_Datetime] DATETIME NOT NULL CONSTRAINT [DF_Dim_Source_Imports_Inserted] DEFAULT (GETDATE()),,
    [Updated_Datetime] DATETIME NULL
    );
END
ELSE
BEGIN
    -- ADD missing columns (order per File_Mapping_SK)
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports' AND c.name = 'Rel_Path')
        ALTER TABLE [ETL].[Dim_Source_Imports] ADD [Rel_Path] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports' AND c.name = 'Pattern')
        ALTER TABLE [ETL].[Dim_Source_Imports] ADD [Pattern] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports' AND c.name = 'Sheet_Name')
        ALTER TABLE [ETL].[Dim_Source_Imports] ADD [Sheet_Name] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports' AND c.name = 'Staging_Table')
        ALTER TABLE [ETL].[Dim_Source_Imports] ADD [Staging_Table] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports' AND c.name = 'Processing_Order')
        ALTER TABLE [ETL].[Dim_Source_Imports] ADD [Processing_Order] VARCHAR(500) NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports' AND c.name = 'Is_Active')
        ALTER TABLE [ETL].[Dim_Source_Imports] ADD [Is_Active] bit NULL;
    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports' AND c.name = 'Description')
        ALTER TABLE [ETL].[Dim_Source_Imports] ADD [Description] VARCHAR(500) NULL;
END
GO


-- Merge proc for Source_Imports -> ETL.Dim_Source_Imports
-- Generated at 2026-03-01 01:00:41

CREATE OR ALTER PROCEDURE [ETL].[SP_Merge_Dim_Source_Imports]
    @Source_Import_SK       INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @ProcName SYSNAME = N'ETL.SP_Merge_Dim_Source_Imports';

    BEGIN TRY

        -- Type 1: UPDATE changed attributes
        UPDATE d SET
            d.[Rel_Path] = o.[Rel_Path], d.[Pattern] = o.[Pattern], d.[Sheet_Name] = o.[Sheet_Name], d.[Staging_Table] = o.[Staging_Table], d.[Processing_Order] = o.[Processing_Order], d.[Is_Active] = o.[Is_Active], d.[Description] = o.[Description],
            d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_Source_Imports] d
        INNER JOIN [ETL].[Source_Imports] o ON d.[Source_Name] = o.[Source_Name]
        WHERE (COALESCE(d.[Rel_Path], '') <> COALESCE(o.[Rel_Path], '') OR COALESCE(d.[Pattern], '') <> COALESCE(o.[Pattern], '') OR COALESCE(d.[Sheet_Name], '') <> COALESCE(o.[Sheet_Name], '') OR COALESCE(d.[Staging_Table], '') <> COALESCE(o.[Staging_Table], '') OR COALESCE(d.[Processing_Order], '') <> COALESCE(o.[Processing_Order], '') OR COALESCE(d.[Is_Active], '') <> COALESCE(o.[Is_Active], '') OR COALESCE(d.[Description], '') <> COALESCE(o.[Description], ''));

        -- INSERT new dimension rows
        INSERT INTO [ETL].[Dim_Source_Imports] ([Source_Name], [Rel_Path], [Pattern], [Sheet_Name], [Staging_Table], [Processing_Order], [Is_Active], [Description], [Inserted_Datetime], [Updated_Datetime])
        SELECT o.[Source_Name], o.[Rel_Path], o.[Pattern], o.[Sheet_Name], o.[Staging_Table], o.[Processing_Order], o.[Is_Active], o.[Description], GETDATE(), NULL
        FROM [ETL].[Source_Imports] o
        WHERE NOT EXISTS (SELECT 1 FROM [ETL].[Dim_Source_Imports] d WHERE d.[Source_Name] = o.[Source_Name]);

        -- SOFT-DELETE: mark rows no longer in staging
        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_Source_Imports] d
        LEFT JOIN [ETL].[Source_Imports] o ON d.[Source_Name] = o.[Source_Name]
        WHERE o.Source_Name IS NULL AND d.Is_Deleted = 0;

        -- RE-ACTIVATE: rows that reappear in staging
        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_Source_Imports] d
        INNER JOIN [ETL].[Source_Imports] o ON d.[Source_Name] = o.[Source_Name]
        WHERE d.Is_Deleted = 1;
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