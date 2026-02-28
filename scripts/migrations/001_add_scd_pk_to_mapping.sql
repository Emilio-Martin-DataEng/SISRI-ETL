-- Migration 001: Add Is_Type2_Attribute and Is_PK to mapping tables
-- Run once before loading updated ETL_Config.xlsx with these columns.
-- Supports mixed SCD Type 1/2 and composite PKs for merge proc generation.

USE [SISRI];
GO

-- Staging table
IF NOT EXISTS (SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON t.object_id = c.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'ETL' AND t.name = 'Source_File_Mapping' AND c.name = 'Is_Type2_Attribute')
BEGIN
    ALTER TABLE ETL.Source_File_Mapping ADD Is_Type2_Attribute BIT NULL;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON t.object_id = c.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'ETL' AND t.name = 'Source_File_Mapping' AND c.name = 'Is_PK')
BEGIN
    ALTER TABLE ETL.Source_File_Mapping ADD Is_PK BIT NULL;
END;
GO

-- Dimension table
IF NOT EXISTS (SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON t.object_id = c.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports_Mapping' AND c.name = 'Is_Type2_Attribute')
BEGIN
    ALTER TABLE ETL.Dim_Source_Imports_Mapping ADD Is_Type2_Attribute BIT NULL;
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns c
    JOIN sys.tables t ON t.object_id = c.object_id
    JOIN sys.schemas s ON s.schema_id = t.schema_id
    WHERE s.name = 'ETL' AND t.name = 'Dim_Source_Imports_Mapping' AND c.name = 'Is_PK')
BEGIN
    ALTER TABLE ETL.Dim_Source_Imports_Mapping ADD Is_PK BIT NULL;
END;
GO

-- Update merge proc to include new columns
CREATE OR ALTER PROCEDURE ETL.SP_Merge_Dim_Source_Imports_Mapping
    @Source_Import_SK       INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    BEGIN TRY
        DECLARE @Datetime DATETIME = GETDATE();

        -- Step 1: UPDATE existing records when any attribute changed
        UPDATE d
        SET
            d.Target_Column       = s.Target_Column,
            d.Data_Type           = s.Data_Type,
            d.Description         = s.Description,
            d.Is_Type2_Attribute  = COALESCE(s.Is_Type2_Attribute, 0),
            d.Is_PK               = COALESCE(s.Is_PK, 0),
            d.Updated_Datetime    = @Datetime
        FROM ETL.Dim_Source_Imports_Mapping d
        INNER JOIN ETL.Source_File_Mapping s
            ON d.Source_Name    = s.Source_Name
           AND d.Source_Column  = s.Source_Column
        WHERE
            (   d.Target_Column       <> s.Target_Column       OR (d.Target_Column IS NULL AND s.Target_Column IS NOT NULL) OR (d.Target_Column IS NOT NULL AND s.Target_Column IS NULL) OR
                d.Data_Type           <> s.Data_Type           OR (d.Data_Type IS NULL AND s.Data_Type IS NOT NULL) OR (d.Data_Type IS NOT NULL AND s.Data_Type IS NULL) OR
                d.Description         <> s.Description         OR (d.Description IS NULL AND s.Description IS NOT NULL) OR (d.Description IS NOT NULL AND s.Description IS NULL) OR
                COALESCE(d.Is_Type2_Attribute, 0) <> COALESCE(s.Is_Type2_Attribute, 0) OR
                COALESCE(d.Is_PK, 0)            <> COALESCE(s.Is_PK, 0)
            );

        -- Step 2: INSERT new records
        INSERT INTO ETL.Dim_Source_Imports_Mapping (
            Source_Import_SK, Source_Name, Source_Column, Target_Column,
            Data_Type, Description, Is_Type2_Attribute, Is_PK,
            Inserted_Datetime, Updated_Datetime
        )
        SELECT
            dsi.Source_Import_SK, s.Source_Name, s.Source_Column, s.Target_Column,
            s.Data_Type, s.Description,
            COALESCE(s.Is_Type2_Attribute, 0), COALESCE(s.Is_PK, 0),
            @Datetime, NULL
        FROM ETL.Source_File_Mapping s
        LEFT JOIN ETL.Dim_Source_Imports_Mapping d
            ON d.Source_Name = s.Source_Name AND d.Source_Column = s.Source_Column
        INNER JOIN ETL.Dim_Source_Imports dsi ON dsi.Source_Name = s.Source_Name
        WHERE d.File_Mapping_SK IS NULL;

        -- Step 3: SOFT-DELETE missing records
        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = @Datetime
        FROM ETL.Dim_Source_Imports_Mapping d
        LEFT JOIN ETL.Source_File_Mapping s ON d.Source_Name = s.Source_Name AND d.Source_Column = s.Source_Column
        WHERE s.Source_Name IS NULL AND d.Is_Deleted = 0;

        -- Step 4: RE-ACTIVATE previously deleted records
        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = @Datetime
        FROM ETL.Dim_Source_Imports_Mapping d
        INNER JOIN ETL.Source_File_Mapping s ON d.Source_Name = s.Source_Name AND d.Source_Column = s.Source_Column
        WHERE d.Is_Deleted = 1;
    END TRY
    BEGIN CATCH
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE(),
                @ErrorNumber INT = ERROR_NUMBER(),
                @ErrorState INT = ERROR_STATE(),
                @ErrorLine INT = ERROR_LINE(),
                @ErrorSeverity INT = ERROR_SEVERITY();
        EXEC ETL.SP_Log_ETL_Error
            @Procedure_Name = N'SP_Merge_Dim_Source_Imports_Mapping',
            @Error_Message = @ErrorMessage, @Error_Number = @ErrorNumber,
            @Error_State = @ErrorState, @Error_Line = @ErrorLine, @Error_Severity = @ErrorSeverity,
            @Source_Import_SK = @Source_Import_SK, @Audit_Source_Import_SK = @Audit_Source_Import_SK;
        THROW;
    END CATCH
END;
GO
