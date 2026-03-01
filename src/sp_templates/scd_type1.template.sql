-- Merge proc for {dim_name} -> ETL.Dim_{dim_name}
-- Generated at {generated_time}
CREATE OR ALTER PROCEDURE [ETL].[SP_Merge_Dim_{dim_name}]
    @Source_Import_SK INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ProcName SYSNAME = N'ETL.SP_Merge_Dim_{dim_name}';
    BEGIN TRY
        -- Type 1: UPDATE changed attributes
        UPDATE d SET
            {update_columns},
            d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_{dim_name}] d
        INNER JOIN [ETL].[{staging_table}] o ON {join_condition}
        WHERE ({where_changes});

        -- INSERT new dimension rows
        INSERT INTO [ETL].[Dim_{dim_name}] ({insert_columns}, [Inserted_Datetime], [Updated_Datetime])
        SELECT {select_columns}, GETDATE(), NULL
        FROM [ETL].[{staging_table}] o
        WHERE NOT EXISTS (SELECT 1 FROM [ETL].[Dim_{dim_name}] d WHERE {join_condition});

        -- SOFT-DELETE: mark rows no longer in staging
        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_{dim_name}] d
        LEFT JOIN [ETL].[{staging_table}] o ON {join_condition}
        WHERE o.{key_column} IS NULL AND d.Is_Deleted = 0;

        -- RE-ACTIVATE: rows that reappear in staging
        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_{dim_name}] d
        INNER JOIN [ETL].[{staging_table}] o ON {join_condition}
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