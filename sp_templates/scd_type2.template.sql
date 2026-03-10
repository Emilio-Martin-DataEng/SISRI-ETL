-- Merge proc for {dim_name} -> {dw_table} (SCD Type 2)
-- Generated at {generated_time}
CREATE OR ALTER PROCEDURE [ETL].[SP_Merge_Dim_{dim_name}]
    @Source_File_Archive_SK INT = -1,
    @Audit_Source_Import_SK INT = -1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ProcName SYSNAME = N'ETL.SP_Merge_Dim_{dim_name}';
    BEGIN TRY
        DECLARE @InsertedCount INT = 0, @UpdatedCount INT = 0, @DeletedCount INT = 0, @ReactivatedCount INT = 0, @ExpiredCount INT = 0;

        -- Type 1 UPDATE block (conditional - injected from Python)
        {type1_update_block}

        -- Type 2 change detected: Expire current row + INSERT new version
        UPDATE d SET 
            d.Row_Is_Current = 0,
            d.Row_Expiry_Datetime = GETDATE(),
            d.Updated_Datetime = GETDATE()
        FROM {dw_table} d
        INNER JOIN {ods_table} o ON {join_condition}
        WHERE d.Row_Is_Current = 1
          AND ({type2_where_changes});
        SET @ExpiredCount = @@ROWCOUNT;

        INSERT INTO {dw_table} (
            {insert_columns},
            Row_Is_Current, Row_Effective_Datetime, Row_Expiry_Datetime,
            Inserted_Datetime, Updated_Datetime,
            Row_Change_Reason, Audit_Source_Import_SK, Source_File_Archive_SK 
        )
        SELECT 
            {select_columns},
            1, GETDATE(), NULL,
            GETDATE(), NULL,
            'NEW', 
            @Audit_Source_Import_SK,
            @Source_File_Archive_SK
        FROM {ods_table} o
        WHERE NOT EXISTS (
            SELECT 1 FROM {dw_table} d 
            WHERE d.{key_column} = o.{key_column} AND d.Row_Is_Current = 1
        );
        SET @InsertedCount = @@ROWCOUNT;

        -- SOFT-DELETE (current row only)
        UPDATE d SET 
            d.Is_Deleted = 1,
            d.Row_Expiry_Datetime = GETDATE(),
            d.Updated_Datetime = GETDATE(),
            d.Row_Change_Reason = 'Soft Deleted',
            d.Audit_Source_Import_SK = @Audit_Source_Import_SK,
            d.Source_File_Archive_SK = @Source_File_Archive_SK
        FROM {dw_table} d
        LEFT JOIN {ods_table} o ON {join_condition}
        WHERE o.{key_column} IS NULL 
          AND d.Row_Is_Current = 1 
          AND d.Is_Deleted = 0;
        SET @DeletedCount = @@ROWCOUNT;

        -- RE-ACTIVATE
        UPDATE d SET 
            d.Is_Deleted = 0,
            d.Updated_Datetime = GETDATE(),
            d.Row_Change_Reason = 'Reactivated',
            d.Audit_Source_Import_SK = @Audit_Source_Import_SK,
            d.Source_File_Archive_SK = @Source_File_Archive_SK
        FROM {dw_table} d
        INNER JOIN {ods_table} o ON {join_condition}
        WHERE d.Is_Deleted = 1;
        SET @ReactivatedCount = @@ROWCOUNT;

        -- Log counts to audit
        UPDATE [ETL].[Fact_Audit_Source_Imports]
        SET Inserted_Count = @InsertedCount,
            Updated_Count = @UpdatedCount,
            Deleted_Count = @DeletedCount,
            Reactivated_Count = @ReactivatedCount
        WHERE Audit_Source_Import_SK = @Audit_Source_Import_SK;
    END TRY
    BEGIN CATCH
        DECLARE @ErrMsg NVARCHAR(MAX) = ERROR_MESSAGE(), @ErrNum INT = ERROR_NUMBER(),
                @ErrState INT = ERROR_STATE(), @ErrLine INT = ERROR_LINE(), @ErrSev INT = ERROR_SEVERITY();
        EXEC ETL.SP_Log_ETL_Error @Procedure_Name = @ProcName, @Error_Message = @ErrMsg,
            @Error_Number = @ErrNum, @Error_State = @ErrState, @Error_Line = @ErrLine, @Error_Severity = @ErrSev,
            @Source_File_Archive_SK = @Source_File_Archive_SK, @Audit_Source_Import_SK = @Audit_Source_Import_SK;
        THROW;
    END CATCH
END;