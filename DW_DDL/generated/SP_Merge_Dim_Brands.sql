-- Merge proc for Brands -> ETL.Dim_Brands (SCD Type 1)
-- Generated at 2026-03-04 12:04:26
CREATE OR ALTER PROCEDURE [ETL].[SP_Merge_Dim_Brands]
    @Source_Import_SK INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ProcName SYSNAME = N'ETL.SP_Merge_Dim_Brands';
    BEGIN TRY
        DECLARE @InsertedCount INT = 0, @UpdatedCount INT = 0, @DeletedCount INT = 0, @ReactivatedCount INT = 0;

        -- Type 1: UPDATE changed attributes
        UPDATE d SET
            d.[Brand_Name] = o.[Brand_Name],
            d.Updated_Datetime = GETDATE()
        FROM [DW].[Dim_Brands] d
        INNER JOIN [ODS].[Brands] o ON d.[Principal_Code] = o.[Principal_Code]
        WHERE (COALESCE(d.[Brand_Name], '') <> COALESCE(o.[Brand_Name], ''));
        SET @UpdatedCount = @@ROWCOUNT;

        -- INSERT new dimension rows
        INSERT INTO [DW].[Dim_Brands] ([Principal_Code], [Brand_Code], [Brand_Name], [Inserted_Datetime], [Updated_Datetime], [Row_Change_Reason])
        SELECT o.[Principal_Code], o.[Brand_Code], o.[Brand_Name], GETDATE(), NULL, 'NEW'
        FROM [ODS].[Brands] o
        WHERE NOT EXISTS (SELECT 1 FROM [DW].[Dim_Brands] d WHERE d.[Principal_Code] = o.[Principal_Code]);
        SET @InsertedCount = @@ROWCOUNT;

        -- SOFT-DELETE: mark rows no longer in staging
        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = GETDATE(), d.Row_Change_Reason = 'Soft Deleted'
        FROM [DW].[Dim_Brands] d
        LEFT JOIN [ODS].[Brands] o ON d.[Principal_Code] = o.[Principal_Code]
        WHERE o.Principal_Code IS NULL AND d.Is_Deleted = 0;
        SET @DeletedCount = @@ROWCOUNT;

        -- RE-ACTIVATE: rows that reappear in staging
        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = GETDATE(), d.Row_Change_Reason = 'Reactivated'
        FROM [DW].[Dim_Brands] d
        INNER JOIN [ODS].[Brands] o ON d.[Principal_Code] = o.[Principal_Code]
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
            @Source_Import_SK = @Source_Import_SK, @Audit_Source_Import_SK = @Audit_Source_Import_SK;
        THROW;
    END CATCH
END;