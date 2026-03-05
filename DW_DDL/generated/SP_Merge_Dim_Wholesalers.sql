-- Merge proc for Wholesalers -> [DW].[Dim_Wholesalers] (SCD Type 1)
-- Generated at 2026-03-05 15:02:49
CREATE OR ALTER PROCEDURE [ETL].[SP_Merge_Dim_Wholesalers]
    @Source_Import_SK INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ProcName SYSNAME = N'ETL.SP_Merge_Dim_Wholesalers';
    BEGIN TRY
        DECLARE @InsertedCount INT = 0, @UpdatedCount INT = 0, @DeletedCount INT = 0, @ReactivatedCount INT = 0;

        -- Type 1 UPDATE block (conditional - injected from Python)
        -- Type 1: UPDATE changed attributes
UPDATE d SET
    d.[Wholesaler_Name] = o.[Wholesaler_Name], d.[Wholesaler_Address] = o.[Wholesaler_Address], d.[Wholesaler_City] = o.[Wholesaler_City], d.[Wholesaler_Province] = o.[Wholesaler_Province], d.[Wholesaler_Country] = o.[Wholesaler_Country],
    d.Updated_Datetime = GETDATE()
FROM [DW].[Dim_Wholesalers] d
INNER JOIN [ODS].[Wholesalers] o ON d.[Wholesaler_Code] = o.[Wholesaler_Code]
WHERE (COALESCE(d.[Wholesaler_Name], '') <> COALESCE(o.[Wholesaler_Name], '') OR COALESCE(d.[Wholesaler_Address], '') <> COALESCE(o.[Wholesaler_Address], '') OR COALESCE(d.[Wholesaler_City], '') <> COALESCE(o.[Wholesaler_City], '') OR COALESCE(d.[Wholesaler_Province], '') <> COALESCE(o.[Wholesaler_Province], '') OR COALESCE(d.[Wholesaler_Country], '') <> COALESCE(o.[Wholesaler_Country], ''));
SET @UpdatedCount = @@ROWCOUNT;


        -- INSERT new dimension rows
        INSERT INTO [DW].[Dim_Wholesalers] ([Wholesaler_Code], [Wholesaler_Name], [Wholesaler_Address], [Wholesaler_City], [Wholesaler_Province], [Wholesaler_Country], [Inserted_Datetime], [Updated_Datetime], [Row_Change_Reason])
        SELECT o.[Wholesaler_Code], o.[Wholesaler_Name], o.[Wholesaler_Address], o.[Wholesaler_City], o.[Wholesaler_Province], o.[Wholesaler_Country], GETDATE(), NULL, 'NEW'
        FROM [ODS].[Wholesalers] o
        WHERE NOT EXISTS (SELECT 1 FROM [DW].[Dim_Wholesalers] d WHERE d.[Wholesaler_Code] = o.[Wholesaler_Code]);
        SET @InsertedCount = @@ROWCOUNT;

        -- SOFT-DELETE: mark rows no longer in staging
        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = GETDATE(), d.Row_Change_Reason = 'Soft Deleted'
        FROM [DW].[Dim_Wholesalers] d
        LEFT JOIN [ODS].[Wholesalers] o ON d.[Wholesaler_Code] = o.[Wholesaler_Code]
        WHERE o.Wholesaler_Code IS NULL AND d.Is_Deleted = 0;
        SET @DeletedCount = @@ROWCOUNT;

        -- RE-ACTIVATE: rows that reappear in staging
        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = GETDATE(), d.Row_Change_Reason = 'Reactivated'
        FROM [DW].[Dim_Wholesalers] d
        INNER JOIN [ODS].[Wholesalers] o ON d.[Wholesaler_Code] = o.[Wholesaler_Code]
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