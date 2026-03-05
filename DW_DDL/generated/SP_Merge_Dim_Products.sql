-- Merge proc for Products -> [DW].[Dim_Products] (SCD Type 1)
-- Generated at 2026-03-05 09:45:40
CREATE OR ALTER PROCEDURE [ETL].[SP_Merge_Dim_Products]
    @Source_Import_SK INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ProcName SYSNAME = N'ETL.SP_Merge_Dim_Products';
    BEGIN TRY
        DECLARE @InsertedCount INT = 0, @UpdatedCount INT = 0, @DeletedCount INT = 0, @ReactivatedCount INT = 0;

        -- Type 1 UPDATE block (conditional - injected from Python)
        -- Type 1: UPDATE changed attributes
UPDATE d SET
    d.[Product_Name] = o.[Product_Name], d.[Product_Barcode] = o.[Product_Barcode], d.[Principal_Code] = o.[Principal_Code], d.[Brand_Code] = o.[Brand_Code], d.[Product_Department] = o.[Product_Department], d.[Product_Category] = o.[Product_Category], d.[Product_Segment] = o.[Product_Segment], d.[Product_Sub_Segment] = o.[Product_Sub_Segment], d.[Product_Packaging_Type] = o.[Product_Packaging_Type], d.[Unit_Of_Measure] = o.[Unit_Of_Measure], d.[Product_Volume] = o.[Product_Volume], d.[Product_Variant] = o.[Product_Variant], d.[Product_Case_Size] = o.[Product_Case_Size], d.[Product_Nappi_Code] = o.[Product_Nappi_Code], d.[Product_Schedule] = o.[Product_Schedule],
    d.Updated_Datetime = GETDATE()
FROM [DW].[Dim_Products] d
INNER JOIN [ODS].[Products] o ON d.[Product_Code] = o.[Product_Code]
WHERE (COALESCE(d.[Product_Name], '') <> COALESCE(o.[Product_Name], '') OR COALESCE(d.[Product_Barcode], '') <> COALESCE(o.[Product_Barcode], '') OR COALESCE(d.[Principal_Code], '') <> COALESCE(o.[Principal_Code], '') OR COALESCE(d.[Brand_Code], '') <> COALESCE(o.[Brand_Code], '') OR COALESCE(d.[Product_Department], '') <> COALESCE(o.[Product_Department], '') OR COALESCE(d.[Product_Category], '') <> COALESCE(o.[Product_Category], '') OR COALESCE(d.[Product_Segment], '') <> COALESCE(o.[Product_Segment], '') OR COALESCE(d.[Product_Sub_Segment], '') <> COALESCE(o.[Product_Sub_Segment], '') OR COALESCE(d.[Product_Packaging_Type], '') <> COALESCE(o.[Product_Packaging_Type], '') OR COALESCE(d.[Unit_Of_Measure], '') <> COALESCE(o.[Unit_Of_Measure], '') OR COALESCE(d.[Product_Volume], '') <> COALESCE(o.[Product_Volume], '') OR COALESCE(d.[Product_Variant], '') <> COALESCE(o.[Product_Variant], '') OR COALESCE(d.[Product_Case_Size], '') <> COALESCE(o.[Product_Case_Size], '') OR COALESCE(d.[Product_Nappi_Code], '') <> COALESCE(o.[Product_Nappi_Code], '') OR COALESCE(d.[Product_Schedule], '') <> COALESCE(o.[Product_Schedule], ''));
SET @UpdatedCount = @@ROWCOUNT;


        -- INSERT new dimension rows
        INSERT INTO [DW].[Dim_Products] ([Product_Code], [Product_Name], [Product_Barcode], [Principal_Code], [Brand_Code], [Product_Department], [Product_Category], [Product_Segment], [Product_Sub_Segment], [Product_Packaging_Type], [Unit_Of_Measure], [Product_Volume], [Product_Variant], [Product_Case_Size], [Product_Nappi_Code], [Product_Schedule], [Inserted_Datetime], [Updated_Datetime], [Row_Change_Reason])
        SELECT o.[Product_Code], o.[Product_Name], o.[Product_Barcode], o.[Principal_Code], o.[Brand_Code], o.[Product_Department], o.[Product_Category], o.[Product_Segment], o.[Product_Sub_Segment], o.[Product_Packaging_Type], o.[Unit_Of_Measure], o.[Product_Volume], o.[Product_Variant], o.[Product_Case_Size], o.[Product_Nappi_Code], o.[Product_Schedule], GETDATE(), NULL, 'NEW'
        FROM [ODS].[Products] o
        WHERE NOT EXISTS (SELECT 1 FROM [DW].[Dim_Products] d WHERE d.[Product_Code] = o.[Product_Code]);
        SET @InsertedCount = @@ROWCOUNT;

        -- SOFT-DELETE: mark rows no longer in staging
        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = GETDATE(), d.Row_Change_Reason = 'Soft Deleted'
        FROM [DW].[Dim_Products] d
        LEFT JOIN [ODS].[Products] o ON d.[Product_Code] = o.[Product_Code]
        WHERE o.Product_Code IS NULL AND d.Is_Deleted = 0;
        SET @DeletedCount = @@ROWCOUNT;

        -- RE-ACTIVATE: rows that reappear in staging
        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = GETDATE(), d.Row_Change_Reason = 'Reactivated'
        FROM [DW].[Dim_Products] d
        INNER JOIN [ODS].[Products] o ON d.[Product_Code] = o.[Product_Code]
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