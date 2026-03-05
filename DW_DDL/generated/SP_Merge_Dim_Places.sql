-- Merge proc for Places -> [DW].[Dim_Places] (SCD Type 1)
-- Generated at 2026-03-05 11:55:49
CREATE OR ALTER PROCEDURE [ETL].[SP_Merge_Dim_Places]
    @Source_Import_SK INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ProcName SYSNAME = N'ETL.SP_Merge_Dim_Places';
    BEGIN TRY
        DECLARE @InsertedCount INT = 0, @UpdatedCount INT = 0, @DeletedCount INT = 0, @ReactivatedCount INT = 0;

        -- Type 1 UPDATE block (conditional - injected from Python)
        -- Type 1: UPDATE changed attributes
UPDATE d SET
    d.[Place_Name] = o.[Place_Name], d.[Place_Chain] = o.[Place_Chain], d.[Place_Address] = o.[Place_Address], d.[Place_City] = o.[Place_City], d.[Place_Province] = o.[Place_Province], d.[Place_Country] = o.[Place_Country], d.[Place_Latitude] = o.[Place_Latitude], d.[Place_Longitude] = o.[Place_Longitude], d.[Place_Contact_Number] = o.[Place_Contact_Number], d.[Place_Contact_Name] = o.[Place_Contact_Name], d.[Place_Contact_Title] = o.[Place_Contact_Title], d.[Place_Contact_Email] = o.[Place_Contact_Email], d.[Place_Website] = o.[Place_Website],
    d.Updated_Datetime = GETDATE()
FROM [DW].[Dim_Places] d
INNER JOIN [ODS].[Places] o ON d.[Place_Code] = o.[Place_Code]
WHERE (COALESCE(d.[Place_Name], '') <> COALESCE(o.[Place_Name], '') OR COALESCE(d.[Place_Chain], '') <> COALESCE(o.[Place_Chain], '') OR COALESCE(d.[Place_Address], '') <> COALESCE(o.[Place_Address], '') OR COALESCE(d.[Place_City], '') <> COALESCE(o.[Place_City], '') OR COALESCE(d.[Place_Province], '') <> COALESCE(o.[Place_Province], '') OR COALESCE(d.[Place_Country], '') <> COALESCE(o.[Place_Country], '') OR COALESCE(d.[Place_Latitude], '') <> COALESCE(o.[Place_Latitude], '') OR COALESCE(d.[Place_Longitude], '') <> COALESCE(o.[Place_Longitude], '') OR COALESCE(d.[Place_Contact_Number], '') <> COALESCE(o.[Place_Contact_Number], '') OR COALESCE(d.[Place_Contact_Name], '') <> COALESCE(o.[Place_Contact_Name], '') OR COALESCE(d.[Place_Contact_Title], '') <> COALESCE(o.[Place_Contact_Title], '') OR COALESCE(d.[Place_Contact_Email], '') <> COALESCE(o.[Place_Contact_Email], '') OR COALESCE(d.[Place_Website], '') <> COALESCE(o.[Place_Website], ''));
SET @UpdatedCount = @@ROWCOUNT;


        -- INSERT new dimension rows
        INSERT INTO [DW].[Dim_Places] ([Place_Code], [Place_Name], [Place_Category], [Place_Sub_Category], [Place_Chain], [Place_Address], [Place_City], [Place_Province], [Place_Country], [Place_Latitude], [Place_Longitude], [Place_Contact_Number], [Place_Contact_Name], [Place_Contact_Title], [Place_Contact_Email], [Place_Website], [Inserted_Datetime], [Updated_Datetime], [Row_Change_Reason])
        SELECT o.[Place_Code], o.[Place_Name], o.[Place_Category], o.[Place_Sub_Category], o.[Place_Chain], o.[Place_Address], o.[Place_City], o.[Place_Province], o.[Place_Country], o.[Place_Latitude], o.[Place_Longitude], o.[Place_Contact_Number], o.[Place_Contact_Name], o.[Place_Contact_Title], o.[Place_Contact_Email], o.[Place_Website], GETDATE(), NULL, 'NEW'
        FROM [ODS].[Places] o
        WHERE NOT EXISTS (SELECT 1 FROM [DW].[Dim_Places] d WHERE d.[Place_Code] = o.[Place_Code]);
        SET @InsertedCount = @@ROWCOUNT;

        -- SOFT-DELETE: mark rows no longer in staging
        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = GETDATE(), d.Row_Change_Reason = 'Soft Deleted'
        FROM [DW].[Dim_Places] d
        LEFT JOIN [ODS].[Places] o ON d.[Place_Code] = o.[Place_Code]
        WHERE o.Place_Code IS NULL AND d.Is_Deleted = 0;
        SET @DeletedCount = @@ROWCOUNT;

        -- RE-ACTIVATE: rows that reappear in staging
        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = GETDATE(), d.Row_Change_Reason = 'Reactivated'
        FROM [DW].[Dim_Places] d
        INNER JOIN [ODS].[Places] o ON d.[Place_Code] = o.[Place_Code]
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