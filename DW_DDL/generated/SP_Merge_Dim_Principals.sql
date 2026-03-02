-- Merge proc for Principals -> ETL.Dim_Principals
-- Generated at 2026-03-02 17:59:23
CREATE OR ALTER PROCEDURE [ETL].[SP_Merge_Dim_Principals]
    @Source_Import_SK INT = NULL,
    @Audit_Source_Import_SK INT = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ProcName SYSNAME = N'ETL.SP_Merge_Dim_Principals';
    BEGIN TRY
        -- Type 1: UPDATE changed attributes
        UPDATE d SET
            ,
            d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_Principals] d
        INNER JOIN [ETL].[ODS.Principals] o ON d.[Principal_Code] = o.[Principal_Code]
        WHERE (1=0);   

        -- INSERT new dimension rows
        INSERT INTO [ETL].[Dim_Principals] ([Principal_Code], [Principal_Name], [Principal_Trading_As_Name], [Principal_Address ], [Principal_City    ], [Principal_Province], [Principal_Country ], [Inserted_Datetime], [Updated_Datetime])
        SELECT o.[Principal_Code], o.[Principal_Name], o.[Principal_Trading_As_Name], o.[Principal_Address ], o.[Principal_City    ], o.[Principal_Province], o.[Principal_Country ], GETDATE(), NULL
        FROM [ETL].[ODS.Principals] o
        WHERE NOT EXISTS (SELECT 1 FROM [ETL].[Dim_Principals] d WHERE d.[Principal_Code] = o.[Principal_Code]);

        -- SOFT-DELETE: mark rows no longer in staging
        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_Principals] d
        LEFT JOIN [ETL].[ODS.Principals] o ON d.[Principal_Code] = o.[Principal_Code]
        WHERE o.Principal_Code IS NULL AND d.Is_Deleted = 0;

        -- RE-ACTIVATE: rows that reappear in staging
        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = GETDATE()
        FROM [ETL].[Dim_Principals] d
        INNER JOIN [ETL].[ODS.Principals] o ON d.[Principal_Code] = o.[Principal_Code]
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