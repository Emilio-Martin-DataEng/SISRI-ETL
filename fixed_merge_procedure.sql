-- Fixed merge procedure for Sales_Format_2
-- Generated: 2026-03-13 08:46:58
-- Fixed compilation errors

IF OBJECT_ID('[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]', 'P') IS NOT NULL
    DROP PROCEDURE [ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]
GO

CREATE PROCEDURE [ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]
    @SourceName VARCHAR(100),
    @Source_File_Archive_SK INT = -1,
    @Audit_Source_Import_SK INT = -1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @ProcName SYSNAME = N'[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]';
    DECLARE @RowsInserted INT = 0;
    DECLARE @ErrMsg NVARCHAR(MAX);
    
    BEGIN TRY
        DECLARE @sql NVARCHAR(MAX) = N'INSERT INTO [ETL].[Staging_Fact_Sales_Conformed] (';
        
        DECLARE @insert_cols NVARCHAR(MAX) = N'';
        DECLARE @select_cols NVARCHAR(MAX) = N'';
        
        -- Build dynamic columns from mappings
        
        -- Add Barcode
        SET @insert_cols = @insert_cols + QUOTENAME('Barcode') + N', ';
        SET @select_cols = @select_cols + N'COALESCE(osi.[Barcode], ''UNKNOWN'') AS [Barcode], ';
        
        -- Add Unit_Cost_Price
        SET @insert_cols = @insert_cols + QUOTENAME('Unit_Cost_Price') + N', ';
        SET @select_cols = @select_cols + N'COALESCE(osi.[Cost_Price], 0) AS [Unit_Cost_Price], ';
        
        -- Add Product_Code
        SET @insert_cols = @insert_cols + QUOTENAME('Product_Code') + N', ';
        SET @select_cols = @select_cols + N'COALESCE(osi.[Product_Name], ''UNKNOWN'') AS [Product_Code], ';
        
        -- Add Total_Amount_Source
        SET @insert_cols = @insert_cols + QUOTENAME('Total_Amount_Source') + N', ';
        SET @select_cols = @select_cols + N'COALESCE(osi.[Sales_Amount], 0) AS [Total_Amount_Source], ';
        
        -- Add Date_SK
        SET @insert_cols = @insert_cols + QUOTENAME('Date_SK') + N', ';
        SET @select_cols = @select_cols + N'COALESCE(CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE,[Sales_Date_YYYY_MM_dd], 111), 112)), 19000101) AS [Date_SK], ';
        
        -- Add Sales_Quantity
        SET @insert_cols = @insert_cols + QUOTENAME('Sales_Quantity') + N', ';
        SET @select_cols = @select_cols + N'COALESCE(osi.[Sales_Quantity], 0) AS [Sales_Quantity], ';
        
        -- Add Place_Code
        SET @insert_cols = @insert_cols + QUOTENAME('Place_Code') + N', ';
        SET @select_cols = @select_cols + N'COALESCE(osi.[Store_Name], ''UNKNOWN'') AS [Place_Code], ';
        
        
        -- Remove trailing commas
        IF LEN(@insert_cols) > 0
            SET @insert_cols = LEFT(@insert_cols, LEN(@insert_cols) - 1);
            
        IF LEN(@select_cols) > 0
            SET @select_cols = LEFT(@select_cols, LEN(@select_cols) - 1);
        
        -- Add standard audit columns
        SET @sql = @sql + @insert_cols + N',
            [Inserted_Datetime], [Audit_Source_Import_SK], [Source_File_Archive_SK]
        )
        SELECT ' + @select_cols + N',
            GETDATE() AS [Inserted_Datetime],
            ' + CAST(@Audit_Source_Import_SK AS NVARCHAR(20)) + N' AS [Audit_Source_Import_SK],
            ' + CAST(@Source_File_Archive_SK AS NVARCHAR(20)) + N' AS [Source_File_Archive_SK]
        FROM [ODS].[Sales_Format_2] osi;';
        
        -- Debug
        PRINT @sql;
        
        EXEC sp_executesql @sql;
        
        SET @RowsInserted = @@ROWCOUNT;
        
        PRINT CONCAT('Inserted ', @RowsInserted, ' rows into conformed staging for source ', @SourceName);
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        
        SELECT @ErrMsg = ERROR_MESSAGE();
        DECLARE @ErrNum INT = ERROR_NUMBER();
        DECLARE @ErrState INT = ERROR_STATE();
        DECLARE @ErrLine INT = ERROR_LINE();
        DECLARE @ErrSev INT = ERROR_SEVERITY();
        
        -- Commented out the error logging call since it may not exist
        -- EXEC ETL.SP_Log_ETL_Error
        --     @Procedure_Name = @ProcName,
        --     @Error_Message = @ErrMsg,
        --     @Error_Number = @ErrNum,
        --     @Error_State = @ErrState,
        --     @Error_Line = @ErrLine,
        --     @Error_Severity = @ErrSev,
        --     @Source_File_Archive_SK = @Source_File_Archive_SK,
        --     @Audit_Source_Import_SK = @Audit_Source_Import_SK;
        
        THROW;
    END CATCH
END;
GO
