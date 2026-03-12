
-- Dynamic Conformed Merge Procedure for Sales_Format_1
-- Generated: 2026-03-12 16:17:42

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
    DECLARE @RowsUpdated INT = 0;
    DECLARE @ErrMsg NVARCHAR(MAX);
    DECLARE @SQL NVARCHAR(MAX);
    
    BEGIN TRY
        -- Build dynamic SQL based on source
        IF @SourceName = 'Sales_Format_1'
        BEGIN
            -- First, DELETE any existing records if needed (optional cleanup)
            -- DELETE FROM [ETL].[Staging_Fact_Sales_Conformed] 
            -- WHERE Source_File_Archive_SK = @Source_File_Archive_SK;
            
            -- Then INSERT all records
            SET @SQL = N'
            INSERT INTO [ETL].[Staging_Fact_Sales_Conformed]
            (
                Date_SK,
                Place_Code,
                Product_Code,
                Barcode,
                Sales_Quantity,
                Unit_Cost_Price,
                Total_Amount_Source,
                Validation_Message,
                Inserted_Datetime,
                Source_File_Archive_SK,
                Audit_Source_Import_SK
            )
            SELECT 
                Date_SK = COALESCE(CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE,[Sales_Date_dd-MM-YYYY], 105), 112)), 19000101),
    Place_Code = COALESCE([Store_Name], 'UNKNOWN'),
    Product_Code = COALESCE(LEFT([Product_Name], CHARINDEX(' ', [Product_Name] + ' ') - 1), 'UNKNOWN'),
    Barcode = COALESCE([Barcode], 'UNKNOWN'),
    Sales_Quantity = COALESCE([Sales_Quantity], 0),
    Unit_Cost_Price = COALESCE([Cost_Price], 0),
    Total_Amount_Source = COALESCE([Sales_Amount], 0),
    Validation_Message = NULL,
    Inserted_Datetime = GETDATE(),
    Source_File_Archive_SK = @Source_File_Archive_SK,
    Audit_Source_Import_SK = @Audit_Source_Import_SK
            FROM [ODS].[Sales_Format_1] AS src';
            
            EXEC sp_executesql @SQL, 
                N'@Source_File_Archive_SK INT, @Audit_Source_Import_SK INT',
                @Source_File_Archive_SK, @Audit_Source_Import_SK;
                
            SET @RowsInserted = @@ROWCOUNT;
        END
        ELSE
        BEGIN
            RAISERROR('Unknown source: %s', 16, 1, @SourceName);
        END
        
        -- Log success
        PRINT 'Successfully processed %d rows from %s';
        
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        RAISERROR('Error in %s: %s', 16, 1, @ProcName, @ErrMsg);
        RETURN -1;
    END CATCH
    
    RETURN @RowsInserted;
END
GO
