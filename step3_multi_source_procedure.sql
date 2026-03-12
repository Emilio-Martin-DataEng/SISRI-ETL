
-- Multi-Source Dynamic Conformed Merge Procedure
-- Generated: 2026-03-12 16:36:20
-- Handles: Sales_Format_1, Sales_Format_2, and future sources

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
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @SelectList NVARCHAR(MAX);
    DECLARE @ODSTable NVARCHAR(200);
    
    BEGIN TRY
        -- Get dynamic SELECT list from metadata
        SELECT @SelectList = STRING_AGG(
            CASE 
                WHEN Transformation_Type = 'Direct' 
                    THEN Conformed_Column + ' = COALESCE([' + ODS_Column + '], ' + ISNULL(''' + Default_Value + ''', 'NULL') + ')'
                WHEN Transformation_Type = 'Expression'
                    THEN Conformed_Column + ' = ' + Transformation_Rule
                WHEN Transformation_Type = 'Calculated'
                    THEN Conformed_Column + ' = ' + Transformation_Rule
                ELSE Conformed_Column + ' = COALESCE([' + ODS_Column + '], ' + ISNULL(''' + Default_Value + ''', 'NULL') + ')'
            END, ',
    '
        ) WITHIN GROUP (ORDER BY Sequence_Order)
        FROM [ETL].[Dim_DW_Mapping_And_Transformations]
        WHERE Source_Name = @SourceName AND Is_Deleted = 0;
        
        -- Validate that ODS columns exist for all mappings
        IF @SelectList IS NOT NULL
        BEGIN
            DECLARE @InvalidColumn NVARCHAR(200);
            DECLARE @TestSQL NVARCHAR(MAX);
            
            -- Check for invalid ODS columns in mappings
            SELECT TOP 1 @InvalidColumn = ODS_Column
            FROM [ETL].[Dim_DW_Mapping_And_Transformations] t
            WHERE Source_Name = @SourceName AND Is_Deleted = 0
              AND NOT EXISTS (
                  SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS c
                  WHERE c.TABLE_SCHEMA = 'ODS' 
                    AND c.TABLE_NAME = @SourceName
                    AND c.COLUMN_NAME = t.ODS_Column
              );
            
            IF @InvalidColumn IS NOT NULL
            BEGIN
                RAISERROR('Mapping rule error: ODS column [%s] does not exist in table [ODS].[%s]. Please check your mapping rules in Dim_DW_Mapping_And_Transformations.', 16, 1, @InvalidColumn, @SourceName);
            END
            
            -- Test transformation rules for compilation errors
            DECLARE @TransformationRule NVARCHAR(MAX);
            DECLARE @TestResult INT;
            
            DECLARE trans_cursor CURSOR FOR
                SELECT Transformation_Rule
                FROM [ETL].[Dim_DW_Mapping_And_Transformations]
                WHERE Source_Name = @SourceName 
                  AND Is_Deleted = 0
                  AND Transformation_Type = 'Expression';
            
            OPEN trans_cursor;
            FETCH NEXT FROM trans_cursor INTO @TransformationRule;
            
            WHILE @@FETCH_STATUS = 0
            BEGIN
                IF @TransformationRule IS NOT NULL AND LEN(@TransformationRule) > 0
                BEGIN
                    -- Test the transformation rule
                    SET @TestSQL = N'SELECT @Result = ' + @TransformationRule;
                    
                    BEGIN TRY
                        EXEC sp_executesql @TestSQL, N'@Result INT OUTPUT', @TestResult OUTPUT;
                    END TRY
                    BEGIN CATCH
                        RAISERROR('Transformation rule compilation error for source [%s]: %s. Please fix the expression in Dim_DW_Mapping_And_Transformations.', 16, 1, @SourceName, 'Invalid SQL syntax in transformation rule');
                    END CATCH
                END
                
                FETCH NEXT FROM trans_cursor INTO @TransformationRule;
            END
            
            CLOSE trans_cursor;
            DEALLOCATE trans_cursor;
        END
        ELSE
        BEGIN
            RAISERROR('No mapping rules found for source [%s]. Please add mapping rules to Dim_DW_Mapping_And_Transformations.', 16, 1, @SourceName);
        END
        
        -- Add standard audit columns
        SET @SelectList = @SelectList + ',
    Validation_Message = NULL,
    Inserted_Datetime = GETDATE(),
    Source_File_Archive_SK = @Source_File_Archive_SK,
    Audit_Source_Import_SK = @Audit_Source_Import_SK';
        
        -- Set ODS table name
        SET @ODSTable = '[ODS].[' + @SourceName + ']';
        
        -- Build dynamic INSERT statement
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
        SELECT ' + @SelectList + '
        FROM ' + @ODSTable + ' AS src';
        
        -- Execute dynamic SQL
        EXEC sp_executesql @SQL, 
            N'@Source_File_Archive_SK INT, @Audit_Source_Import_SK INT',
            @Source_File_Archive_SK, @Audit_Source_Import_SK;
            
        SET @RowsInserted = @@ROWCOUNT;
        
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
