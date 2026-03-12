#!/usr/bin/env python3
"""
Step 3: Enhanced Clean Dynamic Procedure with Key-based Hash and Proper Defaults
Based on docs review and transformation rules analysis
"""

import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection

def create_enhanced_procedure():
    """Create enhanced procedure with key-based hash and proper defaults"""
    
    procedure_sql = """
-- Enhanced Clean Dynamic Conformed Merge Procedure
-- Based on pseudo formula and requirements from docs/decisions.md
-- Features: Key-based RawRowHash, proper defaults, metadata-driven validation

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
    DECLARE @KeyColumns NVARCHAR(MAX);
    
    BEGIN TRY
        -- Step 1: Validate source exists and is active in Dim_Source_Imports
        IF NOT EXISTS (
            SELECT 1 FROM [ETL].[Dim_Source_Imports] 
            WHERE Source_Name = @SourceName AND Is_Active = 1 AND Is_Deleted = 0
        )
        BEGIN
            RAISERROR('Source [%s] is not active or is deleted in Dim_Source_Imports', 16, 1, @SourceName);
        END
        
        -- Step 2: Get ODS table name from metadata
        SELECT @ODSTable = Staging_Table
        FROM [ETL].[Dim_Source_Imports]
        WHERE Source_Name = @SourceName;
        
        -- Step 3: Build dynamic SELECT list from transformation rules with proper defaults
        SELECT @SelectList = STRING_AGG(
            CASE 
                WHEN t.Transformation_Type = 'Direct' 
                    THEN t.Conformed_Column + ' = COALESCE(s.' + t.ODS_Column + ', ' + 
                           CASE 
                               WHEN t.Default_Value IS NOT NULL AND t.Default_Value <> 'None' 
                                   THEN '''' + t.Default_Value + ''''
                               ELSE (
                                   SELECT CASE 
                                       WHEN m.Data_Type LIKE '%VARCHAR%' OR m.Data_Type LIKE '%CHAR%' 
                                           THEN '''UNKNOWN'''
                                       WHEN m.Data_Type LIKE '%DECIMAL%' OR m.Data_Type LIKE '%INT%' OR m.Data_Type LIKE '%FLOAT%'
                                           THEN '0'
                                       WHEN m.Data_Type LIKE '%DATE%' OR m.Data_Type LIKE '%TIME%'
                                           THEN '19000101'
                                       ELSE '''UNKNOWN'''
                                   END
                                   FROM [ETL].[Dim_Source_Imports_Mapping] m
                                   WHERE m.Source_Name = @SourceName 
                                     AND m.Target_Column = t.ODS_Column 
                                     AND m.Is_Deleted = 0
                               )
                           END + ')'
                WHEN t.Transformation_Type = 'Expression'
                    THEN t.Conformed_Column + ' = COALESCE(' + t.Transformation_Rule + ', ' + 
                           CASE 
                               WHEN t.Default_Value IS NOT NULL AND t.Default_Value <> 'None' 
                                   THEN '''' + t.Default_Value + ''''
                               ELSE '19000101'
                           END + ')'
                WHEN t.Transformation_Type = 'Calculated'
                    THEN t.Conformed_Column + ' = COALESCE(' + t.Transformation_Rule + ', ' + 
                           CASE 
                               WHEN t.Default_Value IS NOT NULL AND t.Default_Value <> 'None' 
                                   THEN '''' + t.Default_Value + ''''
                               ELSE '19000101'
                           END + ')'
                ELSE t.Conformed_Column + ' = COALESCE(s.' + t.ODS_Column + ', '''UNKNOWN'')'
            END, ',
    '
        ) WITHIN GROUP (ORDER BY t.Sequence_Order)
        FROM [ETL].[Dim_DW_Mapping_And_Transformations] t
        WHERE t.Source_Name = @SourceName AND t.Is_Deleted = 0;
        
        -- Step 4: Build key columns list for RawRowHash (only Is_Key = True columns)
        SELECT @KeyColumns = STRING_AGG(
            'COALESCE(' + 
            CASE 
                WHEN t.Transformation_Type = 'Direct' 
                    THEN 's.' + t.ODS_Column
                WHEN t.Transformation_Type = 'Expression'
                    THEN '(' + t.Transformation_Rule + ')'
                ELSE 'CAST(' + t.Conformed_Column + ' AS VARCHAR)'
            END + ', '''')',
            ', '
        ) WITHIN GROUP (ORDER BY t.Sequence_Order)
        FROM [ETL].[Dim_DW_Mapping_And_Transformations] t
        WHERE t.Source_Name = @SourceName AND t.Is_Deleted = 0 AND t.Is_Key = 1;
        
        -- Step 5: Validate we have transformation rules
        IF @SelectList IS NULL
        BEGIN
            RAISERROR('No transformation rules found for source [%s] in Dim_DW_Mapping_And_Transformations', 16, 1, @SourceName);
        END
        
        -- Step 6: Build final INSERT statement
        SET @SQL = N'
        INSERT INTO [ETL].[Staging_Fact_Sales_Conformed]
        (
            Date_SK,
            Place_Code,
            Product_Code,
            Barcode,
            Sales_Quantity,
            Unit_Price,
            Unit_Cost_Price,
            Total_Amount_Source,
            Validation_Message,
            Inserted_Datetime,
            Source_File_Archive_SK,
            Audit_Source_Import_SK,
            RawRowHash
        )
        SELECT 
            ' + @SelectList + ',
            Validation_Message = NULL,
            Inserted_Datetime = GETDATE(),
            Source_File_Archive_SK = @Source_File_Archive_SK,
            Audit_Source_Import_SK = @Audit_Source_Import_SK,
            RawRowHash = HASHBYTES(''SHA2_256'', CONCAT(' + @KeyColumns + '))
        FROM ' + @ODSTable + ' s';
        
        -- Step 7: Execute dynamic SQL
        EXEC sp_executesql @SQL, 
            N'@Source_File_Archive_SK INT, @Audit_Source_Import_SK INT',
            @Source_File_Archive_SK, @Audit_Source_Import_SK;
            
        SET @RowsInserted = @@ROWCOUNT;
        
        -- Step 8: Log success
        PRINT 'Successfully processed %d rows from %s to Staging_Fact_Sales_Conformed';
        
    END TRY
    BEGIN CATCH
        SET @ErrMsg = ERROR_MESSAGE();
        RAISERROR('Error in %s: %s', 16, 1, @ProcName, @ErrMsg);
        RETURN -1;
    END CATCH
    
    RETURN @RowsInserted;
END
GO
"""
    
    return procedure_sql

def show_enhanced_design():
    """Display the enhanced procedure design"""
    
    print("🔧 STEP 3: Enhanced Procedure Design")
    print("=" * 80)
    
    procedure_sql = create_enhanced_procedure()
    
    # Save to file
    with open('DW_DDL/generated/SP_Merge_Fact_Sales_ODS_to_Conformed_Enhanced.sql', 'w', encoding='utf-8') as f:
        f.write(procedure_sql)
    
    print("✅ Enhanced procedure designed successfully")
    print("📄 Saved to: DW_DDL/generated/SP_Merge_Fact_Sales_ODS_to_Conformed_Enhanced.sql")
    print()
    
    print("🎯 Key Enhancements Applied:")
    print("   ✅ RawRowHash based on Is_Key columns only")
    print("   ✅ Proper COALESCE with metadata defaults")
    print("   ✅ Data Type-driven defaults from Dim_Source_Imports_Mapping")
    print("   ✅ Active/Deleted validation from metadata")
    print("   ✅ Key-based hash for data integrity")
    print()
    
    print("📋 Data Type-Driven Default Value Logic:")
    print("   📅 VARCHAR/CHAR: 'UNKNOWN'")
    print("   💰 DECIMAL/INT/FLOAT: 0")
    print("   � DATE/TIME: 19000101")
    print("   🔑 Hash: Only Is_Key = True columns")
    print()
    
    print("🏗️ Metadata Validation:")
    print("   📊 Dim_Source_Imports.Is_Active = 1")
    print("   📊 Dim_Source_Imports.Is_Deleted = 0")
    print("   📊 Dim_DW_Mapping_And_Transformations.Is_Deleted = 0")
    print("   📊 Dim_Source_Imports_Mapping.Data_Type for defaults")
    
    return True

if __name__ == "__main__":
    success = show_enhanced_design()
    if success:
        print("\n🎉 STEP 3 COMPLETE: Enhanced Procedure Ready!")
        print("\n✅ Ready for STEP 4: Implementation")
    else:
        print("\n❌ Fix STEP 3 issues first")
