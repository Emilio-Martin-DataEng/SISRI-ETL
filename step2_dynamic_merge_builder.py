#!/usr/bin/env python3
"""
Step 2: Build Dynamic MERGE Statement for Conformed Merge
"""

import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection
from step1_dynamic_sql_builder import build_dynamic_select_list

def build_dynamic_merge_procedure(source_name: str) -> str:
    """Build complete dynamic MERGE procedure"""
    
    # Get the dynamic SELECT list from Step 1
    select_list = build_dynamic_select_list(source_name)
    
    # Determine ODS table name
    ods_table = f"[ODS].[{source_name}]"
    
    # Build the complete procedure
    procedure_sql = f"""
-- Dynamic Conformed Merge Procedure for {source_name}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

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
        IF @SourceName = '{source_name}'
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
                {select_list}
            FROM {ods_table} AS src';
            
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
"""
    
    return procedure_sql

def test_step2():
    """Test Step 2: Build dynamic MERGE statement"""
    
    print("🔧 STEP 2: Testing Dynamic MERGE Builder")
    print("=" * 60)
    
    try:
        # Test with Sales_Format_1
        print("📝 Building Sales_Format_1 MERGE procedure...")
        procedure_sql = build_dynamic_merge_procedure('Sales_Format_1')
        
        # Save to file for inspection
        with open('step2_sales_format_1_merge.sql', 'w', encoding='utf-8') as f:
            f.write(procedure_sql)
        
        print("✅ Sales_Format_1 MERGE procedure built successfully")
        print("📄 Saved to: step2_sales_format_1_merge.sql")
        print()
        
        # Show key parts
        lines = procedure_sql.split('\n')
        for i, line in enumerate(lines[15:25], 15):  # Show SELECT section
            print(f"{i:3}: {line}")
        
        print()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False
    
    print("🎉 STEP 2 COMPLETE: Dynamic MERGE Builder working!")
    return True

if __name__ == "__main__":
    from datetime import datetime
    success = test_step2()
    if success:
        print("\n✅ Ready for STEP 3")
    else:
        print("\n❌ Fix STEP 2 issues first")
