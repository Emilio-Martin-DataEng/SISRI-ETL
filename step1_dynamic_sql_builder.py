#!/usr/bin/env python3
"""
Step 1: Create Dynamic SQL Builder for Conformed Merge
"""

import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection

def build_dynamic_select_list(source_name: str) -> str:
    """Build dynamic SELECT list from mapping rules"""
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Get mappings for this source
        cursor.execute("""
            SELECT ODS_Column, Conformed_Column, Transformation_Type, 
                   Transformation_Rule, Default_Value, Is_Key
            FROM [ETL].[Dim_DW_Mapping_And_Transformations] 
            WHERE Source_Name = ? AND Is_Deleted = 0
            ORDER BY Sequence_Order
        """, source_name)
        
        mappings = cursor.fetchall()
        
        if not mappings:
            raise ValueError(f"No mappings found for source: {source_name}")
        
        # Build SELECT list
        select_parts = []
        for ods_col, conf_col, trans_type, trans_rule, default_val, is_key in mappings:
            
            if trans_type == 'Direct':
                # Direct mapping: ODS_Column -> Conformed_Column
                expression = f"[{ods_col}]"
                
            elif trans_type == 'Expression':
                # Expression mapping: use the transformation rule
                expression = trans_rule
                
            elif trans_type == 'Calculated':
                # Calculated mapping: use the transformation rule
                expression = trans_rule
                
            else:
                # Default to direct mapping
                expression = f"[{ods_col}]"
            
            # Add default value handling
            if default_val:
                expression = f"COALESCE({expression}, {default_val})"
            
            select_parts.append(f"{conf_col} = {expression}")
        
        # Add standard audit columns
        audit_columns = [
            "Validation_Message = NULL",
            "Inserted_Datetime = GETDATE()",
            "Source_File_Archive_SK = @Source_File_Archive_SK",
            "Audit_Source_Import_SK = @Audit_Source_Import_SK"
        ]
        
        select_parts.extend(audit_columns)
        
        # Build complete SELECT list
        select_list = ",\n    ".join(select_parts)
        
        return select_list
        
    finally:
        cursor.close()
        conn.close()

def test_step1():
    """Test Step 1: Build dynamic SELECT list"""
    
    print("🔧 STEP 1: Testing Dynamic SQL Builder")
    print("=" * 60)
    
    # Test with Sales_Format_1
    try:
        select_list = build_dynamic_select_list('Sales_Format_1')
        print("✅ Sales_Format_1 SELECT list built successfully:")
        print(select_list)
        print()
        
    except Exception as e:
        print(f"❌ Sales_Format_1 error: {e}")
        return False
    
    # Test with Sales_Format_2
    try:
        select_list = build_dynamic_select_list('Sales_Format_2')
        print("✅ Sales_Format_2 SELECT list built successfully:")
        print(select_list)
        print()
        
    except Exception as e:
        print(f"❌ Sales_Format_2 error: {e}")
        return False
    
    print("🎉 STEP 1 COMPLETE: Dynamic SQL Builder working!")
    return True

if __name__ == "__main__":
    success = test_step1()
    if success:
        print("\n✅ Ready for STEP 2")
    else:
        print("\n❌ Fix STEP 1 issues first")
