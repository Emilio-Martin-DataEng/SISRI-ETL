#!/usr/bin/env python3
"""
Test transformation rule syntax
"""

import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection

def test_transformation_rule():
    """Test the exact transformation rule"""
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Get the transformation rule
        cursor.execute('''
            SELECT Transformation_Rule
            FROM [ETL].[Dim_DW_Mapping_And_Transformations] 
            WHERE Source_Name = 'Sales_Format_1' 
              AND Conformed_Column = 'Date_SK'
              AND Is_Deleted = 0
        ''')
        
        row = cursor.fetchone()
        if row:
            rule = row[0]
            print(f'🔧 Testing transformation rule:')
            print(f'Rule: {rule}')
            print()
            
            # Test the rule directly
            test_sql = f"SELECT {rule} AS TestResult"
            print(f'Test SQL: {test_sql}')
            
            try:
                cursor.execute(test_sql)
                result = cursor.fetchone()
                print(f'✅ Rule executes successfully: {result[0]}')
                
            except Exception as e:
                print(f'❌ Rule compilation error: {e}')
                
                # Try to fix common issues
                if 'Sales_Date_dd-MM-YYYY' in rule:
                    print()
                    print('🔧 Attempting to fix transformation rule...')
                    
                    # Try fixed version
                    fixed_rule = "CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE, [Sales_Date_dd-MM-YYYY], 105), 112))"
                    test_sql_fixed = f"SELECT {fixed_rule} AS TestResult"
                    
                    try:
                        cursor.execute(test_sql_fixed)
                        result = cursor.fetchone()
                        print(f'✅ Fixed rule works: {result[0]}')
                        print()
                        print('📝 UPDATE statement to fix the rule:')
                        print(f'''
UPDATE [ETL].[Dim_DW_Mapping_And_Transformations]
SET Transformation_Rule = '{fixed_rule}'
WHERE Source_Name = 'Sales_Format_1' 
  AND Conformed_Column = 'Date_SK';
                        ''')
                        
                    except Exception as e2:
                        print(f'❌ Fixed rule still fails: {e2}')
        
        else:
            print('❌ No transformation rule found')
            
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    test_transformation_rule()
