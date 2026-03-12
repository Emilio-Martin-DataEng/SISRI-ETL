#!/usr/bin/env python3
"""
Step 4: Apply and Test the Dynamic Procedure
"""

import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection, execute_proc

def apply_procedure():
    """Apply the multi-source procedure to database"""
    
    print("🔧 STEP 4: Applying Dynamic Procedure")
    print("=" * 60)
    
    try:
        # Read the procedure file
        with open('step3_multi_source_procedure.sql', 'r', encoding='utf-8') as f:
            procedure_sql = f.read()
        
        # Apply to database
        conn = get_connection()
        cursor = conn.cursor()
        
        # Split by GO and execute each batch
        batches = procedure_sql.split('GO')
        for batch in batches:
            if batch.strip():
                cursor.execute(batch)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Procedure applied successfully to database")
        return True
        
    except Exception as e:
        print(f"❌ Error applying procedure: {e}")
        return False

def test_procedure():
    """Test the dynamic procedure with both sources"""
    
    print("\n🧪 STEP 4: Testing Dynamic Procedure")
    print("=" * 60)
    
    # Test with Sales_Format_1
    print("🔄 Testing Sales_Format_1...")
    try:
        result = execute_proc('[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]', 
                             '@SourceName = Sales_Format_1')
        print(f"✅ Sales_Format_1: {result} rows processed")
    except Exception as e:
        print(f"❌ Sales_Format_1 error: {e}")
        return False
    
    # Test with Sales_Format_2
    print("🔄 Testing Sales_Format_2...")
    try:
        result = execute_proc('[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]', 
                             '@SourceName = Sales_Format_2')
        print(f"✅ Sales_Format_2: {result} rows processed")
    except Exception as e:
        print(f"❌ Sales_Format_2 error: {e}")
        return False
    
    return True

def check_results():
    """Check the results in conformed staging"""
    
    print("\n📊 STEP 4: Checking Results")
    print("=" * 60)
    
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Check total rows
        cursor.execute('SELECT COUNT(*) FROM [ETL].[Staging_Fact_Sales_Conformed]')
        total_rows = cursor.fetchone()[0]
        print(f"📈 Total rows in Staging_Fact_Sales_Conformed: {total_rows:,}")
        
        # Check data distribution
        cursor.execute('''
            SELECT 
                COUNT(*) as row_count,
                COUNT(DISTINCT Place_Code) as places,
                COUNT(DISTINCT Product_Code) as products,
                MIN(Date_SK) as min_date,
                MAX(Date_SK) as max_date
            FROM [ETL].[Staging_Fact_Sales_Conformed]
        ''')
        
        result = cursor.fetchone()
        print(f"📊 Data Summary:")
        print(f"   Rows: {result[0]:,}")
        print(f"   Places: {result[1]:,}")
        print(f"   Products: {result[2]:,}")
        print(f"   Date Range: {result[3]} to {result[4]}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking results: {e}")
        return False
        
    finally:
        cursor.close()
        conn.close()

def main():
    """Main test function"""
    
    success = True
    
    # Apply procedure
    if not apply_procedure():
        return False
    
    # Test procedure
    if not test_procedure():
        return False
    
    # Check results
    if not check_results():
        return False
    
    print("\n🎉 STEP 4 COMPLETE: Dynamic Procedure Working!")
    print("\n✅ IMPLEMENTATION COMPLETE!")
    print("   ✅ Step 1: Dynamic SQL Builder")
    print("   ✅ Step 2: INSERT Statement (no MERGE)")
    print("   ✅ Step 3: Multi-Source Procedure")
    print("   ✅ Step 4: Applied and Tested")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        print("\n🚀 Dynamic Conformed Merge System is LIVE!")
    else:
        print("\n❌ Issues found - check logs")
