import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection

conn = get_connection()
cursor = conn.cursor()

# Check transformation rules with Is_Key and Default_Value
cursor.execute('''
    SELECT Source_Name, ODS_Column, Conformed_Column, Is_Key, Default_Value, Is_Deleted
    FROM [ETL].[Dim_DW_Mapping_And_Transformations] 
    WHERE Source_Name LIKE '%Sales%' 
    ORDER BY Source_Name, Sequence_Order
''')

print('📋 Transformation Rules with Key and Default Values:')
print('=' * 100)
for row in cursor.fetchall():
    source, ods_col, conf_col, is_key, default_val, is_deleted = row
    print(f'{source:15} | {ods_col:25} -> {conf_col:20} | Key: {is_key} | Default: {str(default_val):10} | Active: {is_deleted}')

cursor.close()
conn.close()
