import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection

conn = get_connection()
cursor = conn.cursor()

# Check exact column names in ODS Sales_Format_1
cursor.execute('''
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = 'ODS' 
      AND TABLE_NAME = 'Sales_Format_1'
      AND COLUMN_NAME LIKE '%Date%'
    ORDER BY ORDINAL_POSITION
''')

print('📊 Date columns in ODS Sales_Format_1:')
for row in cursor.fetchall():
    print(f'   "{row[0]}"')

cursor.close()
conn.close()
