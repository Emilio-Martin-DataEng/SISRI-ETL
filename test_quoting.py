import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection

conn = get_connection()
cursor = conn.cursor()

# Try different quoting approaches
test_rules = [
    'CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE, [Sales_Date_dd-MM-YYYY], 105), 112))',
    'CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE, "Sales_Date_dd-MM-YYYY", 105), 112))',
    'CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE, Sales_Date_dd-MM-YYYY, 105), 112))'
]

for i, Rule in enumerate(test_rules, 1):
    print(f'🔧 Test {i}: {Rule}')
    try:
        test_sql = f'SELECT {Rule} AS TestResult'
        cursor.execute(test_sql)
        result = cursor.fetchone()
        print(f'✅ Success: {result[0]}')
    except Exception as e:
        print(f'❌ Error: {e}')
    print()

cursor.close()
conn.close()
