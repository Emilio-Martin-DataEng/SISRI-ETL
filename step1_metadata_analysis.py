import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection

conn = get_connection()
cursor = conn.cursor()

print('🔍 HIGH-LEVEL METADATA ANALYSIS')
print('=' * 80)

# 1. Check Dim_Source_Imports (high-level metadata)
cursor.execute("SELECT Source_Name, Source_Type, Staging_Table, Is_Active FROM [ETL].[Dim_Source_Imports] WHERE Source_Name LIKE '%Sales%' ORDER BY Source_Name")
print('📊 Dim_Source_Imports (Sales Sources):')
for row in cursor.fetchall():
    print(f'   {row[0]:15} | {row[1]:12} | {row[2]:25} | Active: {row[3]}')

print()

# 2. Check Dim_DW_Mapping_And_Transformations (transformation rules)
cursor.execute("SELECT Source_Name, COUNT(*) as rule_count FROM [ETL].[Dim_DW_Mapping_And_Transformations] WHERE Source_Name LIKE '%Sales%' AND Is_Deleted = 0 GROUP BY Source_Name")
print('📋 Transformation Rules Available:')
for row in cursor.fetchall():
    print(f'   {row[0]:15} | {row[1]} rules')

print()

# 3. Check ODS tables (actual data)
cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'ODS' AND TABLE_NAME LIKE '%Sales%' ORDER BY TABLE_NAME")
print('📁 ODS Tables Available:')
for row in cursor.fetchall():
    print(f'   {row[0]}')

print()

# 4. Check Conformed Staging table
cursor.execute('SELECT COUNT(*) as row_count FROM [ETL].[Staging_Fact_Sales_Conformed]')
result = cursor.fetchone()
print(f'📈 Staging_Fact_Sales_Conformed: {result[0]:,} rows')

cursor.close()
conn.close()
