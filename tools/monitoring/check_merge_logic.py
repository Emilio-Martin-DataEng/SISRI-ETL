import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print("🔍 CHECKING MERGE LOGIC AND SK FLOW")
print("=" * 60)

# 1. Check ETL.Dim_Brands structure
print("\n📋 ETL.Dim_Brands Structure:")
cursor.execute("""
    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = 'Dim_Brands' AND TABLE_SCHEMA = 'ETL'
    ORDER BY ORDINAL_POSITION
""")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]} ({'NULL' if row[2] == 'YES' else 'NOT NULL'})")

# 2. Check current data in ETL.Dim_Brands
print("\n📊 Current ETL.Dim_Brands Data:")
cursor.execute("""
    SELECT TOP 3 
           Brands_SK, Audit_Source_Import_SK, Source_File_Archive_SK,
           Principal_Code, Brand_Code, Brand_Name,
           Inserted_Datetime, Row_Change_Reason
    FROM [ETL].[Dim_Brands] 
    ORDER BY Inserted_Datetime DESC
""")
for row in cursor.fetchall():
    print(f"  SK: {row[0]} | Audit: {row[1]} | Archive: {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} | {row[7]}")

# 3. Check what the merge procedure is doing
print("\n🔧 Merge Procedure Key Parts:")
cursor.execute("""
    SELECT OBJECT_DEFINITION(OBJECT_ID('[ETL].[SP_Merge_Dim_Brands]'))
""")
proc_def = cursor.fetchone()[0]
if proc_def:
    lines = proc_def.split('\n')
    for i, line in enumerate(lines):
        if 'MERGE' in line.upper():
            print(f"  {i+1}: {line.strip()}")
        elif 'WHEN NOT MATCHED' in line.upper():
            print(f"  {i+1}: {line.strip()}")
        elif 'INSERT' in line.upper() and 'VALUES' in line.upper():
            print(f"  {i+1}: {line.strip()}")
        elif 'Audit_Source_Import_SK' in line:
            print(f"  {i+1}: {line.strip()}")
        elif 'Source_File_Archive_SK' in line:
            print(f"  {i+1}: {line.strip()}")

# 4. Check if merge is using ODS data correctly
print("\n📥 ODS.Brands Data (source for merge):")
cursor.execute("""
    SELECT TOP 3 
           Audit_Source_Import_SK, Source_File_Archive_SK,
           Principal_Code, Brand_Code, Brand_Name,
           Inserted_Datetime
    FROM [ODS].[Brands] 
    ORDER BY Inserted_Datetime DESC
""")
for row in cursor.fetchall():
    print(f"  Audit: {row[0]} | Archive: {row[1]} | {row[2]} | {row[3]} | {row[4]} | {row[5]}")

conn.close()
