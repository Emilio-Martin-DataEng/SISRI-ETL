import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print("🔍 VERIFYING PLACES SK ISSUE")
print("=" * 60)

# Check current ODS.Places data (should have 2 different Archive SKs)
print("\n📊 Current ODS.Places Data:")
cursor.execute("""
    SELECT TOP 10 
           Audit_Source_Import_SK, Source_File_Archive_SK,
           Place_Code, Place_Name, Inserted_Datetime
    FROM [ODS].[Places] 
    ORDER BY Inserted_Datetime DESC
""")
for row in cursor.fetchall():
    print(f"  Audit: {row[0]} | Archive: {row[1]} | {row[2]} | {row[3]} | {row[4]}")

# Check archive records for Places (should be 2 different ones)
print("\n📁 Places Archive Records:")
cursor.execute("""
    SELECT TOP 5 
           Source_File_Archive_SK, Audit_Source_Import_SK,
           Original_File_Name, File_Row_Count, Process_Status
    FROM [ETL].[Fact_Source_File_Archive] 
    WHERE Original_File_Name LIKE 'Places%'
    ORDER BY Source_File_Archive_SK DESC
""")
for row in cursor.fetchall():
    print(f"  Archive: {row[0]} | Audit: {row[1]} | File: {row[2]} | Rows: {row[3]} | {row[4]}")

conn.close()
print("\n🔍 ISSUE: All Places files getting same Archive_SK=48")
print("🔧 SOLUTION: Merge logic needs to handle multiple files per source correctly")
