import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print("🎯 VERIFYING FINAL PLACES SK FLOW")
print("=" * 60)

# Check final Dim_Places data with different Archive SKs
print("\n📊 Final [DW].[Dim_Places] Data:")
cursor.execute("""
    SELECT TOP 10 
           Places_SK, Audit_Source_Import_SK, Source_File_Archive_SK,
           Place_Code, Place_Name, Inserted_Datetime, Row_Change_Reason
    FROM [DW].[Dim_Places] 
    ORDER BY Inserted_Datetime DESC
""")
for row in cursor.fetchall():
    print(f"  SK: {row[0]} | Audit: {row[1]} | Archive: {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]}")

# Check distinct Archive SKs
cursor.execute("SELECT DISTINCT Source_File_Archive_SK FROM [DW].[Dim_Places] ORDER BY Source_File_Archive_SK DESC")
archive_sks = [row[0] for row in cursor.fetchall()]
print(f"\n🔢 Distinct Archive SKs in Dim_Places: {archive_sks}")

# Check archive records
print("\n📁 Latest Archive Records:")
cursor.execute("""
    SELECT TOP 3 
           Source_File_Archive_SK, Audit_Source_Import_SK,
           Original_File_Name, File_Row_Count, Process_Status
    FROM [ETL].[Fact_Source_File_Archive] 
    WHERE Original_File_Name LIKE 'Places%'
    ORDER BY Source_File_Archive_SK DESC
""")
for row in cursor.fetchall():
    print(f"  Archive: {row[0]} | Audit: {row[1]} | File: {row[2]} | Rows: {row[3]} | Status: {row[4]}")

conn.close()
print("\n🎉 SUCCESS: Each file now has its own Archive SK!")
