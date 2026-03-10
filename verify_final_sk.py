import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print("🎯 VERIFYING FINAL SK FLOW IN DIM_Brands")
print("=" * 60)

# Check final Dim_Brands data
print("\n📊 Final ETL.Dim_Brands Data:")
cursor.execute("""
    SELECT TOP 5 
           Brands_SK, Audit_Source_Import_SK, Source_File_Archive_SK,
           Principal_Code, Brand_Code, Brand_Name,
           Inserted_Datetime, Row_Change_Reason
    FROM [ETL].[Dim_Brands] 
    ORDER BY Inserted_Datetime DESC
""")
for row in cursor.fetchall():
    print(f"  SK: {row[0]} | Audit: {row[1]} | Archive: {row[2]} | {row[3]} | {row[4]} | {row[5]} | {row[6]} | {row[7]}")

# Check archive trail
print("\n📁 Archive Trail:")
cursor.execute("""
    SELECT TOP 3 
           Source_File_Archive_SK, Audit_Source_Import_SK,
           Original_File_Name, File_Row_Count, Process_Status
    FROM [ETL].[Fact_Source_File_Archive] 
    ORDER BY Source_File_Archive_SK DESC
""")
for row in cursor.fetchall():
    print(f"  Archive: {row[0]} | Audit: {row[1]} | File: {row[2]} | Rows: {row[3]} | Status: {row[4]}")

conn.close()
print("\n🎉 SK FLOW VERIFICATION COMPLETE!")
