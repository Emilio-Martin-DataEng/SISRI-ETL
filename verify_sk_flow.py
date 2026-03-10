import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

print("🔍 VERIFYING COMPLETE SK FLOW THROUGH ETL PIPELINE")
print("=" * 60)

# 1. Check recent audit entries
print("\n📋 Recent Audit Entries:")
cursor.execute("""
    SELECT TOP 6 
           Audit_Source_Import_SK,
           Source_Import_SK,
           Start_Time,
           End_Time,
           Total_Row_Count,
           Process_Status
    FROM [ETL].[Fact_Audit_Source_Imports] 
    ORDER BY Audit_Source_Import_SK DESC
""")
for row in cursor.fetchall():
    print(f"  Audit_SK: {row[0]} | Source: {row[1]} | Rows: {row[4]} | Status: {row[5]} | {row[2]}")

# 2. Check file archive records
print("\n📁 File Archive Records:")
cursor.execute("""
    SELECT TOP 6 
           Source_File_Archive_SK,
           Audit_Source_Import_SK,
           Source_Import_SK,
           Original_File_Name,
           File_Row_Count,
           Process_Status,
           Inserted_Datetime
    FROM [ETL].[Fact_Source_File_Archive] 
    ORDER BY Source_File_Archive_SK DESC
""")
for row in cursor.fetchall():
    print(f"  Archive_SK: {row[0]} | Audit: {row[1]} | File: {row[3]} | Rows: {row[4]} | {row[6]}")

# 3. Check ODS tables have the correct archive SKs
print("\n📊 ODS Table Archive SK Verification:")
tables = ['Principals', 'Brands', 'Wholesalers', 'Products', 'Places']
for table in tables:
    cursor.execute(f"""
        SELECT DISTINCT Source_File_Archive_SK 
        FROM [ODS].[{table}] 
        ORDER BY Source_File_Archive_SK DESC
    """)
    archive_sks = [row[0] for row in cursor.fetchall()]
    cursor.execute(f"SELECT COUNT(*) FROM [ODS].[{table}]")
    total_rows = cursor.fetchone()[0]
    print(f"  {table}: {total_rows} rows | Archive SKs: {archive_sks}")

# 4. Sample data verification
print("\n🔍 Sample Data Verification (Brands):")
cursor.execute("""
    SELECT TOP 3 
           Principal_Code,
           Brand_Code, 
           Brand_Name,
           Audit_Source_Import_SK,
           Source_File_Archive_SK,
           Inserted_Datetime
    FROM [ODS].[Brands] 
    ORDER BY Inserted_Datetime DESC
""")
for row in cursor.fetchall():
    print(f"  {row[0]} | {row[1]} | {row[2]} | Audit: {row[3]} | Archive: {row[4]} | {row[5]}")

conn.close()
print("\n✅ SK FLOW VERIFICATION COMPLETE!")
