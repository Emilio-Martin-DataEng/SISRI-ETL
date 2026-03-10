import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Check what merge procedures exist
cursor.execute("""
    SELECT Source_Name, Source_Type, Merge_Proc_Name, Staging_Table
    FROM [ETL].[Dim_Source_Imports] 
    WHERE Is_Active = 1 AND Is_Deleted = 0
    ORDER BY Source_Name
""")

print("Source configurations:")
for row in cursor.fetchall():
    print(f"  {row[0]} (Type: {row[1]})")
    print(f"    Merge Proc: {row[2]}")
    print(f"    Staging Table: {row[3]}")
    print()

conn.close()
