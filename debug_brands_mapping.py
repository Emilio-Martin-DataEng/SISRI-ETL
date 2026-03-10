import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Check Brands mapping
cursor.execute("""
    SELECT File_Mapping_SK, Target_Column, Data_Type
    FROM [ETL].[Dim_Source_Imports_Mapping]
    WHERE Source_Name = 'Brands' AND Is_Deleted = 0
    ORDER BY File_Mapping_SK
""")

print("Brands mapping:")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]} ({row[2]})")

conn.close()
