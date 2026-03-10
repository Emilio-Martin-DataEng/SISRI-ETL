import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Test the stored procedure directly
try:
    cursor.execute("""
        EXEC [ETL].[SP_Insert_Source_File_Archive]
            @Audit_Source_Import_SK = 3612,
            @Source_Import_SK = 8,
            @Original_File_Name = 'Brands.xlsx',
            @Archive_File_Name = 'Brands.xlsx',
            @Archive_Full_Path = 'C:\SISRI\raw\MDM\Brands.xlsx',
            @File_Row_Count = 212,
            @Process_Status = 'Success',
            @Source_File_Archive_SK = ? OUTPUT
    """, 0)  # Initial value for OUTPUT param
    
    # Try to get the output parameter
    cursor.execute("SELECT @@IDENTITY")
    identity_result = cursor.fetchone()
    print(f"@@IDENTITY result: {identity_result}")
    
    # Try SCOPE_IDENTITY
    cursor.execute("SELECT SCOPE_IDENTITY()")
    scope_result = cursor.fetchone()
    print(f"SCOPE_IDENTITY result: {scope_result}")
    
    conn.commit()
    
except Exception as e:
    print(f"Error: {e}")
    conn.rollback()

finally:
    cursor.close()
    conn.close()
