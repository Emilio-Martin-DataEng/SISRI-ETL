import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# Check ODS tables
tables = ['Products', 'Brands', 'Principals', 'Wholesalers', 'Places', 'Sales_Format_1']
for table in tables:
    try:
        cursor.execute(f'SELECT COUNT(*) FROM [ODS].[{table}]')
        count = cursor.fetchone()[0]
        print(f'[ODS].[{table}]: {count} rows')
    except Exception as e:
        print(f'[ODS].[{table}]: ERROR - {e}')

conn.close()
