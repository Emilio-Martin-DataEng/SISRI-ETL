import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# List of dimension tables to truncate
dimension_tables = [
    '[ODS].[Principals]',
    '[ODS].[Brands]', 
    '[ODS].[Wholesalers]',
    '[ODS].[Products]',
    '[ODS].[Places]'
]

print("Truncating dimension tables...")
for table in dimension_tables:
    try:
        cursor.execute(f"TRUNCATE TABLE {table}")
        print(f"✅ Truncated {table}")
    except Exception as e:
        print(f"❌ Failed to truncate {table}: {e}")

conn.commit()

# Verify tables are empty
print("\nVerifying table counts...")
for table in dimension_tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"{table}: {count} rows")

conn.close()
print("\n✅ All dimension tables truncated successfully!")
