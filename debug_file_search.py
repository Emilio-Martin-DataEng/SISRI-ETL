from pathlib import Path
from src.config import get_config

# Check what path the ETL is actually using
raw_root = get_config("base", "file_root")
print(f"Raw root from config: {raw_root}")

# Check Products configuration
import pyodbc
from src.config import get_db_config

cfg = get_db_config()
conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={cfg["server"]};DATABASE={cfg["database"]};UID={cfg["username"]};PWD={cfg["password"]}'
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

cursor.execute("""
    SELECT Rel_Path, Pattern 
    FROM [ETL].[Dim_Source_Imports] 
    WHERE Source_Name = 'Products'
""")
row = cursor.fetchone()
rel_path = row[0]
pattern = row[1]

print(f"Products Rel_Path: {rel_path}")
print(f"Products Pattern: {pattern}")

# Construct full path
data_dir = Path(raw_root) / rel_path.strip().lstrip('/\\')
print(f"Full data directory: {data_dir}")

# List files
if data_dir.exists():
    files = list(data_dir.glob(pattern))
    print(f"Files found: {[f.name for f in files]}")
else:
    print(f"Directory does not exist: {data_dir}")

conn.close()
