import subprocess
from src.config import get_db_config

cfg = get_db_config()

# Test with fresh file
bcp_cmd = [
    "bcp",
    "ODS.Brands",
    "in", 
    r"c:\Users\Emilio\SISRI\temp\test_brands_fresh.txt",
    "-S", cfg["server"],
    "-d", cfg["database"], 
    "-U", cfg["username"],
    "-P", cfg["password"],
    "-f", r"c:\Users\Emilio\SISRI\config\format\sources\brands.fmt",
    "-e", r"c:\Users\Emilio\SISRI\temp\fresh_bcp_test.log"
]

print("Testing BCP with fresh file...")
try:
    result = subprocess.run(bcp_cmd, capture_output=True, text=True, encoding="utf-8")
    print(f"Exit code: {result.returncode}")
    print(f"Stdout: {result.stdout}")
    if result.stderr:
        print(f"Stderr: {result.stderr}")
except Exception as e:
    print(f"Error: {e}")
