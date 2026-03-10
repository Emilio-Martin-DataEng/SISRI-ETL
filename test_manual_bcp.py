import subprocess
from src.config import get_db_config

cfg = get_db_config()

# Manual BCP command test
bcp_cmd = [
    "bcp",
    "ODS.Brands",
    "in", 
    r"c:\Users\Emilio\SISRI\temp\Brands_Brands_cleaned.txt",
    "-S", cfg["server"],
    "-d", cfg["database"], 
    "-U", cfg["username"],
    "-P", cfg["password"],
    "-f", r"c:\Users\Emilio\SISRI\config\format\sources\brands.fmt",
    "-e", r"c:\Users\Emilio\SISRI\temp\manual_bcp_test.log"
]

print("Running manual BCP test...")
print(f"Command: {' '.join(bcp_cmd[:6] + ['***'] + bcp_cmd[8:])}")

try:
    result = subprocess.run(bcp_cmd, capture_output=True, text=True, encoding="utf-8")
    print(f"Exit code: {result.returncode}")
    print(f"Stdout: {result.stdout}")
    print(f"Stderr: {result.stderr}")
except Exception as e:
    print(f"Error: {e}")
