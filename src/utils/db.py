# src/utils/db.py

from pathlib import Path
import subprocess
from typing import Optional, Dict
from datetime import datetime

from src.config import get_db_config, get_logs_dir, get_config

# import psycopg2  # Commented out until PostgreSQL is needed


def upload_via_bcp(
    file_path: Path,
    table: str,
    db_config: Dict[str, str],
    format_file: Optional[str] = None,
    first_row: int = 1,
    extra_args: Optional[list] = None,
) -> None:
    """
    Upload tab-delimited text file to SQL Server using BCP.
    Uses configured Logs folder for error logs.
    """
    bcp_cmd = [
        "bcp",
        table,
        "in",
        str(file_path),
        "-S", db_config["server"],
        "-d", db_config["database"],
        "-U", db_config["username"],
        "-P", db_config["password"],
        "-F", str(first_row),
    ]

    if format_file:
        bcp_cmd.extend(['-f', format_file])
        # Don't specify -r when using format file - use terminators from format file
    else:
        bcp_cmd.extend(['-c', '-t', '\\t', '-r', '\\r\\n'])
        
    # Dynamic, timestamped error log in Logs/
    logs_dir = get_logs_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    prefix = get_config("logs", "bcp_errors_prefix", default="bcp_errors")
    error_log_name = f"{prefix}_{timestamp}.log"
    error_log_path = logs_dir / error_log_name
    bcp_cmd.extend(["-e", str(error_log_path)])

    print(f"BCP error log -> {error_log_path}")

    # Mask password in debug output
    masked_cmd = [str(arg) for arg in bcp_cmd]  # force str conversion
    if "-P" in masked_cmd:
        pwd_index = masked_cmd.index("-P") + 1
        masked_cmd[pwd_index] = "***"


    try:
        result = subprocess.run(
            bcp_cmd,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
        )

        print("BCP succeeded.")
        print("Stdout:")
        print(result.stdout.strip() or "<no output>")

        # Check for rejections
        if error_log_path.exists() and error_log_path.stat().st_size > 0:
            print(f"\nRejected rows in {error_log_path}:")
            with open(error_log_path, "r", encoding="utf-8") as f:
                print(f.read().strip())
        else:
            print("No rejected rows logged.")

    except FileNotFoundError:
        raise RuntimeError(
            "bcp.exe not found. Install SQL Server Command Line Utilities and add to PATH."
        )

    except subprocess.TimeoutExpired:
        raise RuntimeError("BCP timed out after 5 minutes.")

    except subprocess.CalledProcessError as e:
        error_msg = (
            f"BCP failed (exit code {e.returncode})\n"
            f"Command: {' '.join(masked_cmd)}\n"
            f"Stdout: {e.stdout.strip() or '<empty>'}\n"
            f"Stderr: {e.stderr.strip() or '<empty>'}\n"
            f"Error log: {error_log_path}"
        )
        if error_log_path.exists() and error_log_path.stat().st_size > 0:
            error_msg += "\nRejected rows:\n" + error_log_path.read_text(encoding="utf-8").strip()
        raise RuntimeError(error_msg)