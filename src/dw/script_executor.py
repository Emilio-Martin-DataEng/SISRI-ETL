# src/dw/script_executor.py
"""
Execute .sql scripts from the run/ folder, archive on success.
Engineer copies approved scripts from generated/ to run/, then triggers this.
"""

import re
from pathlib import Path
from datetime import datetime

from src.config import CONFIG_ROOT, get_config
from src.utils.db_ops import get_connection


def _get_ddl_paths():
    base = CONFIG_ROOT / get_config("dw_ddl", "base_folder", default="DW_DDL")
    return {
        "run": base / get_config("dw_ddl", "run_folder", default="run"),
        "archive": base / get_config("dw_ddl", "archive_folder", default="archive"),
    }


def execute_run_folder_scripts():
    """
    Execute all .sql files in run/ folder.
    On success: move to archive/ with timestamp.
    On failure: leave in run/, return error.
    Returns (successful_files, [(file, error_msg), ...]).
    """
    paths = _get_ddl_paths()
    run_dir = paths["run"]
    archive_dir = paths["archive"]
    archive_dir.mkdir(parents=True, exist_ok=True)

    sql_files = sorted(run_dir.glob("*.sql")) if run_dir.exists() else []
    successful = []
    failed = []

    conn = get_connection()
    cursor = conn.cursor()

    for fpath in sql_files:
        try:
            sql = fpath.read_text(encoding="utf-8")
            batches = [b.strip() for b in re.split(r"^\s*GO\s*$", sql, flags=re.MULTILINE) if b.strip()]
            for batch in batches:
                cursor.execute(batch)
            conn.commit()
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_path = archive_dir / f"{fpath.stem}_{ts}{fpath.suffix}"
            fpath.rename(archive_path)
            successful.append(str(fpath.name))
        except Exception as e:
            conn.rollback()
            failed.append((fpath.name, str(e)))
        finally:
            pass

    cursor.close()
    conn.close()

    return successful, failed
