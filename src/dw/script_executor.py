# src/dw/script_executor.py
"""
Execute .sql scripts from the run/ folder.
Combo: ODS/staging tables + merge procs to conformed staging = auto. DW tables/procs = human review.
"""

import re
from pathlib import Path
from datetime import datetime

from src.config import PROJECT_ROOT, get_config
from src.utils.db_ops import get_connection


def _get_ddl_paths():
    base = PROJECT_ROOT / get_config("dw_ddl", "base_folder", default="DW_DDL")
    return {
        "run": base / get_config("dw_ddl", "run_folder", default="run"),
        "archive": base / get_config("dw_ddl", "archive_folder", default="archive"),
    }


def _is_auto_executable(sql_content: str) -> bool:
    """ODS/staging tables and merge procs to conformed staging = auto. DW tables/procs = human review."""
    upper = sql_content.upper()
    if "CREATE TABLE [ODS]." in upper:
        return True
    if "CREATE TABLE [ETL].[STAGING_" in upper:
        return True
    # Merge procs that target conformed staging (e.g. SP_Merge_Fact_Sales_ODS_to_Conformed)
    if ("CREATE PROCEDURE" in upper or "CREATE OR ALTER PROCEDURE" in upper) and "INTO [ETL].[STAGING_FACT" in upper:
        return True
    return False


def execute_run_folder_scripts():
    """
    Execute ODS/staging DDL and conformed-staging merge procs from run/.
    DW tables and merge procs to DW tables are left for human intervention.
    On success: move to archive/ with timestamp.
    Returns (successful_files, [(file, error_msg), ...]).
    """
    paths = _get_ddl_paths()
    run_dir = paths["run"]
    archive_dir = paths["archive"]
    archive_dir.mkdir(parents=True, exist_ok=True)

    sql_files = sorted(run_dir.glob("*.sql")) if run_dir.exists() else []
    to_run = []
    skipped = []

    for fpath in sql_files:
        sql = fpath.read_text(encoding="utf-8")
        if _is_auto_executable(sql):
            to_run.append(fpath)
        else:
            skipped.append(fpath.name)

    if skipped:
        print(f"Skipped (need human review): {', '.join(skipped)}")

    successful = []
    failed = []
    conn = get_connection()
    cursor = conn.cursor()

    for fpath in to_run:
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

    cursor.close()
    conn.close()

    return successful, failed
