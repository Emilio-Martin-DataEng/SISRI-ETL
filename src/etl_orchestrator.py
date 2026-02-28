# src/etl_orchestrator.py

import argparse
import logging
from datetime import datetime

from src.config import get_config
from src.staging.etl_config import process_etl_config
from src.staging.source_import import process_source
from src.utils.db_ops import get_connection, get_next_audit_import_id, log_audit_source_import, execute_proc
from src.dw.ddl_generator import generate_ddl_for_changed_sources

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="logs/etl_orchestrator.log",
    filemode="a",
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)


def run_config():
    """Load ETL config, merge to dims; generate DDL if mapping changed."""
    process_etl_config()
    changed = generate_ddl_for_changed_sources()
    if changed:
        logging.info(f"DDL generated for changed sources: {changed}")
    return changed


def run_staging(specific_sources: list[str] | None = None) -> tuple[list[str], list[str], int]:
    """Process all data sources (staging). Returns (processed, failed, total_rows)."""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT Source_Name, Source_Import_SK
        FROM ETL.Dim_Source_Imports
        WHERE Is_Active = 1 AND Is_Deleted = 0
          AND Source_Name NOT IN ('Source_Imports', 'Source_File_Mapping')
        ORDER BY Processing_Order ASC
    """)
    sources = cursor.fetchall()
    cursor.close()
    conn.close()

    processed = []
    failed = []
    total_rows = 0

    for source_name, source_sk in sources:
        if specific_sources and source_name not in specific_sources:
            continue
        logging.info(f"Processing source: {source_name}")
        for attempt in range(3):
            try:
                rows = process_source(source_name)
                processed.append(source_name)
                total_rows += rows or 0
                break
            except Exception as e:
                logging.error(f"Retry {attempt + 1}/3 for {source_name}: {e}")
                if attempt >= 2:
                    failed.append(source_name)
    return processed, failed, total_rows


def run_dimension_merges() -> list[str]:
    """Execute SP_Merge_Dim_* for each DW dimension. Returns list of merged sources."""
    dw_dims = get_config("dw_dimensions") or {}
    merged = []
    for source_name, dw_table in dw_dims.items():
        try:
            schema, tbl = dw_table.split(".")
            proc_name = f"{schema}.SP_Merge_{tbl}"
            execute_proc(proc_name)
            merged.append(source_name)
        except Exception as e:
            logging.error(f"Dimension merge failed for {source_name}: {e}")
            raise
    return merged


def run_full_etl(specific_sources: list[str] | None = None, from_step: str = "config"):
    """
    Orchestrates ETL. Supports --from config|staging|dims|facts.
    Each step includes its dependencies.
    """
    global_start = datetime.now()
    global_audit_id = None
    total_rows = 0

    try:
        # Config (always first when needed)
        if from_step in ("config", "staging", "dims", "facts"):
            logging.info("Step: Config load")
            run_config()

        if from_step in ("staging", "dims", "facts"):
            global_audit_id = get_next_audit_import_id()
            log_audit_source_import(
                audit_id=global_audit_id,
                source_import_sk=0,
                start_time=global_start,
                total_row_count=0,
                total_file_count=0,
                process_status="Running",
                pattern="Batch Run",
            )

            logging.info("Step: Staging")
            processed, failed, total_rows = run_staging(specific_sources)
            logging.info(f"Staging done: {processed}, failed: {failed}")

        if from_step in ("dims", "facts"):
            logging.info("Step: Dimension merges")
            merged = run_dimension_merges()
            logging.info(f"Merged dimensions: {merged}")

        if from_step == "facts":
            logging.info("Step: Fact inserts (not yet implemented)")
            pass

        if global_audit_id:
            log_audit_source_import(
                audit_id=global_audit_id,
                source_import_sk=0,
                start_time=global_start,
                end_time=datetime.now(),
                total_row_count=total_rows,
                total_file_count=0,
                exception_detail=None,
                pattern="Batch Run",
                process_status="Success",
            )

        logging.info(f"ETL completed in {(datetime.now() - global_start).total_seconds():.2f}s")

    except Exception as e:
        logging.error(f"ETL failed: {e}")
        if global_audit_id:
            log_audit_source_import(
                audit_id=global_audit_id,
                source_import_sk=0,
                start_time=global_start,
                end_time=datetime.now(),
                total_row_count=total_rows,
                total_file_count=0,
                exception_detail=str(e),
                process_status="Failed",
            )
        raise


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SISRI ETL Orchestrator")
    parser.add_argument(
        "--from",
        dest="from_step",
        choices=["config", "staging", "dims", "facts"],
        default="staging",
        help="Start from this step (includes dependencies)",
    )
    parser.add_argument("--sources", nargs="*", help="Process only these sources (staging)")
    parser.add_argument(
        "--apply-ddl",
        action="store_true",
        help="Execute .sql scripts from config/DW_DDL/run/ and archive on success",
    )
    args = parser.parse_args()

    if args.apply_ddl:
        from src.dw.script_executor import execute_run_folder_scripts

        ok, errs = execute_run_folder_scripts()
        if ok:
            logging.info(f"Executed and archived: {ok}")
        if errs:
            for f, e in errs:
                logging.error(f"{f}: {e}")
            raise SystemExit(1)
    else:
        run_full_etl(
            specific_sources=args.sources if args.sources else None,
            from_step=args.from_step,
        )
