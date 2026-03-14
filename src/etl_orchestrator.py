# src/etl_orchestrator.py
"""ETL data processing (System Operator). Reads from Dim_Source_Imports; no config loading."""

from datetime import datetime
import argparse
import shutil
import sys
from pathlib import Path

from src.config import PROJECT_ROOT
from src.staging.source_import import process_source
from src.staging.fact_sales_import import process_fact_sales
from src.utils.db_ops import (
    log_audit_source_import,
    get_next_audit_import_id,
    execute_proc,
    get_connection,
)
from src.utils.logging_config import setup_logging


def run_etl(sources=None, force_ddl=False):
    logger = setup_logging("etl_orchestrator")
    start_time = datetime.now()
    
    # Clean temp directory to prevent relics
    temp_dir = PROJECT_ROOT / "temp"
    if temp_dir.exists():
        logger.info(f"Cleaning temp directory: {temp_dir}")
        shutil.rmtree(temp_dir)
        temp_dir.mkdir(exist_ok=True)
        logger.info("Temp directory cleaned successfully")
    
    global_audit_id = get_next_audit_import_id()
    log_audit_source_import(
        global_audit_id,
        source_import_sk=0,
        start_time=start_time,
        process_status='Running',
        pattern='Full ETL Run'
    )
    logger.info(f"Full ETL run started at {start_time}")

    if sources is None:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Source_Name, Source_Type, Merge_Proc_Name, Staging_Table
            FROM [ETL].[Dim_Source_Imports] 
            WHERE Is_Active = 1 
            AND Is_Deleted = 0 
            AND Processing_Order >= 1.0
            ORDER BY Processing_Order ASC
        """)
        sources_rows = cursor.fetchall()  # list of (Source_Name, Source_Type, Merge_Proc_Name, Staging_Table)
        cursor.close()
        conn.close()
    else:
        # Handle --sources arg (fetch full row for specified names)
        conn = get_connection()
        cursor = conn.cursor()
        placeholders = ','.join('?' for _ in sources)
        cursor.execute(f"""
            SELECT Source_Name, Source_Type, Merge_Proc_Name, Staging_Table
            FROM [ETL].[Dim_Source_Imports] 
            WHERE Source_Name IN ({placeholders})
        """, sources)
        sources_rows = cursor.fetchall()
        cursor.close()
        conn.close()

    logger.info(f"Processing {len(sources_rows)} sources.")

    total_rows = 0
    for row in sources_rows:
        source_name     = row[0]
        source_type     = row[1] if row[1] is not None else 'Dimension'  # safe fallback
        merge_proc_name = row[2] if row[2] is not None else f"ETL.SP_Merge_Dim_{source_name}"
        staging_table   = row[3] if row[3] is not None else None

        logger.info(f"Processing source: {source_name} (Type: {source_type}, Merge Proc: {merge_proc_name}, Staging: {staging_table})")

        try:
            if source_type == 'Dimension':
                # Existing dimension path
                rows_processed = process_source(source_name, force_ddl=force_ddl, audit_id=global_audit_id)
                total_rows += rows_processed if rows_processed else 0

                # Skip merge for dimensions (now handled per file in source_import.py)
                logger.info(f"Skipping merge for {source_name} (handled per file in source_import.py)")

            elif source_type == 'Fact_Sales':
                # Fact Sales: load to ODS via fact_sales_import, then move to conformed staging via SP
                rows_processed = process_fact_sales(source_name, force_ddl=force_ddl, audit_id=global_audit_id)
                total_rows += rows_processed if rows_processed else 0

                logger.info(f"Calling ETL.SP_Merge_Fact_Sales_ODS_to_Conformed for {source_name}")
                execute_proc(
                    "ETL.SP_Merge_Fact_Sales_ODS_to_Conformed",
                    params_dict={
                        "@SourceName": source_name,
                        "@Source_File_Archive_SK": -1,
                        "@Audit_Source_Import_SK": global_audit_id,
                    },
                )

            elif source_type == 'Fact_Conformed':
                # Move from conformed staging to DW.Fact_Sales
                logger.info("Calling ETL.SP_Merge_Fact_Sales to load DW.Fact_Sales from staging")
                execute_proc("ETL.SP_Merge_Fact_Sales")

            elif source_type == 'System':
                logger.debug(f"Skipping System source: {source_name}")
                continue

            else:
                logger.warning(f"Unknown Source_Type '{source_type}' for {source_name} - skipping")
                continue

            # Update checkpoint
            try:
                conn = get_connection()  # fresh
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE [ETL].[Dim_Source_Imports] 
                    SET Last_Successful_Load_Datetime = GETDATE() 
                    WHERE Source_Name = ?
                    """,
                    (source_name,),
                )
                conn.commit()
            except Exception as update_err:
                logger.warning(f"Failed to update Last_Successful_Load_Datetime for {source_name}: {update_err}")
            finally:
                if 'cursor' in locals() and cursor:
                    cursor.close()
                if 'conn' in locals() and conn:
                    conn.close()

        except Exception as e:
            error_summary = str(e).split('\n')[-1] if str(e) else "Unknown error"
            log_audit_source_import(
                global_audit_id,
                source_import_sk=0,
                start_time=start_time,
                end_time=datetime.now(),
                process_status='Failed',
                exception_detail=f"{source_name} failed: {error_summary}"
            )
            logger.error(f"{source_name} failed - {error_summary}")
            logger.error("Stopping execution. Restart from failed source.")
            break


    end_time = datetime.now()
    log_audit_source_import(
        global_audit_id,
        source_import_sk=0,
        start_time=start_time,
        end_time=end_time,
        total_row_count=total_rows,
        total_file_count=len(sources_rows),
        process_status='Success',
        pattern='Full ETL Run'
    )
    logger.info(f"Full ETL run complete at {end_time}")
    logger.info(f"Duration: {(end_time - start_time).total_seconds():.2f}s")
    logger.info(f"Total rows processed: {total_rows}")
    logger.info(f"Processed {len(sources_rows)} sources")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SISRI ETL – Data processing (System Operator). Assumes metadata is correct."
    )
    parser.add_argument("--sources", nargs="+", default=None, help="Specific sources to process (default: all active)")
    parser.add_argument("--test", choices=["full"], default=None, help="Run integrated scenario tests (full)")
    args = parser.parse_args()

    if args.test == "full":
        from tests.test_scenarios import main as run_scenario_tests
        sys.exit(run_scenario_tests())

    run_etl(sources=args.sources)