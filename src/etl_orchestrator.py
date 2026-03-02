# src/etl_orchestrator.py
"""
High-level ETL orchestrator.
Runs config load → data sources → audit/reporting.
"""

from datetime import datetime
import logging

from src.config import SYSTEM_BASE_PATH
from src.staging.etl_config import process_etl_config
from src.staging.source_import import process_source, CONFIG_SOURCES
from src.utils.db_ops import get_connection, log_audit_source_import, get_next_audit_import_id

# Logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=str(SYSTEM_BASE_PATH() / "logs" / "etl_orchestrator.log"),
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def run_full_etl(specific_sources: list[str] | None = None):
    """Runs the full ETL pipeline.
    Refreshes config, processes sources, shows audit summary.
    """
    global_start = datetime.now()
    logging.info(f"Full ETL run started at {global_start}")

    global_audit_id = get_next_audit_import_id()
    log_audit_source_import(
        audit_id=global_audit_id,
        source_import_sk=0,
        start_time=global_start,
        total_row_count=0,
        total_file_count=0,
        process_status='Running',
        pattern='Batch Run'
    )
    logging.info(f"Global audit entry: {global_audit_id}")

    total_rows = 0
    processed_sources = []
    failed_sources = []

    try:
        # Step 1: Refresh config (includes DDL generation if mapping changed)
        logging.info("Refreshing ETL config...")
        process_etl_config()

        # Step 2: Process active data sources
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Source_Name, Source_Import_SK
            FROM [ETL].[Dim_Source_Imports]
            WHERE Is_Active = 1 
              AND Is_Deleted = 0 
              AND Source_Name NOT IN ('Source_Imports', 'Source_File_Mapping')
            ORDER BY Processing_Order ASC
        """)
        sources = cursor.fetchall()
        cursor.close()
        conn.close()

        if not sources:
            logging.warning("No active data sources found.")
            log_audit_source_import(
                audit_id=global_audit_id,
                source_import_sk=0,
                start_time=global_start,
                end_time=datetime.now(),
                total_row_count=0,
                total_file_count=0,
                exception_detail="No sources",
                process_status='Skipped'
            )
            return

        logging.info(f"Processing {len(sources)} sources.")

        for source_name, source_sk in sources:
            if specific_sources and source_name not in specific_sources:
                continue

            logging.info(f"Processing {source_name} (SK={source_sk})")
            source_start = datetime.now()

            retries = 0
            success = False
            source_rows = 0
            while retries < 3 and not success:
                try:
                    source_rows = process_source(source_name)
                    success = True
                    processed_sources.append(source_name)
                    total_rows += source_rows
                except Exception as e:
                    retries += 1
                    logging.error(f"Retry {retries}/3 for {source_name}: {e}")
                    if retries >= 3:
                        failed_sources.append(source_name)
                        logging.error(f"Failed {source_name} after 3 retries: {e}")

            duration = (datetime.now() - source_start).total_seconds()
            logging.info(f"{source_name} done in {duration:.2f}s (rows: {source_rows})")

        # Finalize global audit
        global_end = datetime.now()
        status = 'Success' if not failed_sources else 'Partial Success'
        detail = f"Failed: {failed_sources}" if failed_sources else None

        log_audit_source_import(
            audit_id=global_audit_id,
            source_import_sk=0,
            start_time=global_start,
            end_time=global_end,
            total_row_count=total_rows,
            total_file_count=0,
            exception_detail=detail,
            pattern='Batch Run',
            process_status=status
        )

        logging.info(f"Full ETL run complete at {global_end}")
        logging.info(f"Duration: {(global_end - global_start).total_seconds():.2f}s")
        logging.info(f"Total rows processed: {total_rows}")
        logging.info(f"Processed sources: {processed_sources}")
        if failed_sources:
            logging.warning(f"Failed sources: {failed_sources}")

        # Optional: Print recent audit summary
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TOP 5 
                Audit_Source_Import_SK, Source_Import_SK, Start_Time, End_Time, 
                Total_Row_Count, Total_File_Count, Process_Status, Exception_Detail
            FROM [ETL].[Fact_Audit_Source_Imports]
            ORDER BY Start_Time DESC
        """)
        rows = cursor.fetchall()
        print("\nRecent ETL Audit Summary:")
        for row in rows:
            print(f"  Run {row[0]} | Source {row[1]} | {row[2]} → {row[3]} | Rows: {row[4]} | Files: {row[5]} | Status: {row[6]} | Detail: {row[7] or 'None'}")
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error(f"Orchestrator failure: {e}")
        if global_audit_id:
            log_audit_source_import(
                audit_id=global_audit_id,
                source_import_sk=0,
                start_time=global_start,
                end_time=datetime.now(),
                total_row_count=total_rows,
                total_file_count=0,
                exception_detail=str(e),
                process_status='Failed'
            )
        raise

if __name__ == "__main__":
    run_full_etl()