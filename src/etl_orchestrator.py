# src/etl_orchestrator.py

from datetime import datetime
import logging

from src.staging.etl_config import process_etl_config
from src.staging.source_import import process_source, CONFIG_SOURCES
from src.utils.db_ops import get_connection, get_next_audit_import_id, log_audit_source_import


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/etl_orchestrator.log',
    filemode='a'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)


def run_full_etl(specific_sources: list[str] | None = None):
    """
    Orchestrates the full ETL pipeline:
    1. Refreshes metadata config
    2. Creates one global audit entry for the entire run
    3. Processes each active data source in order (skips config sources)
    4. Aggregates metrics and updates global audit
    5. Supports specific-sources override
    """
    global_start = datetime.now()
    logging.info(f"Full ETL run started at {global_start}")

    global_audit_id = None
    total_rows = 0
    processed_sources = []
    failed_sources = []

    try:
        # Step 1: Refresh metadata config
        logging.info("Refreshing ETL configuration...")
        process_etl_config()

        # Step 2: Create global audit entry for the entire batch run
        global_audit_id = get_next_audit_import_id()
        log_audit_source_import(
            audit_id=global_audit_id,
            source_import_sk=0,  # or SK of a dummy 'Batch Run' source if desired
            start_time=global_start,
            total_row_count=0,
            total_file_count=0,
            process_status='Running',
            pattern='Batch Run'
        )
        logging.info(f"Global audit entry created: Audit_Source_Import_SK = {global_audit_id}")

        # Step 3: Fetch active data sources (exclude config/metadata ones)
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Source_Name, Source_Import_SK
            FROM ETL.Dim_Source_Imports
            WHERE Is_Active = 1 
              AND Is_Deleted = 0 
              AND Source_Name NOT IN ('Source_Imports', 'Source_File_Mapping')
            ORDER BY Processing_Order ASC
        """)
        sources = cursor.fetchall()  # list of (name, sk)
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
                exception_detail="No active data sources",
                process_status='Skipped'
            )
            return

        logging.info(f"Processing {len(sources)} active data sources.")

        # Step 4: Process each source
        for source_name, source_sk in sources:
            if specific_sources and source_name not in specific_sources:
                continue

            logging.info(f"\n=== Processing source: {source_name} (SK={source_sk}) ===")
            source_start = datetime.now()

            retries = 0
            success = False
            source_rows = 0
            while retries < 3 and not success:
                try:
                    # Call process_source → assume we modify it to return row count
                    source_rows = process_source(source_name)  # ← needs return value!
                    success = True
                    processed_sources.append(source_name)
                    total_rows += source_rows
                except Exception as e:
                    retries += 1
                    logging.error(f"Retry {retries}/3 for {source_name}: {e}")
                    if retries >= 3:
                        failed_sources.append(source_name)
                        logging.error(f"Failed {source_name} after 3 retries: {e}")

            source_duration = (datetime.now() - source_start).total_seconds()
            logging.info(f"{source_name} completed in {source_duration:.2f}s (rows: {source_rows})")

        # Step 5: Finalize global audit
        global_end = datetime.now()
        status = 'Success' if not failed_sources else 'Partial Success'
        exception_detail = f"Failed sources: {failed_sources}" if failed_sources else None

        log_audit_source_import(
            audit_id=global_audit_id,
            source_import_sk=0,
            start_time=global_start,
            end_time=global_end,
            total_row_count=total_rows,
            total_file_count=0,  # or count files across all sources if aggregated
            exception_detail=exception_detail,
            pattern='Batch Run',
            process_status=status
        )

        logging.info(f"Full ETL completed at {global_end}")
        logging.info(f"Duration: {(global_end - global_start).total_seconds():.2f}s")
        logging.info(f"Total rows loaded: {total_rows}")
        logging.info(f"Processed: {processed_sources}")
        if failed_sources:
            logging.warning(f"Failed sources: {failed_sources}")

    except Exception as e:
        logging.error(f"Critical orchestrator failure: {e}")
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
    run_full_etl()                     # all active data sources
    # run_full_etl(['Principals'])     # test single source
    # run_full_etl(['Brands', 'Principals'])