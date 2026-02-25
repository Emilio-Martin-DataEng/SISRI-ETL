# src/etl_orchestrator.py

# Imports for timing, logging, and DB connections
from datetime import datetime
import logging

# Project modules
from src.staging.etl_config import process_etl_config
from src.staging.source_import import process_source
from src.utils.db_ops import get_connection


# Set up logging to file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='logs/etl_orchestrator.log'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)


def run_full_etl(specific_sources: list = None):
    """
    The full ETL orchestrator script.
    
    This runs the complete ETL process:
    1. Refreshes the ETL config by running process_etl_config()
    2. Fetches all active sources from ETL.Dim_Source_Imports
    3. Processes each source in Processing_Order ASC
    4. Handles errors: continues to next source on failure
    5. Logs global start/end time, duration, processed sources, total rows
    6. Can run specific sources if provided (e.g. run_full_etl(['Principals']))
    
    Additional features:
    - Retry logic: retries failed sources up to 3 times
    - Notification: logs errors (future: add email/slack)
    - Dependencies: assumes order covers dependencies (future: add explicit checks)
    - Parallelism: not implemented (sequential for order)
    """
    global_start = datetime.now()
    logging.info(f"Full ETL run started at {global_start}")

    try:
        # Step 1: Refresh ETL config first (loads/merges config tables)
        logging.info("Refreshing ETL config...")
        process_etl_config()

        # Step 2: Fetch all active sources in Processing_Order
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Source_Name
            FROM ETL.Dim_Source_Imports
            WHERE Is_Active = 1 AND Is_Deleted = 0
            ORDER BY Processing_Order ASC
        """)
        sources = [row.Source_Name for row in cursor.fetchall()]
        cursor.close()
        conn.close()

        if not sources:
            logging.warning("No active sources found in Dim_Source_Imports.")
            return

        logging.info(f"Found {len(sources)} active sources to process.")

        # Step 3: Process each source sequentially
        total_rows = 0
        failed_sources = []
        for source in sources:
            if specific_sources and source not in specific_sources:
                continue

            logging.info(f"\n=== Processing source: {source} ===")
            source_start = datetime.now()
            retries = 0
            success = False
            while retries < 3 and not success:
                try:
                    process_source(source)
                    success = True
                    # Assume process_source returns row_count; for now, mock
                    source_rows = 3  # Replace with actual from process_source
                    total_rows += source_rows
                except Exception as e:
                    retries += 1
                    logging.error(f"Retry {retries}/3 for {source}: {e}")
                    if retries >= 3:
                        failed_sources.append(source)
                        logging.error(f"Failed {source} after 3 retries: {e}")

            source_end = datetime.now()
            logging.info(f"{source} completed in {(source_end - source_start).total_seconds():.2f} seconds")

        # Step 4: Global completion log
        global_end = datetime.now()
        logging.info(f"Full ETL run completed at {global_end}")
        logging.info(f"Total duration: {(global_end - global_start).total_seconds():.2f} seconds")
        logging.info(f"Total rows loaded: {total_rows}")
        if failed_sources:
            logging.error(f"Failed sources: {failed_sources}")

    except Exception as e:
        logging.error(f"Full ETL run failed: {e}")
        raise


if __name__ == "__main__":
    run_full_etl()  # Run all active sources in order
    # run_full_etl(['Principals', 'Brands'])  # Run specific sources