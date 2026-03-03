import argparse
from datetime import datetime

from src.staging.etl_config import process_etl_config
from src.staging.source_import import process_source
from src.utils.db_ops import log_audit_source_import, get_next_audit_import_id, execute_proc, get_connection
from src.utils.ddl_generator import apply_ddl_from_run

def run_etl(sources=None, force_ddl=False):
    start_time = datetime.now()
    global_audit_id = get_next_audit_import_id()
    log_audit_source_import(
        global_audit_id,
        source_import_sk=0,
        start_time=start_time,
        process_status='Running',
        pattern='Full ETL Run'
    )
    print(f"Full ETL run started at {start_time}")

    process_etl_config()

    if sources is None:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT Source_Name FROM [ETL].[Dim_Source_Imports] WHERE Is_Active = 1 AND Is_Deleted = 0")
        sources = [row[0] for row in cursor.fetchall()]
        cursor.close()
        conn.close()

    print(f"Processing {len(sources)} sources (force DDL: {force_ddl}).")

    total_rows = 0
    for source_name in sources:
        try:
            # Skip if already processed today
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT Last_Successful_Load_Datetime FROM [ETL].[Dim_Source_Imports] WHERE Source_Name = ?", source_name)
            last_success = cursor.fetchone()
            if last_success and last_success[0] and last_success[0].date() == datetime.now().date():
                print(f"[SKIP] {source_name} already processed today")
                continue

            rows = process_source(source_name, force_ddl=force_ddl)
            total_rows += rows

            # Phase 2: Run merge
            cursor.execute("SELECT Merge_Proc_Name FROM [ETL].[Dim_Source_Imports] WHERE Source_Name = ?", source_name)
            merge_proc_row = cursor.fetchone()
            merge_proc_name = merge_proc_row[0] if merge_proc_row and merge_proc_row[0] else f"ETL.SP_Merge_Dim_{source_name}"
            execute_proc(merge_proc_name)
            print(f"  Merged {source_name} using {merge_proc_name}")

            # Update checkpoint
            cursor.execute("UPDATE [ETL].[Dim_Source_Imports] SET Last_Successful_Load_Datetime = GETDATE() WHERE Source_Name = ?", source_name)
            conn.commit()
            cursor.close()
            conn.close()

        except Exception as e:
            if 'conn' in locals():
                conn.rollback()
                cursor.close()
                conn.close()
            error_summary = str(e).split('\n')[-1] if str(e) else "Unknown error"
            log_audit_source_import(
                global_audit_id,
                source_import_sk=0,
                start_time=start_time,
                end_time=datetime.now(),
                process_status='Failed',
                exception_detail=f"{source_name} failed: {error_summary}"
            )
            print(f"ERROR: {source_name} failed - {error_summary}")
            print("Stopping execution. Restart from failed source.")
            break

    end_time = datetime.now()
    log_audit_source_import(
        global_audit_id,
        source_import_sk=0,
        start_time=start_time,
        end_time=end_time,
        total_row_count=total_rows,
        total_file_count=len(sources),
        process_status='Success',
        pattern='Full ETL Run'
    )
    print(f"Full ETL run complete at {end_time}")
    print(f"Duration: {(end_time - start_time).total_seconds():.2f}s")
    print(f"Total rows processed: {total_rows}")
    print(f"Processed sources: {sources}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="SISRI ETL Orchestrator\n\n"
                    "Runs full ETL: config refresh → source loads (ODS) → dimension merges (DW).\n"
                    "Options:\n"
                    "  --sources Principals Brands     Run only these sources\n"
                    "  --force-ddl                     Force DDL regeneration for all sources\n"
                    "  --help                          Show this help",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--sources", nargs="+", default=None, help="Specific sources to process")
    parser.add_argument("--force-ddl", action="store_true", help="Force DDL generation")
    args = parser.parse_args()

    run_etl(args.sources, args.force_ddl)
    # After all sources
    apply_ddl_from_run()