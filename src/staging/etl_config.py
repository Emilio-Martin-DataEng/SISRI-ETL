# src/staging/etl_config.py (full update)

from pathlib import Path
import pandas as pd
import csv
from datetime import datetime

from src.config import BASE_PATH, SYSTEM_BASE_PATH, get_db_config, get_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import (
    truncate_table,
    execute_proc,
    log_audit_source_import,
    get_next_audit_import_id,
    get_source_import_sk
)
from src.utils.ddl_generator import (  # NEW: Point 1-5,7
    generate_ods_table_ddl,
    generate_bcp_format_file,
    apply_ddl_from_run
)

def process_etl_config():
    """
    Loads ETL configuration from Excel into staging tables,
    merges to dimension tables via stored procedures,
    and logs the process to audit table.
    """
    start_time = datetime.now()

    # NEW: Get the real Source_Import_SK for the config loader itself FIRST
    config_source_sk = get_source_import_sk('Source_Imports')  # 'Source_Imports' is the name in Dim_Source_Imports

    # If no SK yet (first run or not merged), fall back to 0
    if config_source_sk == 0:
        print("[WARNING] No Source_Import_SK found for 'Source_Imports' yet — using 0 temporarily")

    # Create audit entry using the real SK (or 0)
    audit_id = get_next_audit_import_id()

    log_audit_source_import(
        audit_id=audit_id,
        source_import_sk=config_source_sk,          # ← real SK here!
        start_time=start_time,
        end_time=None,
        total_row_count=0,
        total_file_count=0,
        exception_detail=None,
        pattern=None,
        process_status='Running'
    )
    print(f"[AUDIT] Started audit entry: Audit_Source_Import_SK = {audit_id} "
          f"(linked to Source_Import_SK = {config_source_sk})")
    
    error_count = 0  # NEW: Point 9 for no empty error logs

    try:
        config_folder_name = get_config("base", "config_folder") or get_config("system", "config_folder") or "config"
        config_filename = get_config("base", "config_filename") or get_config("system", "config_filename") or "ETL_Config.xlsx"

        config_folder = SYSTEM_BASE_PATH() / config_folder_name
        config_files = list(config_folder.glob(config_filename))
        config_files = list(config_folder.glob(config_filename))
        
        if not config_files:
            end_time = datetime.now()
            log_audit_source_import(
                audit_id=audit_id,
                source_import_sk=0,
                start_time=start_time,
                end_time=end_time,
                total_row_count=0,
                total_file_count=0,
                exception_detail="Config spreadsheet not found",
                pattern=None,
                process_status='Skipped'
            )
            print(f"[SKIP] Config file missing: {config_folder / config_filename}")
            return

        config_file = config_files[0]
        print(f"Loading config from: {config_file}")

        format_dir = config_folder / "format"
        format_imports = format_dir / "source_imports.fmt"
        format_mapping = format_dir / "source_file_mapping.fmt"
        format_dir.mkdir(exist_ok=True)

        df_imports = pd.read_excel(config_file, sheet_name="Source_Imports", dtype=str)
        df_mapping = pd.read_excel(config_file, sheet_name="Source_File_Mapping", dtype=str)  # Now has Is_Required

        df_imports = df_imports.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df_mapping = df_mapping.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
        df_imports['Inserted_Datetime'] = now_str
        df_mapping['Inserted_Datetime'] = now_str

        temp_dir = SYSTEM_BASE_PATH() / (get_config("system", "temp_folder") or "temp")
        temp_dir.mkdir(exist_ok=True)
        
        imports_path = temp_dir / "source_imports_stg.txt"
        mapping_path = temp_dir / "source_file_mapping_stg.txt"

        df_imports.to_csv(imports_path, sep='\t', index=False, header=False, encoding='utf-8', lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')
        df_mapping.to_csv(mapping_path, sep='\t', index=False, header=False, encoding='utf-8', lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')

        db_cfg = get_db_config()

        truncate_table('ETL.Source_Imports')
        truncate_table('ETL.Source_File_Mapping')

        # Generate formats (you already have this)
        generate_bcp_format_file('Source_Imports', str(format_imports))
        generate_bcp_format_file('Source_File_Mapping', str(format_mapping))

        # Relative paths for BCP
        rel_imports_txt   = imports_path.relative_to(BASE_PATH())
        rel_mapping_txt   = mapping_path.relative_to(BASE_PATH())
        rel_imports_fmt   = format_imports.relative_to(BASE_PATH())
        rel_mapping_fmt   = format_mapping.relative_to(BASE_PATH())

        # BCP calls
        upload_via_bcp(
            file_path=rel_imports_txt,               # relative Path object or str(rel_imports_txt)
            table='ETL.Source_Imports',
            db_config=db_cfg,
            format_file=str(rel_imports_fmt),        # relative!
            first_row=1
        )

        upload_via_bcp(
            file_path=rel_mapping_txt,
            table='ETL.Source_File_Mapping',
            db_config=db_cfg,
            format_file=str(rel_mapping_fmt),
            first_row=1
        )

        execute_proc('ETL.SP_Merge_Dim_Source_Imports')
        execute_proc('ETL.SP_Merge_Dim_Source_Imports_Mapping')

        # NEW: Point 3,5,7 - Auto-create ODS + .fmt for new/active sources using Is_Required
        active_sources = df_imports[df_imports['Is_Active'] == '1']['Source_Name'].unique()
        for source in active_sources:
            source_mapping = df_mapping[df_mapping['Source_Name'] == source].to_dict('records')
            ods_ddl = generate_ods_table_ddl(source, source_mapping)
            # Execute ods_ddl (via sp_executesql or file)
            execute_proc(f"EXEC sp_executesql N'{ods_ddl}'")
            generate_bcp_format_file(source, source_mapping, format_dir)

        real_sk = get_source_import_sk('Source_Imports')
        print(f"[DEBUG] Config SK: {real_sk}")

        # Success update
        end_time = datetime.now()
        row_count = len(df_imports) + len(df_mapping)
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=config_source_sk,          # consistent with start
            start_time=start_time,
            end_time=end_time,
            total_row_count=row_count,
            total_file_count=1,
            exception_detail=None,
            pattern=config_filename,
            process_status='Success'
        )
 

        # NEW: Point 6 - Apply DDL if files in run/
        apply_ddl_from_run()

    except Exception as e:
        end_time = datetime.now()
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=config_source_sk,
            start_time=start_time,
            end_time=end_time,
            total_row_count=0,
            total_file_count=0,
            exception_detail=str(e),
            pattern=None,
            process_status='Failed'
        )
        print(f"Config failed: {e}")

        # Safe logs dir creation
        logs_dir = SYSTEM_BASE_PATH() / (get_config("system", "logs_folder") or "logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        error_log_path = logs_dir / f"etl_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(error_log_path, "w", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: {str(e)}\n")
        print(f"Error details saved to: {error_log_path}")

        raise

    finally:
        # NEW: Point 9 - Only write error log if errors
        if error_count > 0:
            with open(BASE_PATH() / "logs" / "etl_errors.log", "a") as f:
                f.write(f"{datetime.now()}: {str(e)}\n")  # Assume e from except
            print("⚠️ Check logs/etl_errors.log for details")
        else:
            print("✅ No errors - clean run")

        # NEW: Point 8 - Final steps message
        print("""
✅ ETL Config Complete!
Next Steps:
1. Review config/DW_DDL/generated/ for new DDL scripts
2. Approve/move to /run/ folder
3. Re-run python -m src.staging.etl_config to apply DDL
4. Enable new sources in Excel (set Is_Active=1, fill Is_Required)
5. Place source files in raw/ folders and run orchestrator
""")

if __name__ == "__main__":
    process_etl_config()