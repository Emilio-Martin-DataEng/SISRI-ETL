# src/staging/etl_config.py

from pathlib import Path
import pandas as pd
import csv
from datetime import datetime

from src.config import SYSTEM_BASE_PATH, get_config, get_db_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import (
    get_connection,
    truncate_table,
    execute_proc,
    log_audit_source_import,
    get_next_audit_import_id,
    get_source_import_sk
)
from src.utils.ddl_generator import generate_ods_table_ddl, generate_merge_proc_ddl, generate_dw_table_ddl, apply_ddl_from_run

def process_etl_config():
    start_time = datetime.now()
    config_source_sk = get_source_import_sk('Source_Imports')
    if config_source_sk == 0:
        print("[WARNING] No Source_Import_SK for 'Source_Imports' - using 0")

    audit_id = get_next_audit_import_id()

    log_audit_source_import(
        audit_id=audit_id,
        source_import_sk=config_source_sk,
        start_time=start_time,
        end_time=None,
        total_row_count=0,
        total_file_count=0,
        exception_detail=None,
        pattern=None,
        process_status='Running'
    )
    print(f"[AUDIT] Started audit entry: Audit_Source_Import_SK = {audit_id} (linked to Source_Import_SK = {config_source_sk})")

    error_count = 0

    try:
        config_folder = SYSTEM_BASE_PATH() / get_config("system", "config_folder", "config")
        config_filename = get_config("system", "config_filename", "ETL_Config.xlsx")
        config_files = list(config_folder.glob(config_filename))
        
        if not config_files:
            end_time = datetime.now()
            log_audit_source_import(
                audit_id=audit_id,
                source_import_sk=config_source_sk,
                start_time=start_time,
                end_time=end_time,
                total_row_count=0,
                total_file_count=0,
                exception_detail="Config spreadsheet not found",
                pattern=None,
                process_status='Skipped'
            )
            print(f"[SKIP] Config spreadsheet not found: {config_folder / config_filename}")
            return

        config_file = config_files[0]
        print(f"Loading config from: {config_file}")

        format_dir = config_folder / get_config("system", "format_subfolder", "format")
        format_imports = format_dir / "source_imports.fmt"
        format_mapping = format_dir / "source_file_mapping.fmt"
        format_dir.mkdir(exist_ok=True)

        df_imports = pd.read_excel(config_file, sheet_name="Source_Imports", dtype=str)
        df_mapping = pd.read_excel(config_file, sheet_name="Source_File_Mapping", dtype=str)

        df_imports = df_imports.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df_mapping = df_mapping.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
        df_imports['Inserted_Datetime'] = now_str
        df_mapping['Inserted_Datetime'] = now_str

        temp_dir = SYSTEM_BASE_PATH() / get_config("system", "temp_folder", "temp")
        temp_dir.mkdir(exist_ok=True)
        
        imports_path = temp_dir / "source_imports_stg.txt"
        mapping_path = temp_dir / "source_file_mapping_stg.txt"

        df_imports.to_csv(
            imports_path,
            sep='\t',
            index=False,
            header=False,
            encoding='utf-8',
            lineterminator='\r\n',
            quoting=csv.QUOTE_NONE,
            escapechar='\\',
            na_rep=''
        )

        df_mapping.to_csv(
            mapping_path,
            sep='\t',
            index=False,
            header=False,
            encoding='utf-8',
            lineterminator='\r\n',
            quoting=csv.QUOTE_NONE,
            escapechar='\\',
            na_rep=''
        )

        db_cfg = get_db_config()

        truncate_table('ETL.Source_Imports')
        truncate_table('ETL.Source_File_Mapping')

        upload_via_bcp(imports_path, 'ETL.Source_Imports', db_cfg, str(format_imports), 1)
        upload_via_bcp(mapping_path, 'ETL.Source_File_Mapping', db_cfg, str(format_mapping), 1)

        execute_proc('ETL.SP_Merge_Dim_Source_Imports')
        execute_proc('ETL.SP_Merge_Dim_Source_Imports_Mapping')

        # Generate DDL only for active data sources if mapping changed
        active_data_sources = df_imports[(df_imports['Is_Active'] == '1') & (~df_imports['Source_Name'].isin(['Source_Imports', 'Source_File_Mapping']))]['Source_Name'].unique()

        generated_dir = SYSTEM_BASE_PATH() / get_config("system", "ddl_generated_subfolder", "DW_DDL/generated")
        generated_dir.mkdir(parents=True, exist_ok=True)

        conn = get_connection()
        cursor = conn.cursor()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        for source in active_data_sources:
            # Get last checked time
            cursor.execute("EXEC [ETL].[SP_Get_Source_Imports_Last_Checked] ?", source)
            last_checked_row = cursor.fetchone()
            last_checked = last_checked_row[0] if last_checked_row else None

            # Get max mapping change time
            cursor.execute("""
                SELECT MAX(Inserted_Datetime) 
                FROM [ETL].[Dim_Source_Imports_Mapping] 
                WHERE Source_Name = ?
            """, source)
            max_mapping_change = cursor.fetchone()[0]

            if last_checked is None or (max_mapping_change and max_mapping_change > last_checked):
                print(f"[DDL] Mapping changed for {source} - generating DDL")
                
                source_mapping = df_mapping[df_mapping['Source_Name'] == source].to_dict('records')

                # Generate ODS DDL
                ods_ddl = generate_ods_table_ddl(source, source_mapping)
                (generated_dir / f"ODS_{source}.sql").write_text(ods_ddl)

                # Generate DW table DDL with backup + insert
                dw_ddl = generate_dw_table_ddl('DW', f"Dim_{source}", source_mapping, timestamp)
                (generated_dir / f"DW_Dim_{source}.sql").write_text(dw_ddl)
                print(f"[DDL] 3")
                # Generate merge proc DDL
                merge_ddl = generate_merge_proc_ddl(source, f"ODS.{source}", source_mapping)
                (generated_dir / f"SP_Merge_Dim_{source}.sql").write_text(merge_ddl)

                # Update last checked time
                execute_proc('ETL.SP_Update_Source_Imports_Last_Checked', f"@SourceName = '{source}', @LastChecked = '{now_str}'")

            else:
                print(f"[DDL] No mapping change for {source} - skipping generation")

        cursor.close()
        conn.close()

        end_time = datetime.now()
        row_count = len(df_imports) + len(df_mapping)
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=config_source_sk,
            start_time=start_time,
            end_time=end_time,
            total_row_count=row_count,
            total_file_count=1,
            exception_detail=None,
            pattern=config_filename,
            process_status='Success'
        )
        print("ETL configuration loaded and merged successfully.")

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

        logs_dir = SYSTEM_BASE_PATH() / get_config("system", "logs_folder", "logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        error_log_path = logs_dir / f"etl_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(error_log_path, "w", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: {str(e)}\n")
        print(f"Error details saved to: {error_log_path}")

        raise

if __name__ == "__module__":
    process_etl_config()