# src/staging/etl_config.py

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

from pathlib import Path
import pandas as pd
import csv
from datetime import datetime

from src.config import PROJECT_ROOT, get_config, get_db_config, get_logs_dir

SYSTEM_BASE_PATH = lambda: PROJECT_ROOT
from src.utils.db import upload_via_bcp
from src.utils.db_ops import (
    truncate_table,
    execute_proc,
    log_audit_source_import,
    get_next_audit_import_id,
    get_source_import_sk,
    get_connection
)
from src.utils.ddl_generator import (
    apply_ddl_from_run,
    generate_ods_table_ddl,
    generate_dw_table_ddl,
    generate_merge_proc_ddl
)
from src.utils.logging_config import setup_logging

CONFIG_ROOT = Path(get_config("base", "config_folder", default="config"))

def process_etl_config(force_ddl: bool = False):
    logger = setup_logging("etl_config")
    start_time = datetime.now()
    config_source_sk = get_source_import_sk('Source_Imports')
    if config_source_sk == 0:
        logger.warning("No Source_Import_SK for 'Source_Imports' - using 0")

    audit_id = get_next_audit_import_id()

    config_folder = get_config("base", "config_folder")
    if config_folder is None:
        config_folder = "config"
        print("[WARNING] 'config_folder' not found in config - using default 'config'")
    
    CONFIG_ROOT = Path(config_folder)

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
    logger.info(f"Started audit entry: Audit_Source_Import_SK = {audit_id} (linked to Source_Import_SK = {config_source_sk})")

    try:
        config_filename = get_config("base", "config_filename")
        config_files = list(CONFIG_ROOT.glob(config_filename))

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
            logger.warning(f"Config spreadsheet not found: {config_folder / config_filename}")
            return

        config_file = config_files[0]
        logger.info(f"Loading config from: {config_file}")

        format_dir = CONFIG_ROOT / "format"
        format_system_dir = format_dir / "system"

        format_imports = format_system_dir / "source_imports.fmt"
        format_mapping = format_system_dir / "source_file_mapping.fmt"
        format_conformed = format_system_dir / "fact_conformed_mapping.fmt"  # NEW

        format_dir.mkdir(parents=True, exist_ok=True)
        format_system_dir.mkdir(parents=True, exist_ok=True)

        # Load existing sheets
        df_imports = pd.read_excel(config_file, sheet_name="Source_Imports", dtype=str)
        df_mapping = pd.read_excel(config_file, sheet_name="Source_File_Mapping", dtype=str)
        
        # NEW: Load the Fact_Conformed_Mapping sheet
        try:
            df_conformed_mapping = pd.read_excel(config_file, sheet_name="Fact_Conformed_Mapping", dtype=str)
            logger.info(f"Loaded Fact_Conformed_Mapping sheet with {len(df_conformed_mapping)} rows")
        except ValueError:
            df_conformed_mapping = pd.DataFrame()  # empty if sheet missing
            logger.warning("Fact_Conformed_Mapping sheet not found - skipping load")

        # Aggressive trimming (same as before)
        for df in [df_imports, df_mapping, df_conformed_mapping]:
            string_cols = df.select_dtypes(include=['object']).columns
            df[string_cols] = df[string_cols].apply(
                lambda x: x.str.strip().replace(r'[\s\xa0\t\r\n]+', ' ', regex=True).str.strip()
            )

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_imports['Inserted_Datetime'] = now_str
        df_mapping['Inserted_Datetime'] = now_str
        if not df_conformed_mapping.empty:
            df_conformed_mapping['Inserted_Datetime'] = now_str

        temp_dir = PROJECT_ROOT / "temp"
        temp_dir.mkdir(exist_ok=True)

        imports_path = temp_dir / "source_imports_stg.txt"
        mapping_path = temp_dir / "source_file_mapping_stg.txt"
        conformed_path = temp_dir / "fact_conformed_mapping_stg.txt"  # NEW

        # Write temp files
        df_imports.to_csv(imports_path, sep='\t', index=False, header=False, encoding='utf-8',
                          lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')
        df_mapping.to_csv(mapping_path, sep='\t', index=False, header=False, encoding='utf-8',
                          lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')

        if not df_conformed_mapping.empty:
            # Select known columns for conformed mapping (adjust as needed)
            known_conformed_cols = [
                'Source_Name', 'ODS_Source_Name', 'ODS_Column', 'Conformed_Column',
                'Transformation_Type', 'Transformation_Rule', 'Sequence_Order',
                'Is_Key', 'Is_Required', 'Data_Type', 'Validation_Rule', 'Description',
                'Inserted_Datetime'
            ]
            df_conformed_mapping = df_conformed_mapping[[c for c in known_conformed_cols if c in df_conformed_mapping.columns]].fillna('')
            df_conformed_mapping.to_csv(conformed_path, sep='\t', index=False, header=False, encoding='utf-8',
                                        lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')

        db_cfg = get_db_config()

        # Truncate existing tables
        truncate_table('ETL.Source_Imports')
        truncate_table('ETL.Source_File_Mapping')
        truncate_table('ETL.Fact_Conformed_Mapping')  # NEW

        # Upload via BCP
        upload_via_bcp(imports_path, 'ETL.Source_Imports', db_cfg, str(format_imports), 1)
        upload_via_bcp(mapping_path, 'ETL.Source_File_Mapping', db_cfg, str(format_mapping), 1)

        if conformed_path.exists() and conformed_path.stat().st_size > 0:
            upload_via_bcp(conformed_path, 'ETL.Fact_Conformed_Mapping', db_cfg, str(format_conformed), 1)

        # Merge procs for metadata
        execute_proc('ETL.SP_Merge_Dim_Source_Imports')
        execute_proc('ETL.SP_Merge_Dim_Source_Imports_Mapping')
        execute_proc('ETL.SP_Merge_Dim_Fact_Conformed_Mapping')  # NEW - create this proc if needed

        # Existing DDL generation for dimensions
        active_data_sources = df_imports[
            (df_imports['Is_Active'] == '1') & 
            (~df_imports['Source_Name'].isin(['Source_Imports', 'Source_File_Mapping', 'Fact_Sales_Conformed']))
        ]['Source_Name'].unique()

        generated_dir = PROJECT_ROOT / get_config("dw_ddl", "base_folder") / get_config("dw_ddl", "generated_folder")
        generated_dir.mkdir(parents=True, exist_ok=True)

        conn = get_connection()
        cursor = conn.cursor()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        for source in active_data_sources:
            cursor.execute("SELECT Force_DDL_Generation FROM [ETL].[Dim_Source_Imports] WHERE Source_Name = ?", source)
            force_row = cursor.fetchone()
            force_this = force_ddl or (force_row and force_row[0])

            cursor.execute("EXEC [ETL].[SP_Get_Source_Imports_Last_Checked] ?", source)
            last_checked_row = cursor.fetchone()
            last_checked = last_checked_row[0] if last_checked_row else None

            cursor.execute("""
                SELECT MAX(Inserted_Datetime) 
                FROM [ETL].[Dim_Source_Imports_Mapping] 
                WHERE Source_Name = ?
            """, source)
            max_mapping_change_row = cursor.fetchone()
            max_mapping_change = max_mapping_change_row[0] if max_mapping_change_row else None

            if force_this or (last_checked is None or (max_mapping_change and max_mapping_change > last_checked)):
                logger.info(f"Generating DDL for {source}")

                source_mapping = df_mapping[df_mapping['Source_Name'] == source].to_dict('records')

                cursor.execute("SELECT Staging_Table, DW_Table_Name FROM [ETL].[Dim_Source_Imports] WHERE Source_Name = ?", source)
                row = cursor.fetchone()
                staging_table, dw_table = row if row else (None, None)

                if staging_table and dw_table:
                    ods_ddl = generate_ods_table_ddl(source, source_mapping)
                    (generated_dir / f"ODS_{source}.sql").write_text(ods_ddl)

                    dw_ddl = generate_dw_table_ddl('DW', f"Dim_{source}", source_mapping, timestamp)
                    (generated_dir / f"DW_Dim_{source}.sql").write_text(dw_ddl)

                    merge_ddl = generate_merge_proc_ddl(source, staging_table, dw_table, source_mapping)
                    (generated_dir / f"SP_Merge_Dim_{source}.sql").write_text(merge_ddl)

                execute_proc('ETL.SP_Update_Source_Imports_Last_Checked', f"@SourceName = '{source}', @LastChecked = '{now_str}'")
            else:
                logger.debug(f"Skipping DDL generation for {source} - no change")

        cursor.close()
        conn.close()

        apply_ddl_from_run()

        end_time = datetime.now()
        row_count = len(df_imports) + len(df_mapping) + len(df_conformed_mapping)
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
        logger.info("ETL configuration loaded and merged successfully.")

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
        logs_dir = PROJECT_ROOT / get_config("logs", "rel_path")
        logs_dir.mkdir(parents=True, exist_ok=True)
        error_log_path = logs_dir / f"etl_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(error_log_path, "w", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: {str(e)}\n")
        logger.error(f"Error details saved to: {error_log_path}")
        raise

if __name__ == "__main__":
    process_etl_config()