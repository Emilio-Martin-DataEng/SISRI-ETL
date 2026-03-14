# src/staging/etl_config.py
"""Admin config loader: Excel → metadata, optional DDL, first load. Used rarely (new source take-on)."""

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
    create_run,
    create_audit_source_import,
    update_run,
    get_source_import_sk,
    get_connection
)
from src.dw.ddl_generator import (
    apply_ddl_from_run,
    generate_ods_table_ddl,
    generate_dw_table_ddl,
    generate_merge_proc_ddl,
    generate_fact_to_conformed_merge_ddl,
)
from src.utils.logging_config import setup_logging
from src.utils.check_mapping import run_check_mapping

CONFIG_ROOT = Path(get_config("base", "config_folder", default="config"))

def _get_config_source_import_sk(table_name: str, fallback_sk: int) -> int:
    """Get Source_Import_SK for config table; fallback if not in Dim_Source_Imports."""
    sk = get_source_import_sk(table_name)
    return sk if sk else fallback_sk


def process_etl_config(force_ddl: bool = False, source: str | None = None, run_first_load: bool = True, show_report_after: bool = True):
    logger = setup_logging("etl_config")
    start_time = datetime.now()

    config_folder = get_config("base", "config_folder")
    if config_folder is None:
        config_folder = "config"
        logger.warning("'config_folder' not found in config - using default 'config'")

    CONFIG_ROOT = Path(config_folder)

    # Run model: one Run_SK, one Audit_Source_Import_SK per config source
    run_sk = create_run(start_time=start_time, run_type='Config')
    config_fallback_sk = get_source_import_sk('Source_Imports') or 0

    sk_imports = _get_config_source_import_sk('Source_Imports', config_fallback_sk)
    sk_mapping = _get_config_source_import_sk('Source_File_Mapping', config_fallback_sk)
    sk_conformed = _get_config_source_import_sk('DW_Mapping_And_Transformations', config_fallback_sk)

    audit_id_imports = create_audit_source_import(run_sk, sk_imports, start_time)
    audit_id_mapping = create_audit_source_import(run_sk, sk_mapping, start_time)
    audit_id_conformed = create_audit_source_import(run_sk, sk_conformed, start_time)

    logger.info(f"Config run started: Run_SK = {run_sk}, Audit IDs: imports={audit_id_imports}, mapping={audit_id_mapping}, conformed={audit_id_conformed}")

    try:
        config_filename = get_config("base", "config_filename")
        config_files = list(CONFIG_ROOT.glob(config_filename))

        if not config_files:
            end_time = datetime.now()
            for aid, sk in [(audit_id_imports, sk_imports), (audit_id_mapping, sk_mapping), (audit_id_conformed, sk_conformed)]:
                log_audit_source_import(aid, source_import_sk=sk, start_time=start_time, end_time=end_time,
                    total_row_count=0, total_file_count=0, exception_detail="Config spreadsheet not found",
                    pattern=None, process_status='Skipped')
            update_run(run_sk, end_time=end_time, process_status='Skipped', exception_detail="Config spreadsheet not found")
            logger.warning(f"Config spreadsheet not found: {config_folder / config_filename}")
            return

        config_file = config_files[0]
        logger.info(f"Loading config from: {config_file}")

        format_dir = CONFIG_ROOT / "format"
        format_system_dir = format_dir / "system"

        format_imports = format_system_dir / "source_imports.fmt"
        format_mapping = format_system_dir / "source_file_mapping.fmt"
        format_conformed = format_system_dir / "dw_mapping_and_transformations.fmt"

        format_dir.mkdir(parents=True, exist_ok=True)
        format_system_dir.mkdir(parents=True, exist_ok=True)

        # Load sheets
        df_imports = pd.read_excel(config_file, sheet_name="Source_Imports", dtype=str)
        df_mapping = pd.read_excel(config_file, sheet_name="Source_File_Mapping", dtype=str)

        try:
            df_conformed_mapping = pd.read_excel(config_file, sheet_name="DW_Mapping_And_Transformations", dtype=str)
            logger.info(f"Loaded DW_Mapping_And_Transformations sheet with {len(df_conformed_mapping)} rows")
        except ValueError:
            df_conformed_mapping = pd.DataFrame()
            logger.warning("DW_Mapping_And_Transformations sheet not found - skipping load")

        # Trim strings
        for df in [df_imports, df_mapping, df_conformed_mapping]:
            if df.empty:
                continue
            string_cols = df.select_dtypes(include=['object', 'string']).columns
            if not string_cols.empty:
                df[string_cols] = df[string_cols].apply(
                    lambda x: x.str.strip().replace(r'[\s\xa0\t\r\n]+', ' ', regex=True).str.strip()
                )

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_imports['Inserted_Datetime'] = now_str
        df_mapping['Inserted_Datetime'] = now_str
        if not df_conformed_mapping.empty:
            df_conformed_mapping['Inserted_Datetime'] = now_str

        # Lineage: each config table gets its own audit_id; Source_File_Archive_SK = -1 (no file)
        df_imports['Audit_Source_Import_SK'] = audit_id_imports
        df_imports['Source_File_Archive_SK'] = -1
        df_mapping['Audit_Source_Import_SK'] = audit_id_mapping
        df_mapping['Source_File_Archive_SK'] = -1
        if not df_conformed_mapping.empty:
            df_conformed_mapping['Audit_Source_Import_SK'] = audit_id_conformed
            df_conformed_mapping['Source_File_Archive_SK'] = -1

        # Source_Imports: enforce column order (Force_DDL_Generation removed; use --force-ddl)
        source_imports_columns = [
            'Source_Name', 'Rel_Path', 'Pattern', 'Sheet_Name', 'Staging_Table',
            'Processing_Order', 'Is_Active', 'Description', 'DW_Table_Name', 'Merge_Proc_Name',
            'Source_Type', 'Is_Conformed_Target', 'Wholesaler_Code', 'Inserted_Datetime',
            'Audit_Source_Import_SK', 'Source_File_Archive_SK'
        ]
        df_imports = df_imports.reindex(columns=source_imports_columns, fill_value='')

        temp_dir = PROJECT_ROOT / "temp"
        temp_dir.mkdir(exist_ok=True)

        imports_path = temp_dir / "source_imports_stg.txt"
        mapping_path = temp_dir / "source_file_mapping_stg.txt"
        conformed_path = temp_dir / "dw_mapping_and_transformations_stg.txt"

        df_imports.to_csv(imports_path, sep='\t', index=False, header=False, encoding='utf-8',
                          lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')

        # Fix Source_File_Mapping to include only columns that exist in Excel and match source table
        required_columns = [
            'Source_Name', 'Source_Column', 'Target_Column',
            'Data_Type', 'Ordinal_Position', 'Description', 'Is_Type2_Attribute', 'Is_PK', 'Is_Required',
            'Inserted_Datetime', 'Audit_Source_Import_SK', 'Source_File_Archive_SK'
        ]
        
        df_mapping = df_mapping.reindex(columns=required_columns, fill_value='')  # Fill missing with empty string
        # Set proper defaults for specific columns
        if 'Inserted_Datetime' not in df_mapping.columns or df_mapping['Inserted_Datetime'].isna().all():
            df_mapping['Inserted_Datetime'] = now_str

        # Regenerate the CSV with all columns
        df_mapping.to_csv(mapping_path, sep='\t', index=False, header=False, encoding='utf-8',
                          lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')

        if not df_conformed_mapping.empty:
            known_conformed_cols = [
                'Source_Name', 'ODS_Column', 'Conformed_Column',
                'Transformation_Type', 'Transformation_Rule',
                'Is_Key', 'Is_Required', 'Default_Value',
                'Validation_Rule', 'Sequence_Order', 'Description', 'Inserted_Datetime',
                'Audit_Source_Import_SK', 'Source_File_Archive_SK'
            ]
            df_conformed_mapping = df_conformed_mapping.reindex(columns=known_conformed_cols).fillna('')
            df_conformed_mapping.to_csv(conformed_path, sep='\t', index=False, header=False, encoding='utf-8',
                                        lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')

        db_cfg = get_db_config()

        truncate_table('ETL.Source_Imports')
        truncate_table('ETL.Source_File_Mapping')
        truncate_table('ETL.DW_Mapping_And_Transformations')

        upload_via_bcp(imports_path, 'ETL.Source_Imports', db_cfg, str(format_imports), 1)
        upload_via_bcp(mapping_path, 'ETL.Source_File_Mapping', db_cfg, str(format_mapping), 1)

        if conformed_path.exists() and conformed_path.stat().st_size > 0:
            upload_via_bcp(conformed_path, 'ETL.DW_Mapping_And_Transformations', db_cfg, str(format_conformed), 1)

        execute_proc('ETL.SP_Merge_Dim_Source_Imports')
        execute_proc('ETL.SP_Merge_Dim_Source_Imports_Mapping')
        execute_proc('ETL.SP_Merge_Dim_DW_Mapping_And_Transformations')

        # Validate mapping metadata after Dim tables loaded
        success, issues = run_check_mapping()
        if not success:
            for msg in issues:
                logger.error(msg)
            raise RuntimeError(
                f"Check mapping failed ({len(issues)} issue(s)). "
                "Admin must correct ETL_Config.xlsx and re-run metadata refresh."
            )
        logger.info("Check mapping: OK")

        # Get all active sources (including Fact_Sales)
        active_sources = df_imports[
            (df_imports['Is_Active'] == '1')
        ]['Source_Name'].unique()

        generated_dir = PROJECT_ROOT / get_config("dw_ddl", "base_folder") / get_config("dw_ddl", "generated_folder")
        generated_dir.mkdir(parents=True, exist_ok=True)
        
        # Clean up old generated files to prevent confusion
        for old_file in generated_dir.glob("*.sql"):
            old_file.unlink()
            logger.info(f"Cleaned up old generated file: {old_file.name}")

        conn = get_connection()
        cursor = conn.cursor()

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        if source and source not in df_imports['Source_Name'].values:
            raise ValueError(f"Source '{source}' not found in config. Check Source_Imports sheet.")
        sources_to_process = [source] if source else list(active_sources)
        for src in sources_to_process:
            cursor.execute("""
                SELECT Source_Type, Staging_Table, DW_Table_Name, DW_Table_Name, Merge_Proc_Name
                FROM [ETL].[Dim_Source_Imports]
                WHERE Source_Name = ?
            """, src)
            row = cursor.fetchone()
            if not row:
                logger.warning(f"No config found for source {src}")
                continue

            source_type, staging_table, dw_table, conformed_target, conformed_merge_proc = row
            force_this = force_ddl  # DDL only when --force-ddl passed (Force_DDL_Generation removed from config)

            if source_type == 'Dimension':
                # Existing dimension generation
                source_mapping = df_mapping[df_mapping['Source_Name'] == src].to_dict('records')
                if staging_table and dw_table:
                    if force_this:
                        logger.info(f"Generating DDL for dimension {src}")
                        ods_ddl = generate_ods_table_ddl(src, source_mapping)
                        (generated_dir / f"ODS_{src}.sql").write_text(ods_ddl)

                        dw_ddl = generate_dw_table_ddl('DW', f"Dim_{src}", source_mapping, timestamp)
                        (generated_dir / f"DW_Dim_{src}.sql").write_text(dw_ddl)

                        merge_ddl = generate_merge_proc_ddl(src, staging_table, dw_table, source_mapping)
                        (generated_dir / f"SP_Merge_Dim_{src}.sql").write_text(merge_ddl)

            elif source_type == 'Fact_Sales':
                # Generate ODS table DDL and conformed merge proc
                source_mapping = df_mapping[df_mapping['Source_Name'] == src].to_dict('records')
                
                # Generate ODS table DDL (same pattern as dimensions) - move to run folder for auto-execution
                if staging_table and force_this:
                    logger.info(f"Generating ODS DDL for Fact_Sales source {src}")
                    ods_ddl = generate_ods_table_ddl(src, source_mapping)
                    run_dir = PROJECT_ROOT / get_config("dw_ddl", "base_folder") / get_config("dw_ddl", "run_folder")
                    run_dir.mkdir(parents=True, exist_ok=True)
                    (run_dir / f"ODS_{src}.sql").write_text(ods_ddl)
                    logger.info(f"Generated and moved to run folder: ODS_{src}.sql")
                
                # Generate conformed merge proc
                if conformed_target and conformed_merge_proc and conformed_target.strip():
                    source_mapping = df_conformed_mapping[df_conformed_mapping['Source_Name'] == src].to_dict('records')
                    if source_mapping and force_this:
                        logger.info(f"Generating conformed merge proc for {src} -> {conformed_target}")
                        merge_ddl = generate_fact_to_conformed_merge_ddl(
                            source_name=src,
                            ods_table=staging_table,
                            conformed_table=conformed_target,
                            mapping_rows=source_mapping
                        )
                        proc_filename = conformed_merge_proc.replace('[ETL].[', '').replace(']', '') + '.sql'
                        # Move conformed target procedures to run folder for manual review
                        run_dir = PROJECT_ROOT / get_config("dw_ddl", "base_folder") / get_config("dw_ddl", "run_folder")
                        run_dir.mkdir(parents=True, exist_ok=True)
                        proc_file = run_dir / proc_filename
                        proc_file.write_text(merge_ddl)
                        logger.info(f"Generated and moved to run folder: {proc_filename}")

            # Common checkpoint update
            execute_proc(
                "ETL.SP_Update_Source_Imports_Last_Checked",
                params_dict={"@SourceName": src, "@LastChecked": now_str},
            )

        cursor.close()
        conn.close()

        apply_ddl_from_run()

        # First load: run data processing to verify config (admin can inspect errors)
        if run_first_load:
            logger.info("Running first load to verify config...")
            from src.etl_orchestrator import run_etl
            run_etl(sources=None, force_ddl=force_ddl)

        end_time = datetime.now()
        log_audit_source_import(audit_id_imports, source_import_sk=sk_imports, start_time=start_time, end_time=end_time,
            total_row_count=len(df_imports), total_file_count=1, process_status='Success', pattern=config_filename)
        log_audit_source_import(audit_id_mapping, source_import_sk=sk_mapping, start_time=start_time, end_time=end_time,
            total_row_count=len(df_mapping), total_file_count=1, process_status='Success', pattern=config_filename)
        log_audit_source_import(audit_id_conformed, source_import_sk=sk_conformed, start_time=start_time, end_time=end_time,
            total_row_count=len(df_conformed_mapping), total_file_count=1, process_status='Success', pattern=config_filename)
        update_run(run_sk, end_time=end_time, process_status='Success',
            total_sources_processed=3, total_rows_processed=len(df_imports) + len(df_mapping) + len(df_conformed_mapping))
        logger.info("ETL configuration loaded and merged successfully.")

        if show_report_after:
            try:
                from tools.etl_run_report import show_report
                show_report(run_sk=run_sk)
            except Exception as rep_err:
                logger.warning(f"Could not open ETL report: {rep_err}")

    except Exception as e:
        end_time = datetime.now()
        for aid, sk in [(audit_id_imports, sk_imports), (audit_id_mapping, sk_mapping), (audit_id_conformed, sk_conformed)]:
            log_audit_source_import(aid, source_import_sk=sk, start_time=start_time, end_time=end_time,
                total_row_count=0, total_file_count=0, exception_detail=str(e), process_status='Failed')
        update_run(run_sk, end_time=end_time, process_status='Failed', exception_detail=str(e))
        logs_dir = PROJECT_ROOT / get_config("logs", "rel_path")
        logs_dir.mkdir(parents=True, exist_ok=True)
        error_log_path = logs_dir / f"etl_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        with open(error_log_path, "w", encoding="utf-8") as f:
            f.write(f"{datetime.now()}: {str(e)}\n")
        logger.error(f"Error details saved to: {error_log_path}")
        raise

if __name__ == "__main__":
    # Prefer: python -m src.admin.load_config (CLI with --force-ddl, --source)
    process_etl_config()