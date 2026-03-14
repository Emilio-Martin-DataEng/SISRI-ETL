# src/staging/source_import.py

import csv
from pathlib import Path
import pandas as pd
from datetime import datetime

from src.config import PROJECT_ROOT, get_config, get_db_config, get_logs_dir

SYSTEM_BASE_PATH = lambda: PROJECT_ROOT
from src.utils.db import upload_via_bcp
from src.utils.db_ops import  get_connection, execute_proc, truncate_table, generate_bcp_format_file, get_source_import_sk, insert_source_file_archive
from src.utils.logging_config import setup_logging
from src.utils.rejected_rows import RejectedRowsHandler


def get_source_pk_columns(source_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT Target_Column 
        FROM [ETL].[Dim_Source_Imports_Mapping] 
        WHERE Source_Name = ? AND Is_PK = 1
        ORDER BY File_Mapping_SK
    """, source_name)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [row[0] for row in rows]

def process_source(source_name: str, force_ddl: bool = False, audit_id: int = None):
    # Use the same config folder as etl_config.py
    config_folder = PROJECT_ROOT / get_config("base", "config_folder")
    format_dir = config_folder / "format" / "sources"
    temp_dir = PROJECT_ROOT / "temp"
    logs_dir = PROJECT_ROOT / get_config("logs", "rel_path")

    format_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    logger = setup_logging("source_import")
    logger.info(f"Processing source: {source_name}")
    
    # Initialize rejected rows handler
    rejected_handler = RejectedRowsHandler(source_name, audit_id)

    # === Get absolute root for RAW DATA from YAML ===
    raw_root = get_config("base", "file_root")
    logger.debug(f"Using data root from config: {raw_root}")

    # === Get source metadata ===
    source_config_path = PROJECT_ROOT / get_config("base", "config_folder") / get_config("base", "config_filename")
    try:
        source_config = pd.read_excel(source_config_path, sheet_name="Source_Imports", dtype=str)
    except Exception as e:
        raise ValueError(f"Failed to read Source_Imports sheet: {str(e)}")

    source_row = source_config[source_config['Source_Name'] == source_name]
    if source_row.empty:
        raise ValueError(f"No row in Source_Imports for '{source_name}'")
    source_row = source_row.iloc[0]

    rel_path = source_row['Rel_Path'].strip().lstrip('/\\')
    pattern = source_row['Pattern'] or "*.xlsx"
    sheet_name = source_row['Sheet_Name'] or "Sheet1"

    # === Build absolute data directory ===
    data_dir = Path(raw_root) / rel_path
    logger.info(f"Data directory: {data_dir}, Pattern: {pattern}, Sheet: {sheet_name}")

    # === Format file ===
    fmt_path = format_dir / f"{source_name.lower()}.fmt"
    if force_ddl or not fmt_path.exists():
        logger.info(f"{'Regenerating' if force_ddl else 'Generating'} format file for {source_name}...")
        generate_bcp_format_file(source_name, fmt_path)

    # === Find raw files ===
    files = list(data_dir.glob(pattern))
    if not files:
        logger.warning(f"No files found in {data_dir} with pattern {pattern}")
        files = list(data_dir.glob("*.xlsx"))
        if not files:
            logger.error(f"No .xlsx files found in {data_dir} — check Rel_Path and file_root")
            return 0

    logger.info(f"Found {len(files)} files to process")

    total_rows = 0
    rejected_count = 0

    pk_cols = get_source_pk_columns(source_name)

    for file_path in files:
        logger.info(f"Processing file: {file_path.name}")
        
        try:
            logger.debug(f"Reading sheet '{sheet_name}' from {file_path.name}")
            df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str, engine='openpyxl')
            logger.debug(f"Excel read successful: {len(df)} rows, columns: {list(df.columns)}")

            # Get column mapping from Source_Column to Target_Column
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT Source_Column, Target_Column
                FROM ETL.Dim_Source_Imports_Mapping
                WHERE Source_Name = ? AND Is_Deleted = 0
                ORDER BY File_Mapping_SK
            """, source_name)
            column_mappings = dict(cursor.fetchall())
            cursor.close()
            conn.close()
            
            # Rename columns from Source_Column to Target_Column
            if column_mappings:
                # Only keep columns that are in the mapping
                expected_source_columns = list(column_mappings.keys())
                available_columns = [col for col in expected_source_columns if col in df.columns]
                
                logger.debug(f"Excel columns: {list(df.columns)}")
                logger.debug(f"Expected source columns: {expected_source_columns}")
                logger.debug(f"Available columns: {available_columns}")
                
                if len(available_columns) != len(expected_source_columns):
                    missing_cols = set(expected_source_columns) - set(available_columns)
                    logger.warning(f"Missing expected columns: {missing_cols}")
                
                # Filter to only expected columns, then rename
                df = df[available_columns]
                df = df.rename(columns=column_mappings)
                logger.debug(f"Filtered and renamed columns: {available_columns} -> {list(column_mappings.values())}")
                logger.debug(f"Final DataFrame shape: {df.shape}")
            
            # Sanitize after column renaming
            df = df.apply(lambda x: x.astype(str).str.replace(r'[\n\r\t\\]', ' ', regex=True).str.strip())
            df = df.apply(lambda x: x.str.replace(r'\s+', ' ', regex=True).str.strip())

            if pk_cols:
                rename_map = {}
                for raw_col in df.columns:
                    clean_col = raw_col.replace(' ', '_').strip()
                    if clean_col in pk_cols:
                        rename_map[raw_col] = clean_col
                
                if rename_map:
                    df = df.rename(columns=rename_map)
                    logger.debug(f"Auto-renamed columns for dedup: {rename_map}")
            
            # Enhanced duplicate detection and handling
            missing_pk = [col for col in pk_cols if col not in df.columns]
            if missing_pk:
                logger.warning(f"Missing PK columns after rename: {missing_pk}")
            else:
                # Identify duplicates before removing them
                duplicates_mask = df.duplicated(subset=pk_cols, keep=False)
                duplicates_df = df[duplicates_mask].copy()
                
                if not duplicates_df.empty:
                    # Log all duplicate records (including ones we'll keep)
                    rejected_handler.log_duplicate_rows(file_path.name, duplicates_df, pk_cols)
                    
                    # Keep only first occurrence (Kimball best practice)
                    df_clean = df.drop_duplicates(subset=pk_cols, keep='first')
                    dup_count = len(df) - len(df_clean)
                    logger.info(f"Identified {len(duplicates_df)} duplicate records, keeping {len(df_clean)} unique records")
                    df = df_clean

            # Add audit system columns for dimensions
            df['Inserted_Datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['Audit_Source_Import_SK'] = str(audit_id if audit_id else 0)  # Convert to string for BCP
            df['Source_File_Archive_SK'] = '0'  # Convert to string for BCP
            logger.debug(f"Added audit columns: Audit_Source_Import_SK={audit_id}, Source_File_Archive_SK=0")

            temp_flat = temp_dir / f"{source_name}_{file_path.stem}_cleaned.txt"
            df.to_csv(
                temp_flat,
                sep='\t',
                index=False,
                header=False,
                encoding='utf-8',
                lineterminator='\r\n',
                quoting=csv.QUOTE_NONE,
                escapechar='\\',
                na_rep=''
            )
            bcp_log = logs_dir / f"bcp_errors_{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            truncate_table(f"ODS.{source_name}")
            upload_via_bcp(temp_flat, f"ODS.{source_name}", get_db_config(), str(fmt_path), 1)

            # Process this file individually - create archive record and merge
            try:
                # Insert file archive record
                archive_sk = insert_source_file_archive(
                    audit_id=audit_id if audit_id else 0,
                    source_import_sk=get_source_import_sk(source_name),
                    original_file_name=file_path.name,
                    archive_file_name=file_path.name,  # Could be enhanced with actual archive path
                    archive_full_path=str(file_path),
                    file_row_count=len(df),
                    process_status='Success'
                )
                
                # Update ODS table with archive SK
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute(f"""
                    UPDATE [ODS].[{source_name}] 
                    SET Source_File_Archive_SK = ? 
                    WHERE Source_File_Archive_SK = 0
                """, archive_sk)
                conn.commit()
                cursor.close()
                conn.close()
                
                logger.info(f"Updated {len(df)} rows in ODS.{source_name} with Source_File_Archive_SK={archive_sk}")
                
                # Call merge procedure for this file
                # Get merge procedure name for this source
                conn = get_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT Merge_Proc_Name FROM [ETL].[Dim_Source_Imports] WHERE Source_Name = ?", source_name)
                merge_proc_row = cursor.fetchone()
                merge_proc_name = merge_proc_row[0] if merge_proc_row and merge_proc_row[0] else f"ETL.SP_Merge_Dim_{source_name}"
                cursor.close()
                conn.close()
                
                execute_proc(
                    merge_proc_name,
                    params_dict={
                        "@Audit_Source_Import_SK": audit_id,
                        "@Source_File_Archive_SK": archive_sk,
                    },
                )
                logger.info(f"Merged {source_name} using {merge_proc_name} with Audit_SK={audit_id}, Archive_SK={archive_sk}")
                
            except Exception as e:
                logger.error(f"Failed to create file archive or update ODS table: {e}")

            if bcp_log.exists() and bcp_log.stat().st_size > 0:
                with open(bcp_log, 'r') as f:
                    log_content = f.read()
                    logger.warning(f"BCP rejects for {file_path.name}: {len(log_content)} chars")
                    
                    # Use rejected handler for BCP errors
                    rejected_handler.log_bcp_rejected_rows(file_path.name, log_content)
                    rejected_count += 1

            total_rows += len(df)

        except Exception as e:
            logger.error(f"Error processing {file_path.name}: {str(e)}")
            import traceback
            full_tb = traceback.format_exc()
            logger.debug(f"Full traceback:\n{full_tb}")
            
            # Log processing error using rejected handler
            raw_data = {
                "error": str(e),
                "traceback": full_tb,
                "file_name": file_path.name
            }
            rejected_handler.log_rejected_row(file_path.name, 0, f"PROCESSING_ERROR: {str(e)[:200]}", raw_data)

    # Get final rejected count from database
    final_rejected_count = rejected_handler.get_rejected_count()
    logger.info(f"Completed {source_name}: {total_rows} rows loaded, {final_rejected_count} rows rejected")
    return total_rows
