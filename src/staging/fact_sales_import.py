# src/staging/fact_sales_import.py

import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
import csv
import os
import shutil

from src.config import PROJECT_ROOT, get_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import generate_bcp_format_file, get_connection, truncate_table
from src.utils.logging_config import setup_logging
from src.utils.rejected_rows import RejectedRowsHandler

logger = logging.getLogger(__name__)

def process_fact_sales(source_name: str, force_ddl: bool = False, audit_id: int = None):
    logger.info(f"Starting Fact_Sales processing for source: {source_name}")
    start_time = datetime.now()

    # === Config & Paths (mirror source_import) ===
    config_folder = PROJECT_ROOT / get_config("base", "config_folder")
    format_dir = config_folder / "format" / "sources"
    temp_dir = PROJECT_ROOT / "temp"
    logs_dir = PROJECT_ROOT / get_config("logs", "rel_path")

    format_dir.mkdir(parents=True, exist_ok=True)
    temp_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    # Rejected rows handler (reuse same class)
    rejected_handler = RejectedRowsHandler(source_name, audit_id)

    # === Get source metadata ===
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT Rel_Path, Pattern, Sheet_Name, Staging_Table
        FROM [ETL].[Dim_Source_Imports] 
        WHERE Source_Name = ?
    """, (source_name,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row is None:
        logger.error(f"CRITICAL: No row found in [ETL].[Dim_Source_Imports] for Source_Name = '{source_name}'")
        logger.error("Check: Is_Active=1, Is_Deleted=0, correct Source_Name spelling/case")
        return 0

    rel_path, pattern, sheet_name, staging_table = row

    # Add debug so we see exactly what is fetched
    logger.debug(f"Config loaded for {source_name}:")
    logger.debug(f"  Rel_Path     = {rel_path}")
    logger.debug(f"  Pattern      = {pattern}")
    logger.debug(f"  Sheet_Name   = {sheet_name}")
    logger.debug(f"  Staging_Table = {staging_table}")

    if not rel_path or not staging_table:
        logger.error("Incomplete config: Rel_Path or Staging_Table is NULL/empty")
        return 0

 
    
    pattern = pattern or "*.xlsx"
    sheet_name = sheet_name or "Sheet1"
    staging_table = staging_table or f"ODS.{source_name}"  # fallback

    raw_root = get_config("base", "file_root")
    data_dir = Path(raw_root) / rel_path.strip().lstrip('/\\')

    # === Format file generation (mirror) ===
    fmt_path = format_dir / f"{source_name.lower()}.fmt"
    if force_ddl or not fmt_path.exists():
        logger.info(f"{'Regenerating' if force_ddl else 'Generating'} format file...")
        generate_bcp_format_file(source_name, fmt_path)

    # === Find files (mirror glob logic) ===
    files = list(data_dir.glob(pattern))
    if not files:
        logger.warning(f"No files matching {pattern} in {data_dir}")
        return 0

    logger.info(f"Found {len(files)} files for {source_name}")

    total_rows = 0

    for file_path in files:
        logger.info(f"Processing file: {file_path.name}")

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)
            logger.debug(f"Read {len(df)} rows from {file_path.name}")

            # Basic cleaning (same as source_import)
            df = df.apply(lambda x: x.astype(str).str.replace(r'[\n\r\t\\]', ' ', regex=True).str.strip())
            df = df.apply(lambda x: x.str.replace(r'\s+', ' ', regex=True).str.strip())

            df['SourceFileName'] = str(file_path.absolute())
            df['LoadTimestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logger.debug("Starting BCP prep for file: " + str(file_path))
            # Temp TXT for BCP
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
            logger.debug(f"Temp TXT created: {temp_flat} ({temp_flat.stat().st_size} bytes)")
            # truncate
            logger.debug(f"Truncating table: {staging_table}")
            truncate_table(staging_table)
            logger.debug("Truncate complete")

            # BCP upload
            logger.debug(f"Starting BCP upload to {staging_table} using fmt {fmt_path}")
            bcp_log = logs_dir / f"bcp_errors_{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            upload_via_bcp(temp_flat, staging_table, get_config("db"), str(fmt_path), 1)
            logger.debug("BCP upload complete")

            # Handle BCP rejects (same as source_import)
            if bcp_log.exists() and bcp_log.stat().st_size > 0:
                with open(bcp_log, 'r') as f:
                    log_content = f.read()
                logger.warning(f"BCP rejects for {file_path.name}")
                rejected_handler.log_bcp_rejected_rows(file_path.name, log_content)

            total_rows += len(df)

            # Archive file (simple move to archive subfolder)
            archive_dir = data_dir / "archive"
            archive_dir.mkdir(exist_ok=True)
            archive_path = archive_dir / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_path.name}"
            shutil.move(file_path, archive_path)
            logger.debug(f"Archived to {archive_path}")

        except Exception as e:
            logger.error(f"Error on {file_path.name}: {str(e)}")
            rejected_handler.log_rejected_row(file_path.name, 0, f"PROCESSING_ERROR: {str(e)[:200]}", {"error": str(e)})

    # Final audit logging (can mirror source_import's style)
    logger.info(f"Completed {source_name}: {total_rows} rows loaded")
    return total_rows