# src/staging/source_import.py (full file with fix)

from pathlib import Path
import pandas as pd
import csv
from datetime import datetime

from src.config import SYSTEM_BASE_PATH, get_config, get_db_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import get_connection, execute_proc

def generate_bcp_format_file(source_name: str, fmt_path: Path):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT Target_Column, Data_Type FROM [ETL].[Dim_Source_Imports_Mapping] WHERE Source_Name = ? ORDER BY File_Mapping_SK", source_name)
        rows = cursor.fetchall()
        if not rows:
            raise ValueError(f"No mapping columns found for source '{source_name}'")

        with open(fmt_path, 'w', encoding='utf-8') as f:
            f.write("14.0\n")
            f.write(f"{len(rows) + 1}\n")  # +1 for Inserted_Datetime

            for i, row in enumerate(rows, 1):
                target_col, data_type = row
                length = 8000
                terminator = "\t" if i < len(rows) else "\r\n"
                f.write(f"{i} SQLCHAR 0 {length} \"{terminator}\" {i} {target_col} SQL_Latin1_General_CP1_CI_AS\n")

            f.write(f"{len(rows)+1} SQLCHAR 0 30 \"\\r\\n\" {len(rows)+1} Inserted_Datetime SQL_Latin1_General_CP1_CI_AS\n")

        print(f"[BCP] Generated format file: {fmt_path}")
    finally:
        cursor.close()
        conn.close()

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
    raw_folder = get_config("system", "raw_folder", "raw")
    format_subfolder = get_config("system", "format_subfolder", "config/format")
    temp_folder = get_config("system", "temp_folder", "temp")
    logs_folder = get_config("system", "logs_folder", "logs")

    raw_dir = SYSTEM_BASE_PATH() / raw_folder
    format_dir = SYSTEM_BASE_PATH() / format_subfolder
    temp_dir = SYSTEM_BASE_PATH() / temp_folder
    logs_dir = SYSTEM_BASE_PATH() / logs_folder
    rejected_dir = SYSTEM_BASE_PATH() / "rejected"
    rejected_dir.mkdir(exist_ok=True)

    format_dir.mkdir(exist_ok=True)
    temp_dir.mkdir(exist_ok=True)
    logs_dir.mkdir(exist_ok=True)

    # Get mapping and table names
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Staging_Table, DW_Table_Name FROM [ETL].[Dim_Source_Imports] WHERE Source_Name = ?", source_name)
    row = cursor.fetchone()
    staging_table, dw_table = row if row else (None, None)
    cursor.close()
    conn.close()

    if not staging_table:
        raise ValueError(f"No Staging_Table found for {source_name}")

    # Generate/re-generate format if forced or missing
    fmt_path = format_dir / f"{source_name.lower()}.fmt"
    if not fmt_path.exists() or force_ddl:
        print(f"{'Regenerating' if force_ddl else 'Generating'} format file for {source_name}...")
        generate_bcp_format_file(source_name, fmt_path)

    # Find source files
    source_config = pd.read_excel(SYSTEM_BASE_PATH() / "config/ETL_Config.xlsx", sheet_name="Source_Imports")
    source_row = source_config[source_config['Source_Name'] == source_name].iloc[0]
    rel_path = source_row['Rel_Path']
    pattern = source_row['Pattern'] or "*.*"
    sheet_name = source_row['Sheet_Name'] or "Sheet1"

    files = list((SYSTEM_BASE_PATH() / rel_path).glob(pattern))
    if not files:
        print(f"No files found for {source_name} in {rel_path} with pattern {pattern}")
        return 0

    total_rows = 0
    rejected_count = 0

    rejected_file = rejected_dir / f"{source_name}_rejected.txt"
    with open(rejected_file, 'w', encoding='utf-8') as rf:
        rf.write(f"Rejected rows for {source_name} - {datetime.now()}\n\n")

    pk_cols = get_source_pk_columns(source_name)

    for file_path in files:
        print(f"[PROCESSING] {file_path.name}")

        try:
            df = pd.read_excel(file_path, sheet_name=sheet_name, dtype=str)

            # Sanitize all fields: remove linebreaks, backslashes, collapse spaces
            df = df.apply(lambda x: x.astype(str).str.replace(r'[\n\r\t\\]', ' ', regex=True).str.strip())
            df = df.apply(lambda x: x.str.replace(r'\s+', ' ', regex=True).str.strip())

            # Deduplicate PK: keep first occurrence
            if pk_cols:
                before = len(df)
                df = df.drop_duplicates(subset=pk_cols, keep='first')
                dup_count = before - len(df)
                if dup_count > 0:
                    print(f"[DEDUPLICATED] Removed {dup_count} duplicate PK rows in {file_path.name}")
                    # Log duplicates to rejected file
                    with open(rejected_file, 'a', encoding='utf-8') as rf:
                        rf.write(f"--- Duplicates removed from {file_path.name} ---\n")
                        rf.write(f"{dup_count} duplicates on PK {pk_cols}\n\n")

            # Add Inserted_Datetime
            df['Inserted_Datetime'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            # Save cleaned file for BCP
            temp_flat = temp_dir / f"{source_name}_{file_path.stem}_cleaned.txt"
            df.to_csv(temp_flat, sep='\t', index=False, header=False, encoding='utf-8', lineterminator='\r\n',
                      quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')

            # BCP load with error file
            bcp_log = logs_dir / f"bcp_errors_{source_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            upload_via_bcp(temp_flat, f"ODS.{source_name}", get_db_config(), str(fmt_path), 1, error_file=str(bcp_log))

            # Check BCP log for rejects
            if bcp_log.exists() and bcp_log.stat().st_size > 0:
                with open(bcp_log, 'r') as f:
                    log_content = f.read()
                    print(f"[REJECTED] BCP log for {file_path.name}:\n{log_content}")
                    # Append to rejected file
                    with open(rejected_file, 'a', encoding='utf-8') as rf:
                        rf.write(f"--- BCP Rejects from {file_path.name} ---\n{log_content}\n\n")
                    # Log to DB
                    conn = get_connection()
                    cursor = conn.cursor()
                    cursor.execute("""
                        EXEC [ETL].[SP_Log_Rejected_Row]
                            @Audit_Source_Import_SK = ?,
                            @Source_Name = ?,
                            @File_Name = ?,
                            @Rejected_Reason = ?,
                            @Raw_Data = ?
                    """, audit_id, source_name, file_path.name, "BCP rejected rows", log_content[:4000])
                    conn.commit()
                    cursor.close()
                    conn.close()

                rejected_count += 1

            total_rows += len(df)

        except Exception as e:
            print(f"[ERROR] Processing file {file_path}: {str(e)}")
            # Log to rejected file
            with open(rejected_file, 'a', encoding='utf-8') as rf:
                rf.write(f"--- ERROR processing {file_path.name} ---\n{str(e)}\n\n")
            # Log to DB
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                EXEC [ETL].[SP_Log_Rejected_Row]
                    @Audit_Source_Import_SK = ?,
                    @Source_Name = ?,
                    @File_Name = ?,
                    @Rejected_Reason = ?,
                    @Raw_Data = ?
            """, audit_id, source_name, file_path.name, str(e)[:500], "File processing failed")
            conn.commit()
            cursor.close()
            conn.close()
            # Continue to next file

    print(f"[SUMMARY] {source_name}: {total_rows} rows loaded, {rejected_count} files with rejects")
    return total_rows