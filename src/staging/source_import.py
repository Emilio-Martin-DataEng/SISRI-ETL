# src/staging/source_import.py

from pathlib import Path
import pandas as pd
from datetime import datetime
import pyodbc
import csv

from src.config import BASE_PATH, get_db_config, get_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import get_next_audit_import_id, log_audit_source_import, truncate_table


def process_source(source_name: str):
    """
    Generic ETL processor for any dimension/source configured in the database.
    
    - One audit record per source (not per file)
    - Row_Count = total rows loaded for this source (across all its files)
    - Fetches real Source_Import_SK from dim table
    - Truncates staging table before load to prevent PK dupes
    """
    print(f"Starting ETL process for source: {source_name}")

    # === PER-SOURCE AUDIT: START ===
    start_time = datetime.now()
    audit_id = get_next_audit_import_id()
    log_audit_source_import(
        audit_id=audit_id,
        source_import_sk=0,  # will update with real SK later
        start_time=start_time,
        end_time=None,
        row_count=0,
        exception_detail=None
    )
    print(f"[AUDIT] Started processing {source_name} - Audit_ID = {audit_id}")

    total_rows = 0

    try:
        # === STEP 1: Fetch source and column mapping config ===
        db_cfg = get_db_config()
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={db_cfg['server']};"
            f"DATABASE={db_cfg['database']};"
            f"UID={db_cfg['username']};"
            f"PWD={db_cfg['password']}"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT Rel_Path, Pattern, Sheet_Name, Staging_Table
            FROM ETL.Dim_Source_Imports
            WHERE Source_Name = ? AND Is_Active = 1 AND Is_Deleted = 0
        """, source_name)
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No active configuration found for source: {source_name}")
        
        rel_path, pattern, sheet_name, table_name = row
        print(f"Config found: path={rel_path}, pattern={pattern}, sheet={sheet_name}, table={table_name}")

        cursor.execute("""
            SELECT Source_Column, Target_Column
            FROM ETL.Dim_Source_Imports_Mapping
            WHERE Source_Name = ? AND Is_Deleted = 0
        """, source_name)
        column_map = {r.Source_Column: r.Target_Column for r in cursor.fetchall()}

        # Fetch real Source_Import_SK for this source
        cursor.execute("""
            SELECT Source_Import_SK 
            FROM ETL.Dim_Source_Imports 
            WHERE Source_Name = ?
        """, source_name)
        sk_row = cursor.fetchone()
        real_sk = sk_row[0] if sk_row else 0
        print(f"[DEBUG] Real Source_Import_SK for {source_name}: {real_sk}")

        cursor.close()
        conn.close()

        # Update audit with real SK immediately
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=None,
            row_count=0,
            exception_detail=f"Processing {source_name}"
        )

        # === STEP 2: Find all matching files ===
        folder = BASE_PATH() / rel_path
        all_files = list(folder.glob(pattern))
        
        if not all_files:
            raise FileNotFoundError(f"No files found in {folder} matching '{pattern}'")

        print(f"Found {len(all_files)} file(s) to process.")

        # === STEP 3: Load and combine files ===
        dfs = []
        for file in all_files:
            try:
                df = pd.read_excel(file, sheet_name=sheet_name, dtype=str)
                dfs.append(df)
                print(f"  Loaded: {file.name} ({len(df)} rows)")
            except Exception as e:
                print(f"  Error reading {file.name}: {e}")
                continue

        if not dfs:
            raise ValueError("No valid data loaded from any files.")

        final_df = pd.concat(dfs, ignore_index=True)
        final_df = final_df.drop_duplicates()
        print(f"Combined {len(final_df)} rows after deduplication (across all files).")

        # === STEP 4: Apply column mapping ===
        if column_map:
            final_df = final_df.rename(columns=column_map)
            print(f"Applied column mapping: {len(column_map)} columns renamed.")

        # Safeguard
        if 'Inserted_Datetime' in final_df.columns:
            final_df = final_df.drop(columns=['Inserted_Datetime'])

        insert_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
        final_df['Inserted_Datetime'] = insert_timestamp

        expected_cols = list(column_map.values()) + ['Inserted_Datetime']
        final_df = final_df[expected_cols]

        print(f"Final columns kept: {final_df.columns.tolist()}")

        # === STEP 5: Export ===
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)
        output_path = temp_dir / f"{source_name.lower()}_stg.txt"

        final_df.to_csv(
            output_path,
            sep='\t',
            index=False,
            header=False,
            encoding='utf-8',
            lineterminator='\r\n',
            quoting=csv.QUOTE_NONE,
            escapechar='\\',
            na_rep=''
        )

        # === STEP 6: Load via BCP ===
        config_folder = BASE_PATH() / get_config("base", "config_folder")
        format_path = config_folder / "format" / f"{source_name.lower()}.fmt"
        
        if not format_path.exists():
            raise FileNotFoundError(f"Format file not found: {format_path}")
        
        print(f"[DEBUG] Using format file: {format_path}")
        
        truncate_table(table_name)  # Clear staging before load

        upload_via_bcp(
            file_path=output_path,
            table=table_name,
            db_config=db_cfg,
            format_file=str(format_path),
            first_row=1
        )

        # === STEP 7: Success - finalize source audit ===
        end_time = datetime.now()
        row_count = len(final_df)
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=end_time,
            row_count=row_count,
            exception_detail=f"Processed {len(all_files)} files, total rows: {row_count}"
        )
        print(f"Successfully uploaded {row_count} records for {source_name} to {table_name}")

    except Exception as e:
        end_time = datetime.now()
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=end_time,
            row_count=total_rows,
            exception_detail=str(e)
        )
        print(f"Failed processing {source_name}: {e}")
        raise


if __name__ == "__main__":
    process_source("Source_Imports")
    process_source("Source_File_Mapping")