# src/staging/source_import.py

from pathlib import Path
import pandas as pd
from datetime import datetime
import csv
import shutil

from src.config import BASE_PATH, get_db_config, get_config, SYSTEM_BASE_PATH
from src.utils.db import upload_via_bcp
from src.utils.db_ops import (
    get_next_audit_import_id,
    insert_source_file_archive,
    log_audit_source_import,
    truncate_table,
    get_connection,
    get_source_import_sk,
    generate_bcp_format_file  # ← NEW IMPORT from db_ops
)

CONFIG_SOURCES = {"Source_Imports", "Source_File_Mapping"}

def process_source(source_name: str):
    if source_name in CONFIG_SOURCES:
        print(f"[SKIP] {source_name} is config/metadata - not processed here.")
        return 0

    start_time = datetime.now()
    audit_id = get_next_audit_import_id()

    log_audit_source_import(
        audit_id=audit_id,
        source_import_sk=0,
        start_time=start_time,
        total_row_count=0,
        total_file_count=0,
        pattern=None,
        process_status='Started'
    )

    real_sk = get_source_import_sk(source_name)
    if real_sk == 0:
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=0,
            start_time=start_time,
            end_time=datetime.now(),
            total_row_count=0,
            total_file_count=0,
            exception_detail=f"No config for {source_name}",
            process_status='Failed'
        )
        raise ValueError(f"No config for {source_name}")

    log_audit_source_import(
        audit_id=audit_id,
        source_import_sk=real_sk,
        start_time=start_time,
        process_status='Processing'
    )

    try:
        conn = get_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT Rel_Path, Pattern, Sheet_Name, Staging_Table
            FROM [ETL].[Dim_Source_Imports]
            WHERE Source_Name = ? AND Is_Active = 1
        """, source_name)
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No active config for {source_name}")
        rel_path, pattern, sheet_name, table_name = row

        cursor.execute("""
            SELECT Source_Column, Target_Column
            FROM [ETL].[Dim_Source_Imports_Mapping]
            WHERE Source_Import_SK = ?
        """, real_sk)
        mappings = cursor.fetchall()
        column_map = {m[0]: m[1] for m in mappings}

        cursor.close()
        conn.close()

        folder = BASE_PATH() / rel_path
        all_files = sorted(folder.glob(pattern))

        if not all_files:
            log_audit_source_import(
                audit_id=audit_id,
                source_import_sk=real_sk,
                start_time=start_time,
                end_time=datetime.now(),
                total_row_count=0,
                total_file_count=0,
                exception_detail="No files found",
                pattern=pattern,
                process_status='Skipped'
            )
            return 0

        # NEW: Auto-generate BCP format file for this data source (only if missing)
        format_dir = SYSTEM_BASE_PATH() / get_config("system", "config_folder", "config") / get_config("system", "format_subfolder", "format")
        fmt_path = format_dir / f"{source_name.lower()}.fmt"

        if not fmt_path.exists():
            print(f"[BCP] Generating format file for {source_name}...")
            generate_bcp_format_file(source_name, fmt_path)
        else:
            print(f"[BCP] Using existing format file: {fmt_path}")

        dfs = []
        file_row_counts = []
        for file in all_files:
            try:
                df = pd.read_excel(file, sheet_name=sheet_name, dtype=str)
                df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
                file_rows = len(df)
                dfs.append(df)
                file_row_counts.append(file_rows)
            except Exception as e:
                print(f"Error reading {file.name}: {e}")
                file_row_counts.append(0)
                continue

        if not dfs:
            raise ValueError("No data from files")

        final_df = pd.concat(dfs, ignore_index=True).drop_duplicates()

        if column_map:
            final_df = final_df.rename(columns=column_map)

        final_df = final_df.drop(columns=['Inserted_Datetime'], errors='ignore')

        insert_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
        final_df['Inserted_Datetime'] = insert_timestamp

        expected_cols = list(column_map.values()) + ['Inserted_Datetime']
        final_df = final_df[[c for c in expected_cols if c in final_df.columns]]

        temp_dir = BASE_PATH() / "temp"
        temp_dir.mkdir(exist_ok=True)
        output_path = temp_dir / f"{source_name.lower()}_stg.txt"

        final_df.to_csv(output_path, sep='\t', index=False, header=False, encoding='utf-8', lineterminator='\r\n', quoting=csv.QUOTE_NONE, escapechar='\\', na_rep='')

        truncate_table(table_name)

        db_cfg = get_db_config()
        upload_via_bcp(
            file_path=output_path,
            table=table_name,
            db_config=db_cfg,
            format_file=str(fmt_path),  # Use the generated/existing path
            first_row=1
        )

        # Archive + lineage (your existing code)
        archive_base = BASE_PATH() / "archive" / datetime.now().strftime("%Y-%m")
        archive_dir = archive_base / source_name
        archive_dir.mkdir(parents=True, exist_ok=True)

        timestamp_suffix = datetime.now().strftime("_%Y%m%d_%H%M%S")
        total_file_count = len(all_files)
        total_row_count = len(final_df)

        for idx, file in enumerate(all_files):
            archive_filename = f"{file.stem}{timestamp_suffix}{file.suffix}"
            archive_path = archive_dir / archive_filename
            shutil.copy(file, archive_path)

            insert_source_file_archive(
                audit_id=audit_id,
                source_import_sk=real_sk,
                original_file_name=file.name,
                archive_file_name=archive_filename,
                archive_full_path=str(archive_path),
                file_row_count=file_row_counts[idx],
                process_status='Success'
            )

        end_time = datetime.now()
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=end_time,
            total_row_count=total_row_count,
            total_file_count=total_file_count,
            pattern=pattern,
            exception_detail=f"Processed {total_file_count} files → {total_row_count} rows",
            process_status='Success'
        )

        return total_row_count

    except Exception as e:
        end_time = datetime.now()
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=end_time,
            total_row_count=0,
            total_file_count=0,
            exception_detail=str(e),
            pattern=pattern if 'pattern' in locals() else None,
            process_status='Failed'
        )
        raise

if __name__ == "__main__":
    process_source("Principals")  # Example test