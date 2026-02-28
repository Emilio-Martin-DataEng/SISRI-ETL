# src/staging/source_import.py

from pathlib import Path
import pandas as pd
from datetime import datetime
import pyodbc
import csv
import shutil

from src.config import BASE_PATH, get_db_config, get_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import (
    get_next_audit_import_id,
    insert_source_file_archive,
    log_audit_source_import,
    truncate_table,
    get_connection,
    generate_bcp_format_file,
)


def process_source(source_name: str):
    start_time = datetime.now()
    audit_id = get_next_audit_import_id()
    log_audit_source_import(
        audit_id=audit_id,
        source_import_sk=0,
        start_time=start_time,
        end_time=None,
        total_row_count=0,
        total_file_count=0,
        exception_detail=None,
        pattern=None,
        process_status='Started'
    )

    real_sk = 0  # Default in case of early failure

    try:
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

        # Get config
        cursor.execute("EXEC ETL.SP_Get_Source_Import_Config ?", source_name)
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No active configuration found for source: {source_name}")
        
        rel_path, pattern, sheet_name, table_name = row

        # Get mapping
        cursor.execute("EXEC ETL.SP_Get_Source_Import_Mapping ?", source_name)
        mappings = cursor.fetchall()
        column_map = {m.Source_Column: m.Target_Column for m in mappings}

        # Get real SK
        cursor.execute("EXEC ETL.SP_Get_Source_Import_SK ?", source_name)
        row = cursor.fetchone()
        real_sk = row[0] if row else 0

        cursor.close()
        conn.close()

        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=None,
            total_row_count=0,
            total_file_count=0,
            exception_detail=f"Processing {source_name}",
            pattern=pattern,
            process_status='Processing'
        )

        folder = BASE_PATH() / rel_path
        all_files = list(folder.glob(pattern))
        
        if not all_files:
            end_time = datetime.now()
            log_audit_source_import(
                audit_id=audit_id,
                source_import_sk=real_sk,
                start_time=start_time,
                end_time=end_time,
                total_row_count=0,
                total_file_count=0,
                exception_detail="Nothing to update - no files matched pattern",
                pattern=pattern,
                process_status='Skipped'
            )
            return

        dfs = []
        file_row_counts = []
        for file in all_files:
            try:
                df = pd.read_excel(file, sheet_name=sheet_name, dtype=str)
                file_rows = len(df)
                dfs.append(df)
                file_row_counts.append(file_rows)
            except Exception as e:
                file_row_counts.append(0)
                continue

        if not dfs:
            raise ValueError("No valid data loaded from any files.")

        final_df = pd.concat(dfs, ignore_index=True)
        final_df = final_df.drop_duplicates()

        if column_map:
            final_df = final_df.rename(columns=column_map)

        if 'Inserted_Datetime' in final_df.columns:
            final_df = final_df.drop(columns=['Inserted_Datetime'])

        insert_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
        final_df['Inserted_Datetime'] = insert_timestamp

        expected_cols = list(column_map.values()) + ['Inserted_Datetime']
        final_df = final_df[expected_cols]

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

        config_folder = BASE_PATH() / get_config("base", "config_folder")
        format_dir = config_folder / "format"
        format_dir.mkdir(parents=True, exist_ok=True)
        format_path = format_dir / f"{source_name.lower()}.fmt"

        # Auto-generate (or refresh) BCP format file based on metadata
        generate_bcp_format_file(source_name, str(format_path))

        truncate_table(table_name)

        upload_via_bcp(
            file_path=output_path,
            table=table_name,
            db_config=db_cfg,
            format_file=str(format_path),
            first_row=1
        )

        # === ARCHIVING + LINEAGE ===
        archive_base = BASE_PATH() / "archive" / "raw" / datetime.now().strftime("%Y-%m-%d")
        archive_dir = archive_base / source_name
        archive_dir.mkdir(parents=True, exist_ok=True)

        timestamp_suffix = datetime.now().strftime("_%Y%m%d_%H%M%S")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        total_file_count = len(all_files)
        total_row_count = len(final_df)

        for idx, file in enumerate(all_files):
            archive_filename = file.stem + timestamp_suffix + file.suffix
            archive_path = archive_dir / archive_filename
            shutil.copy(str(file), str(archive_path))

            new_archive_sk = insert_source_file_archive(
                    audit_id=audit_id,
                    source_import_sk=real_sk,
                    original_file_name=file.name,
                    archive_file_name=archive_filename,
                    archive_full_path=str(archive_path),
                    file_row_count=file_row_counts[idx],
                    process_status='Success'
            )

        conn.commit()
        cursor.close()
        conn.close()

        # === SUCCESS AUDIT ===
        end_time = datetime.now()
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=end_time,
            total_row_count=total_row_count,
            total_file_count=total_file_count,
            exception_detail=f"Processed {total_file_count} files, total rows: {total_row_count}",
            pattern=pattern,
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
            pattern=pattern,
            process_status='Failed'
        )
        return 0
        raise


if __name__ == "__main__":
    # process_source("Source_Imports")
    # process_source("Source_File_Mapping")

    process_source("Principals")
