# src/staging/source_import.py

from pathlib import Path
import pandas as pd
from datetime import datetime
import pyodbc
import csv
import shutil

from src.config import BASE_PATH, get_db_config, get_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import get_next_audit_import_id, log_audit_source_import, truncate_table


def process_source(source_name: str):
    start_time = datetime.now()
    audit_id = get_next_audit_import_id()
    log_audit_source_import(
        audit_id=audit_id,
        source_import_sk=0,
        start_time=start_time,
        end_time=None,
        row_count=0,
        exception_detail=None
    )

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

        cursor.execute("""
            SELECT Rel_Path, Pattern, Sheet_Name, Staging_Table
            FROM ETL.Dim_Source_Imports
            WHERE Source_Name = ? AND Is_Active = 1 AND Is_Deleted = 0
        """, source_name)
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No active configuration found for source: {source_name}")
        
        rel_path, pattern, sheet_name, table_name = row

        cursor.execute("""
            SELECT Source_Column, Target_Column
            FROM ETL.Dim_Source_Imports_Mapping
            WHERE Source_Name = ? AND Is_Deleted = 0
        """, source_name)
        column_map = {r.Source_Column: r.Target_Column for r in cursor.fetchall()}

        cursor.execute("""
            SELECT Source_Import_SK 
            FROM ETL.Dim_Source_Imports 
            WHERE Source_Name = ?
        """, source_name)
        sk_row = cursor.fetchone()
        real_sk = sk_row[0] if sk_row else 0

        cursor.close()
        conn.close()

        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=None,
            row_count=0,
            exception_detail=f"Processing {source_name}"
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
                row_count=0,
                exception_detail="Nothing to update - no files matched pattern"
            )
            return

        dfs = []
        for file in all_files:
            try:
                df = pd.read_excel(file, sheet_name=sheet_name, dtype=str)
                dfs.append(df)
            except Exception as e:
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
        format_path = config_folder / "format" / f"{source_name.lower()}.fmt"
        
        if not format_path.exists():
            raise FileNotFoundError(f"Format file not found: {format_path}")

        truncate_table(table_name)

        upload_via_bcp(
            file_path=output_path,
            table=table_name,
            db_config=db_cfg,
            format_file=str(format_path),
            first_row=1
        )

        # === ARCHIVING + LINEAGE + BRIDGE ===
        archive_base = BASE_PATH() / "archive" / "raw" / datetime.now().strftime("%Y-%m-%d")
        archive_dir = archive_base / source_name
        archive_dir.mkdir(parents=True, exist_ok=True)

        timestamp_suffix = datetime.now().strftime("_%Y%m%d_%H%M%S")
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        for file in all_files:
            archive_filename = file.stem + timestamp_suffix + file.suffix
            archive_path = archive_dir / archive_filename
            shutil.copy(str(file), str(archive_path))

            # Insert lineage and capture SK
            cursor.execute("""
                DECLARE @NewSK INT;
                EXEC [ETL].[SP_Insert_Source_File_Archive]
                    @Audit_Source_Import_SK = ?,
                    @Source_Import_SK = ?,
                    @Original_File_Name = ?,
                    @Archive_File_Name = ?,
                    @Archive_Full_Path = ?,
                    @File_Row_Count = ?,
                    @Process_Status = ?,
                    @Source_File_Archive_SK = @NewSK OUTPUT;
                SELECT @NewSK;
            """, 
            audit_id,
            real_sk,
            file.name,
            archive_filename,
            str(archive_path),
            len(final_df) // len(all_files),
            'Success'
            )

            archive_sk = cursor.fetchone()[0]

            # Insert bridge record
            cursor.execute("""
                EXEC [ETL].[SP_Insert_Bridge_Audit_File_Archive]
                    @Audit_Source_Import_SK = ?,
                    @Source_File_Archive_SK = ?,
                    @Caller_Source_Import_SK = ?,
                    @Caller_Audit_Source_Import_SK = ?
            """, 
            audit_id,
            archive_sk,
            real_sk,
            audit_id
            )

        conn.commit()
        cursor.close()
        conn.close()

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

    except Exception as e:
        end_time = datetime.now()
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=end_time,
            row_count=0,
            exception_detail=str(e)
        )
        raise


if __name__ == "__main__":
    process_source("Source_Imports")
    process_source("Source_File_Mapping")