# src/staging/etl_config.py

from pathlib import Path
import pandas as pd
import csv
from datetime import datetime
import pyodbc

from src.config import BASE_PATH, get_db_config, get_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import (
    truncate_table,
    execute_proc,
    log_audit_source_import,
    get_next_audit_import_id,
    get_connection  # Now imported for SK fetch
)


def process_etl_config():
    """
    Loads ETL configuration from Excel into staging tables,
    merges to dimension tables via stored procedures,
    and logs the process to audit table.
    """
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
    print(f"Started audit log entry: Audit_Source_Import_SK = {audit_id}")

    try:
        config_folder = BASE_PATH() / get_config("base", "config_folder")
        config_filename = get_config("base", "config_filename")
        
        config_files = list(config_folder.glob(config_filename))
        
        if not config_files:
            raise FileNotFoundError(f"Config spreadsheet not found: {config_folder / config_filename}")
        
        config_file = config_files[0]
        print(f"Loading config from: {config_file}")

        format_dir = config_folder / "format"
        format_imports = format_dir / "source_imports.fmt"
        format_mapping = format_dir / "source_file_mapping.fmt"

        format_dir.mkdir(exist_ok=True)

        print(f"Using format files:")
        print(f"  - Source_Imports: {format_imports}")
        print(f"  - Source_File_Mapping: {format_mapping}")

        df_imports = pd.read_excel(config_file, sheet_name="Source_Imports", dtype=str)
        df_mapping = pd.read_excel(config_file, sheet_name="Source_File_Mapping", dtype=str)

        df_imports = df_imports.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df_mapping = df_mapping.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
        df_imports['Inserted_Datetime'] = now_str
        df_mapping['Inserted_Datetime'] = now_str

        temp_dir = Path("temp")
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

        print("Loading Source_Imports table...")
        upload_via_bcp(
            file_path=imports_path,
            table='ETL.Source_Imports',
            db_config=db_cfg,
            format_file=str(format_imports),
            first_row=1
        )

        print("Loading Source_File_Mapping table...")
        upload_via_bcp(
            file_path=mapping_path,
            table='ETL.Source_File_Mapping',
            db_config=db_cfg,
            format_file=str(format_mapping),
            first_row=1
        )

        print("Merging staging to dimension tables...")
        execute_proc('ETL.SP_Merge_Dim_Source_Imports')
        execute_proc('ETL.SP_Merge_Dim_Source_Imports_Mapping')

        # Fetch real Source_Import_SK for 'Source_Imports'
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT Source_Import_SK 
            FROM ETL.Dim_Source_Imports 
            WHERE Source_Name = 'Source_Imports'
        """)
        sk_row = cursor.fetchone()
        real_sk = sk_row[0] if sk_row else 0
        cursor.close()
        conn.close()
        print(f"[DEBUG] Real Source_Import_SK for config: {real_sk}")

        # Update audit with real SK + success data
        end_time = datetime.now()
        row_count = len(df_imports) + len(df_mapping)
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=real_sk,
            start_time=start_time,
            end_time=end_time,
            row_count=row_count,
            exception_detail=None
        )
        print("ETL configuration loaded and merged successfully.")

    except Exception as e:
        end_time = datetime.now()
        log_audit_source_import(
            audit_id=audit_id,
            source_import_sk=0,
            start_time=start_time,
            end_time=end_time,
            row_count=0,
            exception_detail=str(e)
        )
        print(f"ETL config failed: {e}")
        raise


if __name__ == "__main__":
    process_etl_config()