# src/staging/principals.py

from pathlib import Path
import pandas as pd
from datetime import datetime

from src.config import BASE_PATH, get_config, get_db_config
from src.utils.db import upload_via_bcp
import csv as csv
import pyodbc


def process_principals():
    """
    ETL process for Principals dimension:
    - Reads Excel files from configured path
    - Concatenates, deduplicates, renames columns
    - Adds audit timestamp
    - Exports to tab-delimited text
    - Loads into ODS.Principles via BCP
    """
    
    dim_cfg = get_config("dimensions", "principals")
    if not dim_cfg:
        raise ValueError("Principals config not found in config.yaml")

    folder = BASE_PATH() / dim_cfg["rel_path"]
    pattern = dim_cfg["pattern"]
    sheet_name = dim_cfg["sheet_name"]
    table_name = dim_cfg["staging_table"]

    all_files = list(folder.glob(pattern))
    if not all_files:
        print(f"No files found in {folder} matching {pattern}.")
        return

    dfs = []
    for file in all_files:
        try:
            df = pd.read_excel(file, sheet_name=sheet_name, dtype=str)
            dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            continue

    if not dfs:
        print("No valid data loaded.")
        return

    # Combine and clean
    final_df = pd.concat(dfs, ignore_index=True)
    final_df = final_df.drop_duplicates()

    # Rename columns to match staging table exactly
    column_map = {
        'Principle_Code': 'Principle_Code',
        'Principle_Name': 'Principle_Name',
        'Principle_Trading_As_Name': 'Principle_Trading_As_Name',
        'Principle_Address': 'Principle_Address',
        'Principle_City': 'Principle_City',
        'Principle_Province': 'Principle_Province',
        'Principle_Country': 'Principle_Country',
    }
    final_df = final_df.rename(columns=column_map)

    # Keep only expected columns
    expected_cols = list(column_map.values())
    final_df = final_df[expected_cols]

    # Add audit timestamp in BCP-safe format
    insert_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.000')
    final_df['Inserted_Datetime'] = insert_timestamp

    # Export to tab-delimited text
    temp_dir = Path("temp")
    temp_dir.mkdir(exist_ok=True)
    output_path = temp_dir / "principals_stg.txt"

    final_df.to_csv(
        output_path,
        sep='\t',
        index=False,
        header=True,               # BCP skips this row
        encoding='utf-8',
        lineterminator='\r\n',
        quoting=csv.QUOTE_NONE,
        escapechar='\\',
        na_rep=''
    )

    # Load via BCP using pre-generated format file
    db_cfg = get_db_config()
    db_type = get_config("database", "type").lower()

    try:
        if db_type in ("sqlserver", "mssql"):
            upload_via_bcp(
                file_path=output_path,
                table=table_name,
                db_config=db_cfg,
                format_file="principals.fmt",  # Full 8-column mapping
                first_row=2                    # Skip header
            )
            print(f"Uploaded {len(final_df)} principals records to {table_name}")
        else:
            raise ValueError(f"Unsupported DB type: {db_type}")
    except Exception as e:
        print(f"Upload failed: {e}")

    # Optional: keep temp file for inspection during dev
    # output_path.unlink(missing_ok=True)


if __name__ == "__main__":
    process_principals()