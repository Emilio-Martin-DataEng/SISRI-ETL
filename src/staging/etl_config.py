# src/staging/etl_config.py

"""
Meta-loader / config bootstrapper for the ETL system.
- Loads config from Excel → staging tables via BCP
- Merges staging to dimension tables via stored procedures
- Gracefully skips if config file is missing
- Uses stored procedures for truncate and merge operations
- Logs audit start + success/failure with real SK
- All DB logic via stored procs or helpers in db_ops.py
"""

from pathlib import Path
import pandas as pd
import csv
import shutil
from datetime import datetime

from src.config import BASE_PATH, get_db_config, get_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import (
    truncate_table,
    execute_proc,
    log_audit_source_import,
    get_next_audit_import_id,
    get_source_import_sk,
    insert_source_file_archive,
)


def process_etl_config():
    """
    Loads ETL configuration from Excel into staging tables,
    merges to dimension tables via stored procedures,
    and logs the process to audit table at **dataset grain**:
    - One audit row for `Source_Imports`
    - One audit row for `Source_File_Mapping`
    A single physical Excel file is archived once and referenced by
    both audit rows in `Fact_Source_File_Archive`.

    Graceful skip if config file is missing:
    - Creates a single audit entry with "Nothing to update - config spreadsheet not found"
    - Exits without raising exception
    - Allows orchestrator to continue with other sources
    """
    start_time = datetime.now()

    # Per-dataset audit tracking (created only when config file exists)
    imports_audit_id: int | None = None
    mapping_audit_id: int | None = None
    imports_start_time = start_time
    mapping_start_time = start_time

    try:
        # === Locate config file ===
        config_folder = BASE_PATH() / get_config("base", "config_folder")
        config_filename = get_config("base", "config_filename")

        config_files = list(config_folder.glob(config_filename))

        if not config_files:
            # Preserve existing behaviour: one "Skipped" audit row when the
            # config spreadsheet is missing.
            audit_id = get_next_audit_import_id()
            end_time = datetime.now()
            log_audit_source_import(
                audit_id=audit_id,
                source_import_sk=0,
                start_time=start_time,
                end_time=end_time,
                total_row_count=0,
                total_file_count=0,
                exception_detail="Nothing to update - config spreadsheet not found",
                pattern=None,
                process_status="Skipped",
            )
            print(f"[GRACEFUL SKIP] Config spreadsheet not found: {config_folder / config_filename}")
            print("Continuing ETL run without config refresh")
            return  # Exit cleanly - no crash

        config_file = config_files[0]
        print(f"Loading ETL config from: {config_file}")

        # Read sheets (force string to avoid type issues)
        df_imports = pd.read_excel(config_file, sheet_name="Source_Imports", dtype=str)
        df_mapping = pd.read_excel(config_file, sheet_name="Source_File_Mapping", dtype=str)

        # Clean whitespace
        df_imports = df_imports.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        df_mapping = df_mapping.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

        # Ensure Is_Type2_Attribute and Is_PK exist; normalize TRUE/FALSE to 1/0 for BCP bit
        for col in ("Is_Type2_Attribute", "Is_PK"):
            if col not in df_mapping.columns:
                df_mapping[col] = "0"
            else:
                df_mapping[col] = df_mapping[col].apply(
                    lambda v: "1" if str(v).upper() in ("TRUE", "1", "YES") else "0"
                )

        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.000")
        df_imports["Inserted_Datetime"] = now_str
        df_mapping["Inserted_Datetime"] = now_str

        # Column order for Source_File_Mapping (must match .fmt file)
        mapping_cols = [
            "Source_Name", "Source_Column", "Target_Column", "Data_Type", "Description",
            "Is_Type2_Attribute", "Is_PK", "Inserted_Datetime",
        ]
        df_mapping = df_mapping[[c for c in mapping_cols if c in df_mapping.columns]]

        # Prepare text files for BCP
        temp_dir = Path("temp")
        temp_dir.mkdir(exist_ok=True)

        imports_path = temp_dir / "source_imports_stg.txt"
        mapping_path = temp_dir / "source_file_mapping_stg.txt"

        df_imports.to_csv(
            imports_path,
            sep="\t",
            index=False,
            header=False,
            encoding="utf-8",
            lineterminator="\r\n",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            na_rep="",
        )

        df_mapping.to_csv(
            mapping_path,
            sep="\t",
            index=False,
            header=False,
            encoding="utf-8",
            lineterminator="\r\n",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            na_rep="",
        )

        db_cfg = get_db_config()

        # Create per-dataset audit rows now that we know we have a real file
        imports_start_time = datetime.now()
        mapping_start_time = imports_start_time

        imports_audit_id = get_next_audit_import_id()
        mapping_audit_id = get_next_audit_import_id()

        # Initial "Running" audit entries at dataset grain
        log_audit_source_import(
            audit_id=imports_audit_id,
            source_import_sk=0,
            start_time=imports_start_time,
            end_time=None,
            total_row_count=0,
            total_file_count=0,
            exception_detail=None,
            pattern=config_filename,
            process_status="Running",
        )
        log_audit_source_import(
            audit_id=mapping_audit_id,
            source_import_sk=0,
            start_time=mapping_start_time,
            end_time=None,
            total_row_count=0,
            total_file_count=0,
            exception_detail=None,
            pattern=config_filename,
            process_status="Running",
        )

        # Format file paths
        format_dir = config_folder / "format"
        format_imports = format_dir / "source_imports.fmt"
        format_mapping = format_dir / "source_file_mapping.fmt"

        format_dir.mkdir(exist_ok=True)

        # Truncate staging tables via stored procedure (preferred over direct TRUNCATE)
        # Note: if you later move truncate to proc, replace these calls
        truncate_table("ETL.Source_Imports")
        truncate_table("ETL.Source_File_Mapping")

        # BCP load
        upload_via_bcp(
            file_path=imports_path,
            table="ETL.Source_Imports",
            db_config=db_cfg,
            format_file=str(format_imports),
            first_row=1,
        )

        upload_via_bcp(
            file_path=mapping_path,
            table="ETL.Source_File_Mapping",
            db_config=db_cfg,
            format_file=str(format_mapping),
            first_row=1,
        )

        # Merge via stored procedures
        execute_proc("ETL.SP_Merge_Dim_Source_Imports")
        execute_proc("ETL.SP_Merge_Dim_Source_Imports_Mapping")

        # Fetch real SKs for config sources
        imports_sk = get_source_import_sk("Source_Imports")
        mapping_sk = get_source_import_sk("Source_File_Mapping")

        # === ARCHIVING + LINEAGE ===
        # One physical archive copy of the Excel file, referenced by two audit rows.
        archive_base = BASE_PATH() / "archive" / "raw" / datetime.now().strftime("%Y-%m-%d")
        archive_dir = archive_base / "Config"
        archive_dir.mkdir(parents=True, exist_ok=True)

        timestamp_suffix = datetime.now().strftime("_%Y%m%d_%H%M%S")
        archive_filename = config_file.stem + timestamp_suffix + config_file.suffix
        archive_path = archive_dir / archive_filename

        shutil.copy(str(config_file), str(archive_path))

        # Lineage for Source_Imports sheet
        insert_source_file_archive(
            audit_id=imports_audit_id,
            source_import_sk=imports_sk,
            original_file_name=config_file.name,
            archive_file_name=archive_filename,
            archive_full_path=str(archive_path),
            file_row_count=len(df_imports),
            process_status="Success",
        )

        # Lineage for Source_File_Mapping sheet
        insert_source_file_archive(
            audit_id=mapping_audit_id,
            source_import_sk=mapping_sk,
            original_file_name=config_file.name,
            archive_file_name=archive_filename,
            archive_full_path=str(archive_path),
            file_row_count=len(df_mapping),
            process_status="Success",
        )

        # Success audit updates at dataset grain
        end_time = datetime.now()
        log_audit_source_import(
            audit_id=imports_audit_id,
            source_import_sk=imports_sk,
            start_time=imports_start_time,
            end_time=end_time,
            total_row_count=len(df_imports),
            total_file_count=1,  # one physical file feeding this dataset
            exception_detail=None,
            pattern=config_filename,
            process_status="Success",
        )
        log_audit_source_import(
            audit_id=mapping_audit_id,
            source_import_sk=mapping_sk,
            start_time=mapping_start_time,
            end_time=end_time,
            total_row_count=len(df_mapping),
            total_file_count=1,  # one physical file feeding this dataset
            exception_detail=None,
            pattern=config_filename,
            process_status="Success",
        )

    except Exception as e:
        end_time = datetime.now()

        # If per-dataset audits were created, mark them as failed
        if imports_audit_id is not None:
            log_audit_source_import(
                audit_id=imports_audit_id,
                source_import_sk=0,
                start_time=imports_start_time,
                end_time=end_time,
                total_row_count=0,
                total_file_count=0,
                exception_detail=str(e),
                pattern=config_filename if "config_filename" in locals() else None,
                process_status="Failed",
            )

        if mapping_audit_id is not None:
            log_audit_source_import(
                audit_id=mapping_audit_id,
                source_import_sk=0,
                start_time=mapping_start_time,
                end_time=end_time,
                total_row_count=0,
                total_file_count=0,
                exception_detail=str(e),
                pattern=config_filename if "config_filename" in locals() else None,
                process_status="Failed",
            )

        print(f"ETL config failed: {e}")
        raise  # Let orchestrator decide whether to continue or abort


if __name__ == "__main__":
    process_etl_config()