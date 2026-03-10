# src/utils/db_ops.py

# This module handles ALL database operations in the ETL system
# - Connecting to SQL Server
# - Truncating tables
# - Executing stored procedures
# - Generating unique audit IDs (real insert, no dummy delete)
# - Logging/updating audit records (including archive_file_name)

import logging

import pyodbc
from datetime import datetime
from pathlib import Path

from src.config import get_db_config


def get_connection():
    db_cfg = get_db_config()
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_cfg['server']};"
        f"DATABASE={db_cfg['database']};"
        f"UID={db_cfg['username']};"
        f"PWD={db_cfg['password']};"
        f"AUTOCOMMIT=ON"  # Prevents lingering transactions
    )
    return pyodbc.connect(conn_str)

def truncate_table(table_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Truncate failed for {table_name}: {e}")
        raise  # Let caller handle
    finally:
        cursor.close()
        conn.close()


def execute_proc(proc_name: str, params: str = None):
    """
    Executes a stored procedure.
    
    Examples:
    - execute_proc('ETL.SP_Merge_Dim_Source_Imports')
    - execute_proc('SomeProc', '@Param1=Value, @Param2=123')
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    if params:
        cursor.execute(f"EXEC {proc_name} {params}")
    else:
        cursor.execute(f"EXEC {proc_name}")
    
    conn.commit()
    
    cursor.close()
    conn.close()
    
    print(f"Executed stored procedure: {proc_name}")

def get_source_import_sk(source_name: str) -> int:
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Source_Import_SK FROM ETL.Dim_Source_Imports WHERE Source_Name = ?", source_name)
    row = cursor.fetchone()
    sk = row[0] if row else 0
    cursor.close()
    conn.close()
    return sk


def get_next_audit_import_id() -> int:
    """
    Inserts a new real audit row with Start_Time and returns its ID.
    Uses two separate executes to ensure the SELECT SCOPE_IDENTITY() result is available.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Step 1: Insert the real audit row (minimal data)
        cursor.execute("""
            INSERT INTO ETL.Fact_Audit_Source_Imports (Source_Import_SK, Start_Time)
            VALUES (0, GETDATE());
        """)
        
        # Step 2: Separate execute to get the newly inserted ID
        cursor.execute("SELECT SCOPE_IDENTITY();")
        
        row = cursor.fetchone()
        if row is None or row[0] is None:
            raise RuntimeError("Failed to retrieve new Audit_Source_Import_SK")
        
        audit_id = int(row[0])
        
        conn.commit()
        return audit_id
    
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed to generate audit ID: {e}")
    
    finally:
        cursor.close()
        conn.close()


# src/utils/db_ops.py (excerpt – replace the function)

def log_audit_source_import(
    audit_id: int,
    source_import_sk: int,
    start_time: datetime,
    end_time: datetime = None,
    total_row_count: int = 0,
    total_file_count: int = 0,
    exception_detail: str = None,
    pattern: str = None,
    process_status: str = 'Success'
):
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE [ETL].[Fact_Audit_Source_Imports]
        SET 
            Source_Import_SK     = ?,
            Start_Time           = ?,
            End_Time             = ?,
            Total_Row_Count      = ?,
            Total_File_Count     = ?,
            Exception_Detail     = ?,
            Pattern              = ?,
            Process_Status       = ?
        WHERE Audit_Source_Import_SK = ?
    """, 
    source_import_sk,
    start_time,
    end_time,
    total_row_count,
    total_file_count,
    exception_detail,
    pattern,
    process_status,
    audit_id
    )
    
    conn.commit()
    cursor.close()
    conn.close()
    

def insert_source_file_archive(
    audit_id: int,
    source_import_sk: int,
    original_file_name: str,
    archive_file_name: str,
    archive_full_path: str,
    file_row_count: int = None,
    process_status: str = 'Success'
):
    """
    Inserts one record into Fact_Source_File_Archive using the stored procedure.
    Returns the new Source_File_Archive_SK.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    new_sk = 0  # placeholder for OUTPUT
    
    cursor.execute("""
        EXEC [ETL].[SP_Insert_Source_File_Archive]
            @Audit_Source_Import_SK = ?,
            @Source_Import_SK = ?,
            @Original_File_Name = ?,
            @Archive_File_Name = ?,
            @Archive_Full_Path = ?,
            @File_Row_Count = ?,
            @Process_Status = ?,
            @Source_File_Archive_SK = ? OUTPUT
    """,
    audit_id,
    source_import_sk,
    original_file_name,
    archive_file_name,
    archive_full_path,
    file_row_count,
    process_status,
    new_sk
    )
    
    
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return new_sk


def generate_bcp_format_file(source_name: str, fmt_path: Path):
    """
    Generate BCP format file (.fmt) for a given source based on mapping metadata.
    Automatically appends audit/lineage columns at the end.
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Generating BCP format file for {source_name} → {fmt_path}")

    conn = get_connection()
    cursor = conn.cursor()

    try:
        # Fetch column mappings (existing logic)
        cursor.execute("""
            SELECT Target_Column, Data_Type
            FROM [ETL].[Dim_Source_Imports_Mapping]
            WHERE Source_Name = ?
              AND Is_Deleted = 0
            ORDER BY File_Mapping_SK
        """, (source_name,))
        columns = cursor.fetchall()

        if not columns:
            logger.warning(f"No mappings found for {source_name} - empty format file")
            return

        # Build list of columns + forced audit columns
        all_columns = list(columns)  # list of (Target_Column, Data_Type)
        all_columns.append(('Inserted_Datetime', 'DATETIME'))
        all_columns.append(('Audit_Source_Import_SK', 'INT'))
        all_columns.append(('Source_File_Archive_SK', 'INT'))

        # Total number of columns
        num_cols = len(all_columns)

        # Start building .fmt content
        fmt_lines = [
            "15.0",                # Version
            str(num_cols)          # Total columns
        ]

        col_num = 1
        for col_name, data_type in all_columns:
            # BCP type mapping (simplified - adjust for your real types)
            if 'VARCHAR' in data_type.upper() or 'CHAR' in data_type.upper():
                bcp_type = "SQLCHAR"
                prefix_len = "0"
                length = "8000"  # safe max for VARCHAR
            elif 'INT' in data_type.upper():
                bcp_type = "SQLINT"
                prefix_len = "0"
                length = "4"
            elif 'DECIMAL' in data_type.upper():
                bcp_type = "SQLCHAR"  # treat as string for safety
                prefix_len = "0"
                length = "50"
            elif 'DATETIME' in data_type.upper():
                bcp_type = "SQLCHAR"  # safer as string
                prefix_len = "0"
                length = "30"
            else:
                bcp_type = "SQLCHAR"
                prefix_len = "0"
                length = "8000"

            # Terminator: \t for all except last (\r\n)
            terminator = "\\t" if col_num < num_cols else "\\r\\n"

            fmt_line = (
                f"{col_num:<8} {bcp_type:<10} {prefix_len:<8} {length:<8} "
                f'"{terminator}" {col_num:<8} {col_name:<30} ""'
            )
            fmt_lines.append(fmt_line)
            col_num += 1

        # Write format file
        fmt_path.parent.mkdir(parents=True, exist_ok=True)
        with open(fmt_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(fmt_lines) + '\n')

        logger.info(f"BCP format file generated: {fmt_path} ({num_cols} columns)")

    except Exception as e:
        logger.error(f"Failed generating .fmt for {source_name}: {e}")
        raise
    finally:
        cursor.close()
        conn.close()