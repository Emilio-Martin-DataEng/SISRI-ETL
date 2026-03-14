# src/utils/db_ops.py

# This module handles ALL database operations in the ETL system
# - Connecting to SQL Server
# - Truncating tables
# - Executing stored procedures
# - Generating unique audit IDs (real insert, no dummy delete)
# - Logging/updating audit records (including archive_file_name)

import re
import logging

import pyodbc
from datetime import datetime
from pathlib import Path
from src.config import get_db_config, PROJECT_ROOT
from src.utils.logging_config import setup_logging
from src.utils.bcp_type_mapper import BCPTypeMapper, BCPTypeMapping

SYSTEM_BASE_PATH = lambda: PROJECT_ROOT

# SQL identifier: Schema.Table, [Schema].[Table], or single part. Alphanumeric + underscore + dot + brackets.
_SQL_IDENTIFIER_RE = re.compile(
    r"^(?:\[?[A-Za-z_][A-Za-z0-9_]*\]?\.)?\[?[A-Za-z_][A-Za-z0-9_]*\]?$"
)


def _validate_sql_identifier(name: str, kind: str = "identifier") -> None:
    """Raise ValueError if name is not a safe SQL identifier (schema.object, [schema].[object], or object)."""
    if not name or not isinstance(name, str):
        raise ValueError(f"Invalid {kind}: must be non-empty string")
    s = name.strip()
    if not s or not _SQL_IDENTIFIER_RE.match(s):
        raise ValueError(f"Invalid {kind} '{name}': only alphanumeric, underscore, dot, brackets allowed")


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
    _validate_sql_identifier(table_name, "table_name")
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


def execute_proc(proc_name: str, params: str = None, params_dict: dict = None):
    """
    Executes a stored procedure.
    - proc_name: validated identifier (e.g. ETL.SP_Merge_Dim_Source_Imports)
    - params: legacy string (avoid for user/config input; use params_dict)
    - params_dict: safe parameterized execution, e.g. {'@SourceName': 'X', '@Audit_Source_Import_SK': 1}
    """
    _validate_sql_identifier(proc_name, "proc_name")
    conn = get_connection()
    cursor = conn.cursor()

    if params_dict is not None:
        placeholders = ", ".join(f"{k}=?" for k in params_dict)
        cursor.execute(f"EXEC {proc_name} {placeholders}", list(params_dict.values()))
    elif params:
        cursor.execute(f"EXEC {proc_name} {params}")
    
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
    
    # Get the newly inserted Source_File_Archive_SK
    cursor.execute("SELECT @@IDENTITY")
    identity_row = cursor.fetchone()
    if identity_row and identity_row[0] is not None:
        new_sk = int(identity_row[0])
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return new_sk


def generate_bcp_format_file(source_name: str, fmt_path: Path):
    """
    Generate a BCP format file for a given source based on
    ETL.Dim_Source_Imports_Mapping with dynamic data type mapping.

    Rules:
    - One entry per Target_Column (Is_Deleted = 0), ordered by Ordinal_Position
    - Map Data_Type to proper BCP SQL type:
      * VARCHAR(n) -> SQLCHAR with length n
      * DECIMAL(m,n) -> SQLCHAR with length m+2 (for decimal point and sign)
      * INT -> SQLCHAR with length 20
      * Inserted_Datetime -> SQLCHAR with length 30
    - Field terminator: "\\t" for all but the last column
    - Row terminator: "\\r\\n" for the last column only
    - For dimensions: Include Audit_Source_Import_SK and Source_File_Archive_SK at end
    """
    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            SELECT Ordinal_Position, Source_Column, Target_Column, Data_Type
            FROM ETL.Dim_Source_Imports_Mapping
            WHERE Source_Name = ?
              AND Is_Deleted = 0
            ORDER BY Ordinal_Position
            """,
            source_name,
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    if not rows:
        raise ValueError(f"No mapping found for source: {source_name}")

    # Check if this is a dimension source
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT Source_Type FROM ETL.Dim_Source_Imports WHERE Source_Name = ?", source_name)
    row = cursor.fetchone()
    source_type = row[0] if row and row[0] is not None else 'Dimension'
    cursor.close()
    conn.close()

    # Build format file entries using the reusable BCPTypeMapper
    format_entries = []
    for idx, (ordinal_position, source_column, target_column, data_type) in enumerate(rows, start=1):
        # Use the reusable BCPTypeMapper for consistent type mapping
        bcp_mapping = BCPTypeMapper.map_sql_to_bcp(data_type)
        
        # Generate format file entry
        entry = BCPTypeMapper.get_format_file_entry(
            field_number=idx,
            sql_type=bcp_mapping.sql_type,
            length=bcp_mapping.length,
            terminator=BCPTypeMapper.get_terminator(is_last_field=False),
            column_name=target_column
        )
        format_entries.append(entry)

    # For dimensions, add system columns in correct order
    if source_type == 'Dimension':
        # Inserted_Datetime (tab terminator, not last column)
        idx = len(format_entries) + 1
        entry = BCPTypeMapper.get_format_file_entry(
            field_number=idx,
            sql_type='SQLCHAR',
            length=30,
            terminator=BCPTypeMapper.get_terminator(is_last_field=False),
            column_name='Inserted_Datetime'
        )
        format_entries.append(entry)
        
        # Audit_Source_Import_SK (SQLINT)
        idx = len(format_entries) + 1
        entry = BCPTypeMapper.get_format_file_entry(
            field_number=idx,
            sql_type='SQLINT',
            length=4,
            terminator=BCPTypeMapper.get_terminator(is_last_field=False),
            column_name='Audit_Source_Import_SK',
            collation='""'
        )
        format_entries.append(entry)
        
        # Source_File_Archive_SK (last column - SQLINT with \r\n)
        idx = len(format_entries) + 1
        entry = BCPTypeMapper.get_format_file_entry(
            field_number=idx,
            sql_type='SQLINT',
            length=4,
            terminator=BCPTypeMapper.get_terminator(is_last_field=True),
            column_name='Source_File_Archive_SK',
            collation='""'
        )
        format_entries.append(entry)
    else:
        # For fact tables, add all audit columns if they're not already in the mapping
        audit_columns = [
            ("Inserted_Datetime", "SQLCHAR", 30),
            ("Audit_Source_Import_SK", "SQLINT", 4),
            ("Source_File_Archive_SK", "SQLINT", 4)
        ]
        
        for col_idx, (col_name, sql_type, length) in enumerate(audit_columns):
            if col_name not in [row[2] for row in rows]:  # Check Target_Column
                idx = len(format_entries) + 1
                is_last = (col_idx == len(audit_columns) - 1)  # Last audit column
                entry = BCPTypeMapper.get_format_file_entry(
                    field_number=idx,
                    sql_type=sql_type,
                    length=length,
                    terminator=BCPTypeMapper.get_terminator(is_last_field=is_last),
                    column_name=col_name,
                    collation='""' if sql_type != 'SQLCHAR' else 'SQL_Latin1_General_CP1_CI_AS'
                )
                format_entries.append(entry)

    # Write format file
    lines = []
    lines.append("14.0")
    lines.append(str(len(format_entries)))
    lines.extend(format_entries)

    fmt_dir = Path(fmt_path).parent
    fmt_dir.mkdir(parents=True, exist_ok=True)

    with open(fmt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")