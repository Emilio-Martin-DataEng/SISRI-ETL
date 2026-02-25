# src/utils/db_ops.py

# This module handles ALL database operations in the ETL system
# - Connecting to SQL Server
# - Truncating tables
# - Executing stored procedures
# - Generating unique audit IDs (real insert, no dummy delete)
# - Logging/updating audit records

import pyodbc
from datetime import datetime

from src.config import get_db_config


def get_connection():
    """
    Creates and returns a new pyodbc connection to the database.
    Uses settings from config.yaml.
    """
    db_cfg = get_db_config()
    
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={db_cfg['server']};"
        f"DATABASE={db_cfg['database']};"
        f"UID={db_cfg['username']};"
        f"PWD={db_cfg['password']}"
    )
    
    return pyodbc.connect(conn_str)


def truncate_table(table_name: str):
    """
    Truncates (deletes all rows from) the specified table.
    Useful before loading fresh data into staging tables.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    conn.commit()
    
    cursor.close()
    conn.close()
    
    print(f"Truncated table: {table_name}")


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


def get_next_audit_import_id() -> int:
    """
    Inserts a new real audit row with Start_Time and returns its ID.
    Uses two separate executes to ensure the SELECT SCOPE_IDENTITY() result is available.
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Step 1: Insert the real audit row (no result set)
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
        print(f"[AUDIT] Created real audit entry with ID: {audit_id}")
        return audit_id
    
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Failed to generate audit ID: {e}")
    
    finally:
        cursor.close()
        conn.close()


def log_audit_source_import(
    audit_id: int,
    source_import_sk: int,
    start_time: datetime,
    end_time: datetime = None,
    row_count: int = 0,
    exception_detail: str = None
):
    """
    Updates the existing audit entry with end time, row count, and exception detail.
    - First call (start): inserts minimal row and returns ID
    - Second call (end): updates the row with completion info
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        UPDATE ETL.Fact_Audit_Source_Imports
        SET 
            Source_Import_SK    = ?,
            Start_Time          = ?,
            End_Time            = ?,
            Row_Count           = ?,
            Exception_Detail    = ?
        WHERE Audit_Source_Import_SK = ?
    """, 
    source_import_sk,
    start_time,
    end_time,
    row_count,
    exception_detail,
    audit_id
    )
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Audit updated for Audit_Source_Import_SK = {audit_id}")