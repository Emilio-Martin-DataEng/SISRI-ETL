# src/dw/ddl_generator.py
"""
Metadata-driven DDL generator for DW dimensions.
- Detects mapping changes via Inserted_Datetime/Updated_Datetime
- Outputs .sql to configurable generated/ folder
- Column order follows File_Mapping_SK
- Uses Is_PK for merge ON clause, Is_Type2_Attribute for SCD logic
"""

import json
from datetime import datetime
from pathlib import Path

from src.config import BASE_PATH, get_config
from src.utils.db_ops import get_connection


def _get_ddl_paths():
    base = BASE_PATH() / get_config("dw_ddl", "base_folder", default="config/DW_DDL")
    return {
        "generated": base / get_config("dw_ddl", "generated_folder", default="generated"),
        "run": base / get_config("dw_ddl", "run_folder", default="run"),
        "archive": base / get_config("dw_ddl", "archive_folder", default="archive"),
        "state": base / get_config("dw_ddl", "state_folder", default="state"),
    }


def _get_state_path() -> Path:
    paths = _get_ddl_paths()
    paths["state"].mkdir(parents=True, exist_ok=True)
    return paths["state"] / "dim_mapping_last_checked.json"


def _load_state() -> dict:
    p = _get_state_path()
    if not p.exists():
        return {}
    try:
        with open(p, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def _save_state(state: dict) -> None:
    p = _get_state_path()
    with open(p, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def _get_mapping_max_datetime(conn, source_name: str):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT CONVERT(VARCHAR(30), MAX(COALESCE(Updated_Datetime, Inserted_Datetime)), 121)
        FROM ETL.Dim_Source_Imports_Mapping
        WHERE Source_Name = ?
          AND Is_Deleted = 0
        """,
        source_name,
    )
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row and row[0] else None


def _get_mapping_rows(conn, source_name: str) -> list:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT File_Mapping_SK, Target_Column, Data_Type,
               COALESCE(Is_Type2_Attribute, 0) AS Is_Type2_Attribute,
               COALESCE(Is_PK, 0) AS Is_PK
        FROM ETL.Dim_Source_Imports_Mapping
        WHERE Source_Name = ?
          AND Is_Deleted = 0
          AND Target_Column IS NOT NULL
        ORDER BY File_Mapping_SK
        """,
        source_name,
    )
    rows = cursor.fetchall()
    cursor.close()
    return rows


def _get_ods_table(conn, source_name: str):
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT Staging_Table
        FROM ETL.Dim_Source_Imports
        WHERE Source_Name = ?
          AND Is_Active = 1
          AND Is_Deleted = 0
        """,
        source_name,
    )
    row = cursor.fetchone()
    cursor.close()
    if not row or not row[0]:
        return None
    tbl = row[0].strip()
    if "." in tbl:
        return tbl
    return f"dbo.{tbl}"


def _generate_table_ddl(source_name: str, dw_table: str, mapping_rows: list) -> str:
    """Generate CREATE TABLE or ALTER ADD for new columns."""
    lines = []
    schema, table = dw_table.replace("[", "").replace("]", "").split(".")
    schema = schema.strip()
    table = table.strip()

    sk_col = f"{table.replace('Dim_', '')}_SK"
    if sk_col.endswith("s_SK"):
        sk_col = sk_col[:-4] + "_SK"  # e.g. Dim_Principals -> Principal_SK

    pk_cols = [r.Target_Column for r in mapping_rows if r.Is_PK]
    attr_cols = [r for r in mapping_rows if not r.Is_PK]

    lines.append(f"-- DDL for {source_name} -> {dw_table}")
    lines.append(f"-- Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("USE [SISRI];")
    lines.append("GO")
    lines.append("")

    has_type2 = any(r.Is_Type2_Attribute for r in attr_cols)
    lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')")
    lines.append(f"    EXEC ('CREATE SCHEMA [{schema}] AUTHORIZATION dbo;');")
    lines.append("GO")
    lines.append("")

    lines.append(f"IF NOT EXISTS (SELECT 1 FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = '{schema}' AND t.name = '{table}')")
    lines.append("BEGIN")
    cols = [
        f"    [{sk_col}] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_{table}] PRIMARY KEY,",
        *[f"    [{r.Target_Column}] {r.Data_Type} NULL" for r in mapping_rows],
    ]
    if has_type2:
        cols.extend([
            "    [Row_Is_Current] BIT NOT NULL CONSTRAINT [DF_" + table + "_Row_Is_Current] DEFAULT (1),",
            "    [Row_Effective_Datetime] DATETIME NOT NULL CONSTRAINT [DF_" + table + "_Row_Eff] DEFAULT (GETDATE()),",
            "    [Row_Expiry_Datetime] DATETIME NULL,",
        ])
    cols.extend([
        "    [Inserted_Datetime] DATETIME NOT NULL CONSTRAINT [DF_" + table + "_Inserted] DEFAULT (GETDATE()),",
        "    [Updated_Datetime] DATETIME NULL",
    ])
    lines.append("    CREATE TABLE [" + schema + "].[" + table + "] (")
    lines.append(",\n".join(cols))
    lines.append("    );")
    lines.append("END")
    lines.append("ELSE")
    lines.append("BEGIN")
    lines.append("    -- ADD missing columns (order per File_Mapping_SK)")
    for r in attr_cols:
        lines.append(f"    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = '{schema}' AND t.name = '{table}' AND c.name = '{r.Target_Column}')")
        lines.append(f"        ALTER TABLE [{schema}].[{table}] ADD [{r.Target_Column}] {r.Data_Type} NULL;")
    lines.append("END")
    lines.append("GO")
    lines.append("")
    return "\n".join(lines)


def _generate_merge_proc(source_name: str, dw_table: str, ods_table: str, mapping_rows: list) -> str:
    """Generate SP_Merge_Dim_* using COALESCE, Is_PK for ON, Is_Type2 for SCD, no CTE."""
    lines = []
    schema, table = dw_table.replace("[", "").replace("]", "").split(".")
    schema = schema.strip()
    table = table.strip()
    ods_schema, ods_tbl = (ods_table.split(".") + [ods_table])[:2]

    pk_cols = [r.Target_Column for r in mapping_rows if r.Is_PK]
    attr_cols = [r for r in mapping_rows if not r.Is_PK]
    type1_cols = [r.Target_Column for r in attr_cols if not r.Is_Type2_Attribute]
    type2_cols = [r.Target_Column for r in attr_cols if r.Is_Type2_Attribute]
    has_type2 = bool(type2_cols)

    on_clause = " AND ".join(f"d.[{c}] = o.[{c}]" for c in pk_cols) if pk_cols else "1=0"
    not_exists_clause = " AND ".join(f"d.[{c}] = o.[{c}]" for c in pk_cols) if pk_cols else "1=0"

    lines.append(f"-- Merge proc for {source_name} -> {dw_table}")
    lines.append(f"-- Generated at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("CREATE OR ALTER PROCEDURE [" + schema + "].[SP_Merge_" + table + "]")
    lines.append("    @Source_Import_SK       INT = NULL,")
    lines.append("    @Audit_Source_Import_SK INT = NULL")
    lines.append("AS")
    lines.append("BEGIN")
    lines.append("    SET NOCOUNT ON;")
    lines.append("    SET XACT_ABORT ON;")
    lines.append("")
    lines.append("    DECLARE @ProcName SYSNAME = N'" + schema + ".SP_Merge_" + table + "';")
    lines.append("")
    lines.append("    BEGIN TRY")
    lines.append("")

    # Type 1: UPDATE in place
    if type1_cols:
        set_clause = ", ".join(f"d.[{c}] = o.[{c}]" for c in type1_cols)
        where_changes = " OR ".join(
            f"COALESCE(d.[{c}], '') <> COALESCE(o.[{c}], '')" for c in type1_cols
        )
        lines.append("        -- Type 1: UPDATE changed attributes")
        lines.append(f"        UPDATE d SET")
        lines.append(f"            {set_clause},")
        lines.append("            d.Updated_Datetime = GETDATE()")
        lines.append(f"        FROM [{schema}].[{table}] d")
        lines.append(f"        INNER JOIN [{ods_schema}].[{ods_tbl}] o ON {on_clause}")
        lines.append(f"        WHERE ({where_changes});")
        lines.append("")

    # Type 2: close current row, insert new (simplified - single insert for now)
    if has_type2:
        lines.append("        -- Type 2: close expired rows, insert new (simplified)")
        lines.append("        UPDATE d SET d.Row_Is_Current = 0, d.Row_Expiry_Datetime = GETDATE(), d.Updated_Datetime = GETDATE()")
        lines.append(f"        FROM [{schema}].[{table}] d")
        lines.append(f"        INNER JOIN [{ods_schema}].[{ods_tbl}] o ON {on_clause}")
        lines.append("        WHERE d.Row_Is_Current = 1 AND (")
        type2_where = " OR ".join(
            f"COALESCE(d.[{c}], '') <> COALESCE(o.[{c}], '')" for c in type2_cols
        )
        lines.append(f"            {type2_where}")
        lines.append("        );")
        lines.append("")
        all_attr = [c for c in [r.Target_Column for r in mapping_rows] if c not in pk_cols]
        ins_cols = pk_cols + all_attr + ["Row_Is_Current", "Row_Effective_Datetime", "Row_Expiry_Datetime", "Inserted_Datetime", "Updated_Datetime"]
        sel_cols = pk_cols + [f"o.[{c}]" for c in all_attr] + ["1", "GETDATE()", "NULL", "GETDATE()", "NULL"]
        lines.append("        INSERT INTO [" + schema + "].[" + table + "] (" + ", ".join("[" + c + "]" for c in ins_cols) + ")")
        lines.append("        SELECT " + ", ".join(sel_cols))
        lines.append(f"        FROM [{ods_schema}].[{ods_tbl}] o")
        lines.append(f"        INNER JOIN [{schema}].[{table}] d ON {on_clause} AND d.Row_Is_Current = 1")
        lines.append("        WHERE (")
        lines.append(f"            {type2_where}")
        lines.append("        );")
        lines.append("")

    # INSERT new rows (not matched by target)
    all_attr = [r.Target_Column for r in mapping_rows if not r.Is_PK]
    ins_cols = pk_cols + all_attr
    if has_type2:
        ins_cols += ["Row_Is_Current", "Row_Effective_Datetime", "Row_Expiry_Datetime", "Inserted_Datetime", "Updated_Datetime"]
        sel_cols = [f"o.[{c}]" for c in pk_cols + all_attr] + ["1", "GETDATE()", "NULL", "GETDATE()", "NULL"]
    else:
        ins_cols += ["Inserted_Datetime", "Updated_Datetime"]
        sel_cols = [f"o.[{c}]" for c in pk_cols + all_attr] + ["GETDATE()", "NULL"]

    lines.append("        -- INSERT new dimension rows")
    lines.append(f"        INSERT INTO [{schema}].[{table}] (" + ", ".join("[" + c + "]" for c in ins_cols) + ")")
    lines.append("        SELECT " + ", ".join(sel_cols))
    lines.append(f"        FROM [{ods_schema}].[{ods_tbl}] o")
    subq = f"WHERE NOT EXISTS (SELECT 1 FROM [{schema}].[{table}] d WHERE {not_exists_clause}"
    if has_type2:
        subq += " AND d.Row_Is_Current = 1"
    lines.append("        " + subq + ");")
    lines.append("    END TRY")
    lines.append("    BEGIN CATCH")
    lines.append("        DECLARE @ErrMsg NVARCHAR(MAX) = ERROR_MESSAGE(), @ErrNum INT = ERROR_NUMBER(),")
    lines.append("                @ErrState INT = ERROR_STATE(), @ErrLine INT = ERROR_LINE(), @ErrSev INT = ERROR_SEVERITY();")
    lines.append("        EXEC ETL.SP_Log_ETL_Error @Procedure_Name = @ProcName, @Error_Message = @ErrMsg,")
    lines.append("            @Error_Number = @ErrNum, @Error_State = @ErrState, @Error_Line = @ErrLine, @Error_Severity = @ErrSev,")
    lines.append("            @Source_Import_SK = @Source_Import_SK, @Audit_Source_Import_SK = @Audit_Source_Import_SK;")
    lines.append("        THROW;")
    lines.append("    END CATCH")
    lines.append("END;")
    lines.append("GO")
    return "\n".join(lines)


def generate_ddl_for_changed_sources():
    """
    For each DW dimension source, detect mapping changes (Inserted_Datetime/Updated_Datetime).
    If changed, generate DDL and merge proc to generated/ folder.
    Returns list of source names that had changes.
    """
    dw_dims = get_config("dw_dimensions") or {}
    paths = _get_ddl_paths()
    paths["generated"].mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    state = _load_state()
    changed_sources = []

    try:
        for source_name, dw_table in dw_dims.items():
            max_dt = _get_mapping_max_datetime(conn, source_name)
            if not max_dt:
                continue

            last = state.get(source_name)
            if last and last >= max_dt:
                continue

            mapping_rows = _get_mapping_rows(conn, source_name)
            if not mapping_rows:
                continue

            ods_table = _get_ods_table(conn, source_name)
            if not ods_table:
                continue

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_name = f"dim_{source_name.lower()}_{ts}"
            ddl_path = paths["generated"] / f"{base_name}.sql"

            ddl_table = _generate_table_ddl(source_name, dw_table, mapping_rows)
            ddl_merge = _generate_merge_proc(source_name, dw_table, ods_table, mapping_rows)
            full_ddl = ddl_table + "\n\n" + ddl_merge

            with open(ddl_path, "w", encoding="utf-8") as f:
                f.write(full_ddl)

            state[source_name] = max_dt
            changed_sources.append(source_name)
    finally:
        conn.close()

    if changed_sources:
        _save_state(state)

    return changed_sources
