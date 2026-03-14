# src/dw/ddl_generator.py
"""
Single DDL generator for SISRI ETL.
- ODS tables, DW dim tables, merge procs, fact conformed merge
- Outputs to config/DW_DDL/generated/ or run/
- Uses PROJECT_ROOT for DW_DDL base (consistent with etl_config)
"""

import json
from datetime import datetime
from pathlib import Path

from src.config import PROJECT_ROOT, get_config
from src.utils.db_ops import get_connection, log_audit_source_import, get_next_audit_import_id


def _get_ddl_paths():
    """DW_DDL paths: project root / base_folder (consistent with etl_config)."""
    base = PROJECT_ROOT / get_config("dw_ddl", "base_folder", default="DW_DDL")
    return {
        "generated": base / get_config("dw_ddl", "generated_folder", default="generated"),
        "run": base / get_config("dw_ddl", "run_folder", default="run"),
        "archive": base / get_config("dw_ddl", "archive_folder", default="archive"),
        "state": base / get_config("dw_ddl", "state_folder", default="state"),
    }


# ---------------------------------------------------------------------------
# ODS, DW dim, merge proc, fact conformed (from legacy utils.ddl_generator)
# ---------------------------------------------------------------------------


def generate_ods_table_ddl(source_name: str, columns: list[dict], schema: str = "ODS") -> str:
    """Generate CREATE TABLE DDL for an ODS staging table from metadata mappings."""
    column_defs = []
    pk_cols = []

    for col in columns:
        col_name = col.get("Target_Column", "").strip()
        if not col_name:
            continue
        data_type = col.get("Data_Type", "VARCHAR(255)").strip() or "VARCHAR(255)"
        is_required = str(col.get("Is_Required", 0)).strip().lower() in ("1", "true", "y", "yes")
        nullability = "NOT NULL" if is_required else "NULL"
        column_defs.append(f"    [{col_name}] {data_type} {nullability}")
        if str(col.get("Is_PK", 0)).strip().lower() in ("1", "true", "y", "yes"):
            pk_cols.append(f"[{col_name}]")

    column_defs.append("    [Inserted_Datetime] DATETIME NOT NULL")
    column_defs.append("    [Audit_Source_Import_SK] INT NOT NULL")
    column_defs.append("    [Source_File_Archive_SK] INT NOT NULL")

    pk_clause = ""
    if pk_cols:
        pk_cols_str = ", ".join(pk_cols)
        pk_clause = f""",
    CONSTRAINT [PK_{source_name}] PRIMARY KEY CLUSTERED (
        {pk_cols_str}
    ) WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, 
            ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]"""

    return f"""-- ODS table for {source_name} (generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')})
IF OBJECT_ID('[{schema}].[{source_name}]', 'U') IS NOT NULL
    DROP TABLE [{schema}].[{source_name}];
GO

CREATE TABLE [{schema}].[{source_name}] (
{',\n'.join(column_defs)}
{pk_clause}
) ON [PRIMARY];
GO

ALTER TABLE [{schema}].[{source_name}] ADD DEFAULT (GETDATE()) FOR [Inserted_Datetime];
GO
"""


def generate_dw_table_ddl(schema: str, table_name: str, columns: list[dict], timestamp: str) -> str:
    """Generate DW dimension table DDL using SCD1/SCD2 templates."""
    is_scd2 = any(
        val in (1, 1.0, "1", True) or (isinstance(val, str) and val.strip() == "1")
        for c in columns
        for val in [c.get("Is_Type2_Attribute")]
        if val is not None
    )
    pattern = "dim_table_scd_type2" if is_scd2 else "dim_table_scd_type1"
    template_path = PROJECT_ROOT / "sp_templates" / f"{pattern}.template.sql"
    if not template_path.exists():
        raise FileNotFoundError(f"Template missing: {template_path}")
    template = template_path.read_text(encoding="utf-8")

    sk_col = f"{table_name.replace('Dim_', '')}_SK"
    data_defs = []
    insert_cols = []
    for col in columns:
        col_name = col["Target_Column"].strip()
        is_required = int(col.get("Is_Required", 0)) == 1
        nullability = "NOT NULL" if is_required else "NULL"
        data_defs.append(f", [{col_name}] {col['Data_Type']} {nullability}")
        insert_cols.append(f"[{col_name}]")

    nk_cols = [c["Target_Column"].strip() for c in columns if int(c.get("Is_PK", 0)) == 1]
    nk_where = "WHERE [Row_Is_Current] = 1 AND [Is_Deleted] = 0" if is_scd2 else "WHERE [Is_Deleted] = 0"
    unique_index_clause = (
        f"CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_{table_name}_Active] "
        f"ON [{schema}].[{table_name}] ({', '.join([f'[{c}]' for c in nk_cols])}) {nk_where};"
        if nk_cols
        else "-- No natural key columns defined - no unique index created"
    )

    return template.format(
        schema=schema,
        table_name=table_name,
        sk_col=sk_col,
        generated_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        timestamp=timestamp,
        column_defs="\n".join(data_defs),
        insert_columns=", ".join(insert_cols),
        select_columns=", ".join(insert_cols),
        unique_index_clause=unique_index_clause,
    )


def generate_merge_proc_ddl(source_name: str, staging_table: str, dw_table: str, columns: list[dict]) -> str:
    """Generate merge proc DDL for dimension ODS -> DW using SCD1/SCD2 templates."""
    is_scd2 = any(
        val in (1, 1.0, "1", True) or (isinstance(val, str) and val.strip() == "1")
        for c in columns
        for val in [c.get("Is_Type2_Attribute")]
        if val is not None
    )
    pattern = "scd_type2" if is_scd2 else "scd_type1"
    template_path = PROJECT_ROOT / "sp_templates" / f"{pattern}.template.sql"
    if not template_path.exists():
        raise FileNotFoundError(f"Template missing: {template_path}")
    template = template_path.read_text(encoding="utf-8")

    key_column = next((c["Target_Column"] for c in columns if int(c.get("Is_PK", 0)) == 1), "Source_Name")
    type1_cols = [c for c in columns if int(c.get("Is_Type2_Attribute", 0)) != 1 and int(c.get("Is_PK", 0)) != 1]
    update_columns = ", ".join([f"d.[{c['Target_Column']}] = o.[{c['Target_Column']}]" for c in type1_cols])
    type1_where_changes = (
        " OR ".join(
            f"COALESCE(d.[{c['Target_Column']}], '') <> COALESCE(o.[{c['Target_Column']}], '')"
            for c in type1_cols
        )
        or "1=0"
    )
    type2_cols = [c for c in columns if int(c.get("Is_Type2_Attribute", 0)) == 1]
    type2_where_changes = (
        " OR ".join(
            f"COALESCE(d.[{c['Target_Column']}], '') <> COALESCE(o.[{c['Target_Column']}], '')"
            for c in type2_cols
        )
        or "1=0"
    )

    type1_update_block = ""
    if type1_cols:
        type1_update_block = f"""-- Type 1: UPDATE changed attributes
UPDATE d SET
    {update_columns},
    d.Updated_Datetime = GETDATE(),
    d.Audit_Source_Import_SK = @Audit_Source_Import_SK,
    d.Source_File_Archive_SK = @Source_File_Archive_SK
FROM {dw_table} d
INNER JOIN {staging_table} o ON d.[{key_column}] = o.[{key_column}]
WHERE ({type1_where_changes});
SET @UpdatedCount = @@ROWCOUNT;
"""
    else:
        type1_update_block = """-- No Type 1 columns to update
SET @UpdatedCount = 0;
"""

    return template.format(
        dim_name=source_name,
        dw_table=dw_table,
        ods_table=staging_table,
        generated_time=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        type1_update_block=type1_update_block,
        type2_where_changes=type2_where_changes,
        insert_columns=", ".join([f"[{c['Target_Column']}]" for c in columns]),
        select_columns=", ".join([f"o.[{c['Target_Column']}]" for c in columns]),
        join_condition=f"d.[{key_column}] = o.[{key_column}]",
        key_column=key_column,
    )


def generate_fact_to_conformed_merge_ddl(
    source_name: str, ods_table: str, conformed_table: str, mapping_rows: list[dict]
) -> str:
    """Generate merge proc for Fact_Sales ODS -> conformed staging (uses Dim_DW_Mapping_And_Transformations)."""
    proc_name = "[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]"
    return f"""-- Generated merge procedure for {source_name}
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

IF OBJECT_ID('{proc_name}', 'P') IS NOT NULL
    DROP PROCEDURE {proc_name}
GO

CREATE PROCEDURE {proc_name}
    @SourceName VARCHAR(100),
    @Source_File_Archive_SK INT = -1,
    @Audit_Source_Import_SK INT = -1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    DECLARE @ProcName SYSNAME = N'{proc_name}';
    DECLARE @RowsInserted INT = 0;
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @SelectList NVARCHAR(MAX);
    DECLARE @InsertList NVARCHAR(MAX);
    
    BEGIN TRY
        IF NOT EXISTS (SELECT 1 FROM [ETL].[Dim_Source_Imports] WHERE Source_Name = @SourceName AND Is_Active = 1)
            RAISERROR('Source [%s] is not active in Dim_Source_Imports', 16, 1, @SourceName);
        
        SELECT @InsertList = STRING_AGG(QUOTENAME(Conformed_Column), ', '),
               @SelectList = STRING_AGG(
            CASE 
                WHEN Transformation_Type = 'Direct' 
                    THEN N'COALESCE(s.' + QUOTENAME(ODS_Column) + ', ' + CASE WHEN Default_Value IS NULL OR LTRIM(RTRIM(ISNULL(Default_Value,''))) = '' THEN N'NULL' WHEN TRY_CAST(Default_Value AS BIGINT) IS NOT NULL THEN Default_Value ELSE N'''''' + REPLACE(ISNULL(Default_Value,''), '''''', '''''''''') + N'''''' END + ') AS ' + QUOTENAME(Conformed_Column)
                WHEN Transformation_Type = 'Expression'
                    THEN N'COALESCE(' + REPLACE(Transformation_Rule, '[' + ODS_Column + ']', 's.' + QUOTENAME(ODS_Column)) + ', ' + CASE WHEN Default_Value IS NULL OR LTRIM(RTRIM(ISNULL(Default_Value,''))) = '' THEN N'NULL' WHEN TRY_CAST(Default_Value AS BIGINT) IS NOT NULL THEN Default_Value ELSE N'''''' + REPLACE(ISNULL(Default_Value,''), '''''', '''''''''') + N'''''' END + ') AS ' + QUOTENAME(Conformed_Column)
                WHEN Transformation_Type = 'Date_SK'
                    THEN N'CASE WHEN TRY_CONVERT(DATE, s.' + QUOTENAME(ODS_Column) + ', ' +
                    CASE WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'dd-MM-yyyy' THEN N'105' WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'yyyy/MM/dd' THEN N'111' WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'yyyy-MM-dd' THEN N'23' WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'MM/dd/yyyy' THEN N'101' WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'dd/MM/yyyy' THEN N'103' ELSE N'105' END +
                    N') IS NOT NULL THEN CONVERT(INT, FORMAT(TRY_CONVERT(DATE, s.' + QUOTENAME(ODS_Column) + ', ' +
                    CASE WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'dd-MM-yyyy' THEN N'105' WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'yyyy/MM/dd' THEN N'111' WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'yyyy-MM-dd' THEN N'23' WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'MM/dd/yyyy' THEN N'101' WHEN LTRIM(RTRIM(ISNULL(Transformation_Rule,''))) = 'dd/MM/yyyy' THEN N'103' ELSE N'105' END +
                    N'), ''yyyyMMdd'')) ELSE ' + CASE WHEN Default_Value IS NULL OR LTRIM(RTRIM(ISNULL(Default_Value,''))) = '' THEN N'''19000101''' WHEN TRY_CAST(Default_Value AS BIGINT) IS NOT NULL THEN Default_Value ELSE N'''19000101''' END + ' END AS ' + QUOTENAME(Conformed_Column)
                WHEN Transformation_Type = 'Calculated'
                    THEN Transformation_Rule + ' AS ' + QUOTENAME(Conformed_Column)
                ELSE N'COALESCE(s.' + QUOTENAME(ODS_Column) + ', ' + CASE WHEN Default_Value IS NULL OR LTRIM(RTRIM(ISNULL(Default_Value,''))) = '' THEN N'NULL' WHEN TRY_CAST(Default_Value AS BIGINT) IS NOT NULL THEN Default_Value ELSE N'''''' + REPLACE(ISNULL(Default_Value,''), '''''', '''''''''') + N'''''' END + ') AS ' + QUOTENAME(Conformed_Column)
            END, N',' + CHAR(13) + CHAR(10)
        ) WITHIN GROUP (ORDER BY Sequence_Order)
        FROM [ETL].[Dim_DW_Mapping_And_Transformations]
        WHERE Source_Name = @SourceName AND Is_Deleted = 0;
        
        IF @SelectList IS NULL
            RAISERROR('No transformation rules found for source [%s] in Dim_DW_Mapping_And_Transformations', 16, 1, @SourceName);
        
        DECLARE @KeyJoinConditions NVARCHAR(MAX) = (
            SELECT STRING_AGG('c.' + QUOTENAME(Conformed_Column) + ' = t.' + QUOTENAME(Conformed_Column), N' AND ')
            FROM [ETL].[Dim_DW_Mapping_And_Transformations]
            WHERE Source_Name = @SourceName AND Is_Deleted = 0 AND Is_Key = 1
        );
        
        IF @KeyJoinConditions IS NULL
            RAISERROR('No key columns found for source [%s] in Dim_DW_Mapping_And_Transformations', 16, 1, @SourceName);
        
        -- Temp table: apply all mappings/transformations once; then delete + insert in same batch
        -- Idempotency: drop #Staged if exists before SELECT INTO
        -- Source_File_Archive_SK: flow from ODS (COALESCE with param fallback)
        SET @SQL = N'
        IF OBJECT_ID(''tempdb..#Staged'') IS NOT NULL DROP TABLE #Staged;
        SELECT ' + @SelectList + N', COALESCE(s.[Source_File_Archive_SK], @Source_File_Archive_SK) AS [Source_File_Archive_SK]
        INTO #Staged FROM [ODS].' + QUOTENAME(@SourceName) + N' s;
        DELETE c FROM {conformed_table} c INNER JOIN #Staged t ON ' + @KeyJoinConditions + N';
        INSERT INTO {conformed_table}
        (' + @InsertList + N',
            [Inserted_Datetime], [Audit_Source_Import_SK], [Source_File_Archive_SK]
        )
        SELECT ' + @InsertList + N',
            GETDATE(),
            @Audit_Source_Import_SK,
            t.[Source_File_Archive_SK]
        FROM #Staged t';
        EXEC sp_executesql @SQL, N'@Audit_Source_Import_SK INT, @Source_File_Archive_SK INT',
            @Audit_Source_Import_SK, @Source_File_Archive_SK;
        SET @RowsInserted = @@ROWCOUNT;
        PRINT CONCAT('Inserted ', @RowsInserted, ' rows from ', @SourceName, ' to Staging_Fact_Sales_Conformed');
        
    END TRY
    BEGIN CATCH
        DECLARE
            @ErrorMessage   NVARCHAR(MAX) = ERROR_MESSAGE(),
            @ErrorNumber    INT           = ERROR_NUMBER(),
            @ErrorState     INT           = ERROR_STATE(),
            @ErrorLine      INT           = ERROR_LINE(),
            @ErrorSeverity  INT           = ERROR_SEVERITY();

        EXEC ETL.SP_Log_ETL_Error
            @Procedure_Name         = @ProcName,
            @Error_Message          = @ErrorMessage,
            @Error_Number           = @ErrorNumber,
            @Error_State            = @ErrorState,
            @Error_Line             = @ErrorLine,
            @Error_Severity         = @ErrorSeverity,
            @Source_File_Archive_SK = @Source_File_Archive_SK,
            @Audit_Source_Import_SK = @Audit_Source_Import_SK;

        THROW;
    END CATCH
    RETURN @RowsInserted;
END;
GO
"""


def apply_ddl_from_run():
    """
    Execute only ODS table DDL files in DW_DDL/run/.
    ODS tables: auto-executed. Procedures & DW tables: left in run/ for manual review.
    On success: copy to archive/ with timestamp.
    """
    paths = _get_ddl_paths()
    run_dir = paths["run"]
    archive_dir = paths["archive"]
    archive_dir.mkdir(parents=True, exist_ok=True)

    if not run_dir.exists():
        print("No run folder found - skipping DDL apply")
        return

    sql_files = list(run_dir.glob("*.sql"))
    if not sql_files:
        print("No .sql files in run/ folder")
        return

    def _is_auto_executable(content: str) -> bool:
        u = content.upper()
        if "CREATE TABLE [ODS]." in u or "CREATE TABLE [ETL].[STAGING_" in u:
            return True
        if ("CREATE PROCEDURE" in u or "CREATE OR ALTER PROCEDURE" in u) and "INTO [ETL].[STAGING_FACT" in u:
            return True
        return False

    auto_files = [f for f in sql_files if _is_auto_executable(f.read_text(encoding="utf-8"))]
    other_files = [f for f in sql_files if f not in auto_files]

    if not auto_files:
        print("No ODS/staging table DDL files in run/ folder")
        return

    print(f"Applying {len(auto_files)} ODS/staging/conformed scripts (DW tables & DW merge procs need human review)...")
    if other_files:
        print(f"Note: {len(other_files)} DW/proc files left for manual review:")
        for f in other_files:
            print(f"  - {f.name}")

    conn = get_connection()
    cursor = conn.cursor()

    for sql_file in auto_files:
        try:
            sql_content = sql_file.read_text(encoding="utf-8")
            batches = []
            current_batch = []
            for line in sql_content.splitlines():
                stripped = line.strip()
                if stripped.upper() == "GO":
                    if current_batch:
                        batches.append("\n".join(current_batch))
                    current_batch = []
                else:
                    current_batch.append(line)
            if current_batch:
                batches.append("\n".join(current_batch))

            print(f"  Processing {sql_file.name} ({len(batches)} batches)")
            for i, batch in enumerate(batches, 1):
                batch = batch.strip()
                if not batch:
                    continue
                try:
                    cursor.execute(batch)
                    conn.commit()
                    print(f"    Batch {i}/{len(batches)} applied successfully")
                except Exception as batch_e:
                    conn.rollback()
                    print(f"    ERROR in batch {i} of {sql_file.name}: {str(batch_e)}")
                    raise

            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = archive_dir / f"{sql_file.stem}_{ts}{sql_file.suffix}"
            backup_path.write_text(sql_content, encoding="utf-8")
            print(f"  SUCCESS: Applied and backed up to {backup_path.name}")
            print(f"           Original file remains in run folder: {sql_file.name}")

        except Exception as e:
            conn.rollback()
            print(f"  ERROR applying {sql_file.name}: {str(e)}")
            log_audit_source_import(
                audit_id=get_next_audit_import_id(),
                source_import_sk=0,
                start_time=datetime.now(),
                end_time=datetime.now(),
                process_status="Failed",
                exception_detail=f"ODS DDL apply failed: {sql_file.name} - {str(e)}",
            )

    cursor.close()
    conn.close()
    print("ODS DDL apply complete.")


# ---------------------------------------------------------------------------
# Metadata-driven change detection (optional; process_etl_config is primary)
# ---------------------------------------------------------------------------


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
    if has_type2:
        for syscol, deftxt in [("Row_Is_Current", "BIT NOT NULL CONSTRAINT [DF_" + table + "_Row_Is_Current] DEFAULT (1)"),
                               ("Row_Effective_Datetime", "DATETIME NOT NULL CONSTRAINT [DF_" + table + "_Row_Eff] DEFAULT (GETDATE())"),
                               ("Row_Expiry_Datetime", "DATETIME NULL")]:
            lines.append(f"    IF NOT EXISTS (SELECT 1 FROM sys.columns c JOIN sys.tables t ON t.object_id = c.object_id JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE s.name = '{schema}' AND t.name = '{table}' AND c.name = '{syscol}')")
            lines.append(f"        ALTER TABLE [{schema}].[{table}] ADD [{syscol}] {deftxt};")
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
    lines.append("")
    has_soft_delete = schema == "ETL" and table == "Dim_Source_Imports"
    if has_soft_delete:
        match_row = " AND d.Row_Is_Current = 1" if has_type2 else ""
        lines.append("        -- SOFT-DELETE: mark rows no longer in staging")
        lines.append(f"        UPDATE d SET d.Is_Deleted = 1, d.Updated_Datetime = GETDATE()" + (" , d.Row_Is_Current = 0, d.Row_Expiry_Datetime = GETDATE()" if has_type2 else ""))
        lines.append(f"        FROM [{schema}].[{table}] d")
        lines.append(f"        LEFT JOIN [{ods_schema}].[{ods_tbl}] o ON {not_exists_clause}")
        lines.append(f"        WHERE o." + (pk_cols[0] if pk_cols else "1") + " IS NULL AND d.Is_Deleted = 0" + match_row + ";")
        lines.append("")
        lines.append("        -- RE-ACTIVATE: rows that reappear in staging")
        lines.append("        UPDATE d SET d.Is_Deleted = 0, d.Updated_Datetime = GETDATE()")
        lines.append(f"        FROM [{schema}].[{table}] d")
        lines.append(f"        INNER JOIN [{ods_schema}].[{ods_tbl}] o ON {not_exists_clause}")
        lines.append("        WHERE d.Is_Deleted = 1;")
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
    For each dimension source (DW + metadata), detect mapping changes.
    If changed, generate DDL and merge proc to generated/ folder.
    Returns list of source names that had changes.
    """
    dw_dims = get_config("dw_dimensions") or {}
    meta_dims = get_config("metadata_dimensions") or {}
    all_dims = dict(dw_dims, **meta_dims)
    paths = _get_ddl_paths()
    paths["generated"].mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    state = _load_state()
    changed_sources = []

    try:
        for source_name, dw_table in all_dims.items():
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
