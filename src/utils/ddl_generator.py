from datetime import datetime
from pathlib import Path

from src.config import BASE_PATH, SYSTEM_BASE_PATH
from src.utils.db_ops import execute_proc, get_connection, get_next_audit_import_id, log_audit_source_import

def generate_ods_table_ddl(source_name: str, columns: list[dict]) -> str:
    column_defs = []
    pk_cols = []  # Only Is_PK columns for PK

    for col in columns:
        col_name = col['Target_Column'].strip()
        nullability = "NOT NULL" if col.get('Is_Required', False) else "NULL"
        column_defs.append(f"    [{col_name}] {col['Data_Type']} {nullability}")
        
        if col.get('Is_PK', False):
            pk_cols.append(f"[{col_name}]")

    pk_clause = ""
    if pk_cols:
        pk_clause = f", CONSTRAINT PK_{source_name} PRIMARY KEY ({', '.join(pk_cols)})"

    ddl = f"""IF OBJECT_ID('ODS.{source_name}', 'U') IS NOT NULL
DROP TABLE [ODS].[{source_name}];
GO

CREATE TABLE [ODS].[{source_name}] (
{',\n'.join(column_defs)},
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
{pk_clause}
);
GO"""
    return ddl

def generate_dw_table_ddl(schema: str, table_name: str, columns: list[dict], timestamp: str) -> str:
    sk_col = f"{table_name.replace('Dim_', '')}_SK"
    
    column_defs = [f"    [{sk_col}] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_{table_name}] PRIMARY KEY"]
    
    has_scd2 = any(c.get('Is_Type2_Attribute', False) for c in columns)
    if has_scd2:
        column_defs.extend([
            "    [Row_Is_Current] BIT NOT NULL DEFAULT 1",
            "    [Row_Effective_Datetime] DATETIME NOT NULL DEFAULT GETDATE()",
            "    [Row_Expiry_Datetime] DATETIME NULL"
        ])
    
    column_defs.append("    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()")
    column_defs.append("    [Updated_Datetime] DATETIME NULL")
    column_defs.append("    [Is_Deleted] BIT NOT NULL DEFAULT 0")
    
    for col in columns:
        col_name = col['Target_Column'].strip()
        nullability = "NOT NULL" if col.get('Is_Required', False) else "NULL"
        column_defs.append(f"    [{col_name}] {col['Data_Type']} {nullability}")
    
    nk_cols = [c['Target_Column'].strip() for c in columns if c.get('Is_PK', False)]
    nk_clause = f"CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_{table_name}_Active] ON [{schema}].[{table_name}] ({', '.join([f'[{col}]' for col in nk_cols])}) WHERE [Row_Is_Current] = 1 AND [Is_Deleted] = 0;" if nk_cols else ""

    common_cols = [c['Target_Column'].strip() for c in columns] + ["Inserted_Datetime", "Updated_Datetime", "Is_Deleted"]
    if has_scd2:
        common_cols += ["Row_Is_Current", "Row_Effective_Datetime", "Row_Expiry_Datetime"]
    insert_list = ', '.join([f'[{col}]' for col in common_cols])
    
    # Drop old PK constraint before rename (safe)
    drop_pk = f"IF OBJECT_ID('PK_{table_name}', 'PK') IS NOT NULL ALTER TABLE [{schema}].[{table_name}] DROP CONSTRAINT PK_{table_name};"

    ddl = f"""IF OBJECT_ID('{schema}.{table_name}', 'U') IS NOT NULL
BEGIN
    {drop_pk}
    EXEC sp_rename '{schema}.{table_name}', '{table_name}_backup_{timestamp}';
    CREATE TABLE [{schema}].[{table_name}] (
{',\n'.join(column_defs)}
    );
    INSERT INTO [{schema}].[{table_name}] ({insert_list})
    SELECT {insert_list} FROM [{schema}].[{table_name}_backup_{timestamp}];
END
ELSE
BEGIN
    CREATE TABLE [{schema}].[{table_name}] (
{',\n'.join(column_defs)}
    );
END

{nk_clause}"""
    return ddl

def generate_merge_proc_ddl(source_name: str, staging_table: str, columns: list[dict]) -> str:
    is_scd2 = any(c.get('Is_Type2_Attribute', False) for c in columns)
    pattern = 'scd_type2' if is_scd2 else 'scd_type1'
    
    template_path = BASE_PATH() / "sp_templates" / f"{pattern}.template.sql"
    if not template_path.exists():
        raise FileNotFoundError(f"Template missing: {template_path}")
    
    template = template_path.read_text(encoding="utf-8")

    key_column = next((c['Target_Column'] for c in columns if c.get('Is_PK', False)), 'Source_Name')
    
    type1_cols = [c for c in columns if not c.get('Is_Type2_Attribute', False) and not c.get('Is_PK', False)]
    update_columns = ', '.join([f"d.[{c['Target_Column']}] = o.[{c['Target_Column']}]" for c in type1_cols])
    type1_where_changes = ' OR '.join([f"COALESCE(d.[{c['Target_Column']}], '') <> COALESCE(o.[{c['Target_Column']}], '')" for c in type1_cols]) or "1=0"
    
    type2_cols = [c for c in columns if c.get('Is_Type2_Attribute', False)]
    type2_where_changes = ' OR '.join([f"COALESCE(d.[{c['Target_Column']}], '') <> COALESCE(o.[{c['Target_Column']}], '')" for c in type2_cols]) or "1=0"
    
    insert_columns = ', '.join([f"[{c['Target_Column']}]" for c in columns])
    select_columns = ', '.join([f"o.[{c['Target_Column']}]" for c in columns])
    join_condition = f"d.[{key_column}] = o.[{key_column}]"

    ddl = template.format(
        dim_name=source_name,
        staging_table=staging_table,
        generated_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        update_columns=update_columns,
        type1_update_columns=update_columns,
        type1_where_changes=type1_where_changes,
        type2_where_changes=type2_where_changes,
        insert_columns=insert_columns,
        select_columns=select_columns,
        join_condition=join_condition,
        key_column=key_column
    )
    return ddl

def apply_ddl_from_run():
    """
    Executes all .sql files in DW_DDL/run/ folder.
    On success: moves to archive/ with timestamp.
    On failure: leaves in run/, logs error, continues with next file.
    """
    base_path = SYSTEM_BASE_PATH()
    run_dir = base_path / "DW_DDL" / "run"
    archive_dir = base_path / "DW_DDL" / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    if not run_dir.exists():
        print("No run folder found - skipping DDL apply")
        return

    sql_files = list(run_dir.glob("*.sql"))
    if not sql_files:
        print("No .sql files in run/ folder")
        return

    print(f"Applying {len(sql_files)} DDL scripts from run/ folder...")

    conn = get_connection()
    cursor = conn.cursor()

    for sql_file in sql_files:
        try:
            sql = sql_file.read_text(encoding="utf-8")
            cursor.execute(sql)
            conn.commit()
            print(f"  SUCCESS: Applied {sql_file.name}")

            # Archive with timestamp
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_path = archive_dir / f"{sql_file.stem}_{ts}{sql_file.suffix}"
            sql_file.rename(archive_path)
            print(f"  Archived to: {archive_path.name}")

        except Exception as e:
            conn.rollback()
            print(f"  ERROR applying {sql_file.name}: {str(e)}")
            # Leave file in run/ for fix & retry
            log_audit_source_import(
                audit_id=get_next_audit_import_id(),
                source_import_sk=0,
                start_time=datetime.now(),
                end_time=datetime.now(),
                process_status='Failed',
                exception_detail=f"DDL apply failed: {sql_file.name} - {str(e)}"
            )

    cursor.close()
    conn.close()
    print("DDL apply complete.")