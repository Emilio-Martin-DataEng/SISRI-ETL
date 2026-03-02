# src/utils/ddl_generator.py
from datetime import datetime
from pathlib import Path

from src.config import BASE_PATH
from src.utils.db_ops import execute_proc

def generate_ods_table_ddl(source_name: str, columns: list[dict]) -> str:
    is_scd2 = any(c.get('Is_Type2_Attribute', False) for c in columns)
    
    pk_cols = [c['Target_Column'] for c in columns if c.get('Is_PK', False)]
    pk_clause = f", CONSTRAINT PK_{source_name} PRIMARY KEY ({', '.join([f'[{col}]' for col in pk_cols])})" if pk_cols else ""
    
    column_defs = []
    for col in columns:
        nullability = "NOT NULL" if col.get('Is_Required', False) else "NULL"
        column_defs.append(f"    [{col['Target_Column']}] {col['Data_Type']} {nullability}")
    
    if is_scd2:
        column_defs.extend([
            "    [Row_Is_Current] BIT NOT NULL DEFAULT 1",
            "    [Row_Effective_Datetime] DATETIME NOT NULL DEFAULT GETDATE()",
            "    [Row_Expiry_Datetime] DATETIME NULL"
        ])
    
    ddl = """IF OBJECT_ID('ODS.{0}', 'U') IS NULL
CREATE TABLE [ODS].[{0}] (
{1},
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
{2}
);""".format(source_name, ',\n'.join(column_defs), pk_clause)
    return ddl

def generate_dw_table_ddl(schema: str, table_name: str, columns: list[dict], timestamp: str) -> str:
    column_defs = []
    for col in columns:
        nullability = "NOT NULL" if col.get('Is_Required', False) else "NULL"
        column_defs.append(f"    [{col['Target_Column']}] {col['Data_Type']} {nullability}")
    
    ddl = """IF OBJECT_ID('{0}.{1}', 'U') IS NOT NULL
BEGIN
    EXEC sp_rename '{0}.{1}', '{1}_backup_{2}';
    -- Create new table
    CREATE TABLE [{0}].[{1}] (
{3},
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
    );
    -- Insert backed up data
    INSERT INTO [{0}].[{1}] SELECT * FROM [{0}].[{1}_backup_{2}];
END
ELSE
BEGIN
    CREATE TABLE [{0}].[{1}] (
{3},
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE()
    );
END""".format(schema, table_name, timestamp, ',\n'.join(column_defs))
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

def apply_ddl_from_run(base_path: Path):
    run_dir = base_path / "config" / "DW_DDL" / "run"
    for sql_file in run_dir.glob("*.sql"):
        sql = sql_file.read_text(encoding="utf-8")
        execute_proc("sp_executesql", f"@stmt = N'{sql.replace("'", "''")}'")
        print(f"Applied DDL from {sql_file}")
        sql_file.unlink()