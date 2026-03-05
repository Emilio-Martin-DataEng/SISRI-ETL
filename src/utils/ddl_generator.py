# src/utils/ddl_generator.py

from datetime import datetime
from pathlib import Path

from src.config import SYSTEM_BASE_PATH
from src.utils.db_ops import execute_proc, get_connection

def generate_ods_table_ddl(source_name: str, columns: list[dict]) -> str:
    column_defs = []
    pk_cols = []

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
    template_path = SYSTEM_BASE_PATH() / "sp_templates" / "dw_dim_tables_regeneration.template.sql"
    if not template_path.exists():
        raise FileNotFoundError(f"Template missing: {template_path}")
    
    template = template_path.read_text(encoding="utf-8")

    sk_col = f"{table_name.replace('Dim_', '')}_SK"
    
    column_defs_list = []
    insert_columns_list = []
    for col in columns:
        col_name = col['Target_Column'].strip()
        nullability = "NOT NULL" if col.get('Is_Required', False) else "NULL"
        column_defs_list.append(f", [{col_name}] {col['Data_Type']} {nullability}")
        insert_columns_list.append(f"[{col_name}]")
    
    column_defs = '\n'.join(column_defs_list)
    insert_columns = ', '.join(insert_columns_list)
    select_columns = insert_columns

    nk_cols = [c['Target_Column'].strip() for c in columns if c.get('Is_PK', False)]
    nk_where = "WHERE [Is_Deleted] = 0"
    if any(c.get('Is_Type2_Attribute', False) for c in columns):
        nk_where = "WHERE [Row_Is_Current] = 1 AND [Is_Deleted] = 0"
    unique_index_clause = f"CREATE UNIQUE NONCLUSTERED INDEX [UIX_NK_{table_name}_Active] ON [{schema}].[{table_name}] ({', '.join([f'[{col}]' for col in nk_cols])}) {nk_where};" if nk_cols else ""

    ddl = template.format(
        schema=schema,
        table_name=table_name,
        sk_col=sk_col,
        generated_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        timestamp=timestamp,
        column_defs=column_defs,
        insert_columns=insert_columns,
        select_columns=select_columns,
        unique_index_clause=unique_index_clause
    )
    return ddl

def generate_merge_proc_ddl(source_name: str, staging_table: str, dw_table: str, columns: list[dict]) -> str:
    is_scd2 = any(c.get('Is_Type2_Attribute', 0) == 1 for c in columns)
    pattern = 'scd_type2' if is_scd2 else 'scd_type1'
    
    template_path = SYSTEM_BASE_PATH() / "sp_templates" / f"{pattern}.template.sql"
    if not template_path.exists():
        raise FileNotFoundError(f"Template missing: {template_path}")
    
    template = template_path.read_text(encoding="utf-8")

    key_column = next((c['Target_Column'] for c in columns if c.get('Is_PK', False)), 'Source_Name')
    for c in columns:
        if 'Is_Type2_Attribute' in c:
            c['Is_Type2_Attribute'] = int(c['Is_Type2_Attribute']) if c['Is_Type2_Attribute'] else 0
        if 'Is_PK' in c:
            c['Is_PK'] = int(c['Is_PK']) if c['Is_PK'] else 0

    type1_cols = [c for c in columns if c.get('Is_Type2_Attribute', 0) != 1 and c.get('Is_PK', 0) != 1]
    update_columns = ', '.join([f"d.[{c['Target_Column']}] = o.[{c['Target_Column']}]" for c in type1_cols])
    type1_where_changes = ' OR '.join([f"COALESCE(d.[{c['Target_Column']}], '') <> COALESCE(o.[{c['Target_Column']}], '')" for c in type1_cols]) or "1=0"
    
    type2_cols = [c for c in columns if c.get('Is_Type2_Attribute', False)]
    type2_where_changes = ' OR '.join([f"COALESCE(d.[{c['Target_Column']}], '') <> COALESCE(o.[{c['Target_Column']}], '')" for c in type2_cols]) or "1=0"
    
    insert_columns = ', '.join([f"[{c['Target_Column']}]" for c in columns])
    select_columns = ', '.join([f"o.[{c['Target_Column']}]" for c in columns])
    join_condition = f"d.[{key_column}] = o.[{key_column}]"

    # Conditional Type 1 UPDATE block
    type1_update_block = ""
    if type1_cols:
        type1_update_block = f"""-- Type 1: UPDATE changed attributes
UPDATE d SET
    {update_columns},
    d.Updated_Datetime = GETDATE()
FROM {dw_table} d
INNER JOIN {staging_table} o ON {join_condition}
WHERE ({type1_where_changes});
SET @UpdatedCount = @@ROWCOUNT;
"""
    else:
        type1_update_block = """-- No Type 1 columns to update
SET @UpdatedCount = 0;
"""

    ddl = template.format(
        dim_name=source_name,
        dw_table=dw_table,
        ods_table=staging_table,
        generated_time=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        type1_update_block=type1_update_block,
        type2_where_changes=type2_where_changes,
        insert_columns=insert_columns,
        select_columns=select_columns,
        join_condition=join_condition,
        key_column=key_column
    )
    return ddl

def apply_ddl_from_run():
    base_path = SYSTEM_BASE_PATH()
    run_dir = base_path / "DW_DDL" / "run"
    archive_dir = base_path / "DW_DDL" / "archive"
    archive_dir.mkdir(parents=True, exist_ok=True)

    sql_files = list(run_dir.glob("*.sql"))
    if not sql_files:
        print("No .sql files in run/ folder - nothing to apply")
        return

    print(f"Applying {len(sql_files)} DDL scripts from run/...")

    conn = get_connection()
    cursor = conn.cursor()

    for sql_file in sql_files:
        try:
            sql_content = sql_file.read_text(encoding="utf-8")
            
            # Split by GO (case-insensitive, handle variations)
            batches = [batch.strip() for batch in sql_content.split('GO') if batch.strip()]
            
            print(f"  Processing {sql_file.name} ({len(batches)} batches)")
            
            for i, batch in enumerate(batches, 1):
                if batch:
                    try:
                        cursor.execute(batch)
                        conn.commit()
                        print(f"    Batch {i}/{len(batches)} applied successfully")
                    except Exception as batch_e:
                        conn.rollback()
                        print(f"    ERROR in batch {i} of {sql_file.name}: {str(batch_e)}")
                        raise  # Stop on error, leave file in run/
            
            # Archive on full success
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_path = archive_dir / f"{sql_file.stem}_{ts}{sql_file.suffix}"
            sql_file.rename(archive_path)
            print(f"  SUCCESS: Applied and archived to {archive_path.name}")

        except Exception as e:
            conn.rollback()
            print(f"  ERROR applying {sql_file.name}: {str(e)}")
            # Leave in run/ for fix & retry

    cursor.close()
    conn.close()
    print("DDL apply complete.")