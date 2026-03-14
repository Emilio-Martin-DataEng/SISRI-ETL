"""
Check that mapping metadata is complete and valid for ETL.
Validates Dim_Source_Imports, Dim_Source_Imports_Mapping, Dim_DW_Mapping_And_Transformations.
Reports issues only - does not fix. Admin must correct config spreadsheet and re-run metadata refresh.
"""
from src.utils.db_ops import get_connection

VALID_DATE_FORMATS = frozenset({"dd-MM-yyyy", "yyyy/MM/dd", "yyyy-MM-dd", "MM/dd/yyyy", "dd/MM/yyyy"})


def check_source_imports(cursor) -> list[str]:
    """Validate Dim_Source_Imports. Return list of issues."""
    issues = []
    cursor.execute("""
        SELECT Source_Name, Source_Type, Rel_Path, Staging_Table, DW_Table_Name, Merge_Proc_Name, Is_Active
        FROM ETL.Dim_Source_Imports
        WHERE Is_Deleted = 0 AND Source_Type NOT IN ('System', 'Fact_Conformed')
    """)
    for row in cursor.fetchall():
        src, stype, rel, staging, dw, merge, active = row
        if not src or not str(src).strip():
            issues.append("Source_Imports: Source_Name is empty")
        if not rel or not str(rel).strip():
            issues.append(f"Source_Imports [{src}]: Rel_Path is empty")
        if not staging or not str(staging).strip():
            issues.append(f"Source_Imports [{src}]: Staging_Table is empty")
        if not stype or not str(stype).strip():
            issues.append(f"Source_Imports [{src}]: Source_Type is empty")
        if stype == "Fact_Sales":
            if not dw or not str(dw).strip():
                issues.append(f"Source_Imports [{src}]: DW_Table_Name required for Fact_Sales")
            if not merge or not str(merge).strip():
                issues.append(f"Source_Imports [{src}]: Merge_Proc_Name required for Fact_Sales")
    return issues


def check_source_file_mapping(cursor) -> list[str]:
    """Validate Dim_Source_Imports_Mapping. Return list of issues."""
    issues = []
    cursor.execute("""
        SELECT Source_Name, COUNT(*) as Cnt
        FROM ETL.Dim_Source_Imports_Mapping
        WHERE Is_Deleted = 0
        GROUP BY Source_Name
    """)
    for row in cursor.fetchall():
        src, cnt = row
        if cnt == 0:
            issues.append(f"Source_File_Mapping: No rows for source [{src}]")
    cursor.execute("""
        SELECT Source_Name FROM ETL.Dim_Source_Imports
        WHERE Is_Deleted = 0 AND Source_Type = 'Fact_Sales'
    """)
    fact_sources = [r[0] for r in cursor.fetchall()]
    cursor.execute("""
        SELECT DISTINCT Source_Name FROM ETL.Dim_Source_Imports_Mapping WHERE Is_Deleted = 0
    """)
    mapped_sources = {r[0] for r in cursor.fetchall()}
    for src in fact_sources:
        if src not in mapped_sources:
            issues.append(f"Source_File_Mapping: Fact_Sales [{src}] has no column mapping")
    return issues


def check_dw_mapping_and_transformations(cursor) -> list[str]:
    """Validate Dim_DW_Mapping_And_Transformations for Fact_Sales sources."""
    issues = []
    cursor.execute("""
        SELECT Source_Name FROM ETL.Dim_Source_Imports
        WHERE Is_Deleted = 0 AND Source_Type = 'Fact_Sales'
    """)
    fact_sources = [r[0] for r in cursor.fetchall()]

    for src in fact_sources:
        cursor.execute("""
            SELECT ODS_Column, Conformed_Column, Transformation_Type, Transformation_Rule, Is_Key
            FROM ETL.Dim_DW_Mapping_And_Transformations
            WHERE Source_Name = ? AND Is_Deleted = 0
        """, (src,))
        rows = cursor.fetchall()
        if not rows:
            issues.append(f"DW_Mapping [{src}]: No conformed mapping rows")
            continue

        has_key = any(r[4] for r in rows)
        if not has_key:
            issues.append(f"DW_Mapping [{src}]: No Is_Key=1 row (required for conformed merge)")

        for r in rows:
            ods_col, conf_col, trans_type, trans_rule, is_key = r
            if not ods_col or not str(ods_col).strip():
                issues.append(f"DW_Mapping [{src}]: ODS_Column is empty")
            if not conf_col or not str(conf_col).strip():
                issues.append(f"DW_Mapping [{src}]: Conformed_Column is empty")
            if not trans_type or not str(trans_type).strip():
                issues.append(f"DW_Mapping [{src}]: Transformation_Type is empty")

            if trans_type == "Date_SK":
                rule = (trans_rule or "").strip()
                if not rule:
                    issues.append(f"DW_Mapping [{src}] Date_SK: Transformation_Rule is empty")
                elif rule not in VALID_DATE_FORMATS:
                    issues.append(
                        f"DW_Mapping [{src}] Date_SK: Transformation_Rule '{rule}' not in "
                        f"supported formats: {sorted(VALID_DATE_FORMATS)}"
                    )

    return issues


def run_check_mapping(cursor=None) -> tuple[bool, list[str]]:
    """
    Run all mapping validations. Returns (success, issues).
    If cursor is provided, uses it (caller manages connection). Otherwise creates own connection.
    """
    issues = []
    own_conn = False
    if cursor is None:
        conn = get_connection()
        cursor = conn.cursor()
        own_conn = True
    try:
        issues.extend(check_source_imports(cursor))
        issues.extend(check_source_file_mapping(cursor))
        issues.extend(check_dw_mapping_and_transformations(cursor))
    except Exception as e:
        issues.append(f"Cannot run check: {e}")
    finally:
        if own_conn:
            cursor.close()
            conn.close()
    return (len(issues) == 0, issues)
