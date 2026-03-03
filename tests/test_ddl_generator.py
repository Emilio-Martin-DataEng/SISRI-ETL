import pytest
from src.utils.ddl_generator import generate_ods_table_ddl, generate_dw_table_ddl, generate_merge_proc_ddl

def normalize_sql(sql: str) -> str:
    """Remove extra whitespace and line breaks for easier comparison."""
    return ' '.join(sql.split()).replace(' ,', ',').replace(', ', ',').strip()

def test_dw_table_scd2():
    columns = [
        {"Target_Column": "Principal_ID", "Data_Type": "INT", "Is_PK": True, "Is_Required": True, "Is_Type2_Attribute": False},
        {"Target_Column": "Status", "Data_Type": "VARCHAR(50)", "Is_PK": False, "Is_Required": False, "Is_Type2_Attribute": True}
    ]
    timestamp = "20260302"
    ddl = generate_dw_table_ddl("DW", "Dim_Principals", columns, timestamp)
    norm_ddl = normalize_sql(ddl)

    # Check SK (ignore name variation)
    assert "IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Principals] PRIMARY KEY" in norm_ddl
    assert "[Row_Is_Current] BIT NOT NULL DEFAULT 1" in norm_ddl
    assert "[Row_Effective_Datetime] DATETIME NOT NULL DEFAULT GETDATE()" in norm_ddl
    assert "[Row_Expiry_Datetime] DATETIME NULL" in norm_ddl
    assert "[Is_Deleted] BIT NOT NULL DEFAULT 0" in norm_ddl
    assert "INSERT INTO [DW].[Dim_Principals]" in norm_ddl  # explicit insert

def test_merge_scd1():
    columns = [
        {"Target_Column": "Principal_ID", "Data_Type": "INT", "Is_PK": True, "Is_Type2_Attribute": False},
        {"Target_Column": "Name", "Data_Type": "VARCHAR(100)", "Is_Type2_Attribute": False}
    ]
    merge_ddl = generate_merge_proc_ddl("Principals", "ODS.Principals", columns)
    norm_merge = normalize_sql(merge_ddl)

    assert "UPDATE d SET" in norm_merge
    assert "d.[Name]=o.[Name]" in norm_merge.replace(" ", "")  # ignore spaces
    assert "COALESCE(d.[Name],'')<>COALESCE(o.[Name],'')" in norm_merge.replace(" ", "")
    assert "Row_Is_Current" not in norm_merge  # no SCD2

def test_merge_scd2():
    columns = [
        {"Target_Column": "Principal_ID", "Data_Type": "INT", "Is_PK": True, "Is_Type2_Attribute": False},
        {"Target_Column": "Status", "Data_Type": "VARCHAR(50)", "Is_Type2_Attribute": True}
    ]
    merge_ddl = generate_merge_proc_ddl("Principals", "ODS.Principals", columns)
    norm_merge = normalize_sql(merge_ddl)

    assert "UPDATE d SET" in norm_merge
    assert "Row_Is_Current=0" in norm_merge.replace(" ", "")
    assert "Row_Expiry_Datetime=GETDATE()" in norm_merge.replace(" ", "")
    assert "COALESCE(d.[Status],'')<>COALESCE(o.[Status],'')" in norm_merge.replace(" ", "")

def test_ods_table_no_scd2():
    columns = [
        {"Target_Column": "Principal_ID", "Data_Type": "INT", "Is_PK": True, "Is_Required": True, "Is_Type2_Attribute": False},
        {"Target_Column": "Name", "Data_Type": "VARCHAR(100)", "Is_PK": False, "Is_Required": False, "Is_Type2_Attribute": False}
    ]
    ddl = generate_ods_table_ddl("Principals", columns)
    assert "[Principal_ID] INT NOT NULL" in ddl
    assert "[Name] VARCHAR(100) NULL" in ddl
    assert "Row_Is_Current" not in ddl  # no SCD2
    assert "CONSTRAINT PK_Principals PRIMARY KEY" in ddl
