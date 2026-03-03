import pytest
from src.utils.ddl_generator import generate_ods_table_ddl, generate_dw_table_ddl, generate_merge_proc_ddl

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

def test_dw_table_scd2():
    columns = [
        {"Target_Column": "Principal_ID", "Data_Type": "INT", "Is_PK": True, "Is_Required": True, "Is_Type2_Attribute": False},
        {"Target_Column": "Status", "Data_Type": "VARCHAR(50)", "Is_PK": False, "Is_Required": False, "Is_Type2_Attribute": True}
    ]
    timestamp = "20260302"
    ddl = generate_dw_table_ddl("DW", "Dim_Principals", columns, timestamp)
    assert " [Principal_SK] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_Dim_Principals] PRIMARY KEY" in ddl
    assert "[Row_Is_Current] BIT NOT NULL DEFAULT 1" in ddl
    assert "[Row_Effective_Datetime] DATETIME NOT NULL DEFAULT GETDATE()" in ddl
    assert "[Row_Expiry_Datetime] DATETIME NULL" in ddl
    assert "SELECT [Principal_ID], [Status], [Inserted_Datetime], [Updated_Datetime] FROM" in ddl  # explicit columns

def test_merge_scd1():
    columns = [
        {"Target_Column": "Principal_ID", "Data_Type": "INT", "Is_PK": True, "Is_Type2_Attribute": False},
        {"Target_Column": "Name", "Data_Type": "VARCHAR(100)", "Is_Type2_Attribute": False}
    ]
    merge_ddl = generate_merge_proc_ddl("Principals", "ODS.Principals", columns)
    assert "UPDATE d SET d.[Name] = o.[Name]" in merge_ddl
    assert "WHERE (COALESCE(d.[Name], '') <> COALESCE(o.[Name], ''))" in merge_ddl
    assert "Row_Is_Current" not in merge_ddl  # no SCD2

def test_merge_scd2():
    columns = [
        {"Target_Column": "Principal_ID", "Data_Type": "INT", "Is_PK": True, "Is_Type2_Attribute": False},
        {"Target_Column": "Status", "Data_Type": "VARCHAR(50)", "Is_Type2_Attribute": True}
    ]
    merge_ddl = generate_merge_proc_ddl("Principals", "ODS.Principals", columns)
    assert "UPDATE d SET d.Row_Is_Current = 0, d.Row_Expiry_Datetime = GETDATE()" in merge_ddl
    assert "WHERE (COALESCE(d.[Status], '') <> COALESCE(o.[Status], ''))" in merge_ddl