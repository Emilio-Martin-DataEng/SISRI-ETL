import pytest
from src.utils.ddl_generator import generate_ods_table_ddl, generate_dw_table_ddl, generate_merge_proc_ddl

def test_ods_table_composite_pk_only_is_pk():
    columns = [
        {"Target_Column": "Code", "Data_Type": "VARCHAR(50)", "Is_PK": True},
        {"Target_Column": "Name", "Data_Type": "VARCHAR(100)", "Is_PK": False},
        {"Target_Column": "Description", "Data_Type": "VARCHAR(200)", "Is_PK": True}
    ]
    ddl = generate_ods_table_ddl("TestODS", columns)
    assert "CONSTRAINT PK_TestODS PRIMARY KEY ([Code], [Description])" in ddl
    assert "[Name]" in ddl  # non-PK column exists but not in PK

def test_dw_table_always_has_row_change_reason():
    columns = [{"Target_Column": "Name", "Data_Type": "VARCHAR(100)", "Is_PK": False}]
    ddl = generate_dw_table_ddl("DW", "Dim_Test", columns, "20260304")
    assert "[Row_Change_Reason] VARCHAR(50) NULL" in ddl

def test_merge_scd1_update_block_present_when_type1_columns():
    columns = [
        {"Target_Column": "ID", "Data_Type": "INT", "Is_PK": True},
        {"Target_Column": "Name", "Data_Type": "VARCHAR(100)", "Is_Type2_Attribute": False}
    ]
    merge_ddl = generate_merge_proc_ddl("Test", "[ODS].[Test]", "[DW].[Dim_Test]", columns)
    assert "-- Type 1: UPDATE changed attributes" in merge_ddl
    assert "d.[Name] = o.[Name]" in merge_ddl
    assert "WHERE (COALESCE(d.[Name], '') <> COALESCE(o.[Name], ''))" in merge_ddl

def test_merge_scd1_no_update_block_when_no_type1_columns():
    columns = [
        {"Target_Column": "ID", "Data_Type": "INT", "Is_PK": True}
    ]
    merge_ddl = generate_merge_proc_ddl("Test", "[ODS].[Test]", "[DW].[Dim_Test]", columns)
    assert "No Type 1 columns to update" in merge_ddl
    assert "SET @UpdatedCount = 0;" in merge_ddl
    assert "UPDATE d SET" not in merge_ddl  # no broken comma

def test_merge_scd2_has_both_type1_and_type2_logic():
    columns = [
        {"Target_Column": "ID", "Data_Type": "INT", "Is_PK": True},
        {"Target_Column": "Status", "Data_Type": "VARCHAR(50)", "Is_Type2_Attribute": True},
        {"Target_Column": "Name", "Data_Type": "VARCHAR(100)", "Is_Type2_Attribute": False}
    ]
    merge_ddl = generate_merge_proc_ddl("Test", "[ODS].[Test]", "[DW].[Dim_Test]", columns)
    assert "-- Type 1: UPDATE changed attributes" in merge_ddl
    assert "d.[Name] = o.[Name]" in merge_ddl
    assert "Row_Is_Current = 0" in merge_ddl
    assert "Row_Expiry_Datetime = GETDATE()" in merge_ddl

def test_no_trailing_whitespace_in_column_names():
    columns = [
        {"Target_Column": "City ", "Data_Type": "VARCHAR(100)"},  # simulate dirty input
        {"Target_Column": "Name", "Data_Type": "VARCHAR(100)"}
    ]
    merge_ddl = generate_merge_proc_ddl("Test", "[ODS].[Test]", "[DW].[Dim_Test]", columns)
    assert "[City ]" not in merge_ddl
    assert "[City]" in merge_ddl