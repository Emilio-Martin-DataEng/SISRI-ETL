import pytest
from src.utils.ddl_generator import generate_ods_table_ddl, generate_dw_table_ddl, generate_merge_proc_ddl

def test_trim_field_names():
    columns = [{'Target_Column': '  TestCol  ', 'Data_Type': 'VARCHAR(50)', 'Is_Required': True}]
    ddl = generate_ods_table_ddl('Test', columns)
    assert '[TestCol]' in ddl  # trimmed

def test_scd2_detection():
    columns = [{'Target_Column': 'Col1', 'Is_Type2_Attribute': True}]
    ddl = generate_merge_proc_ddl('Test', 'ODS.Test', columns)
    assert 'Row_Is_Current' in ddl  # SCD2 columns present

def test_backup_insert_uses_columns():
    ddl = generate_dw_table_ddl('DW', 'Dim_Test', [{'Target_Column': 'Col1', 'Data_Type': 'VARCHAR(50)'}], '20260302')
    assert 'SELECT * FROM' not in ddl  # no *
    assert 'Col1' in ddl  # explicit columns