import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from src.config import get_config
from src.utils.db import upload_via_bcp
from src.utils.db_ops import get_connection, truncate_table, execute_proc

import sys

# Add project root to sys.path so pytest can find 'src'
PROJECT_ROOT = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(PROJECT_ROOT))

@pytest.fixture
def mock_db_config():
    return {
        "server": "localhost",
        "database": "SISRI_TEST",
        "username": "test_user",
        "password": "test_pass"
    }


@pytest.fixture
def mock_connection(mock_db_config):
    with patch("src.utils.db_ops.pyodbc.connect") as mock_connect:
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        yield mock_conn, mock_cursor


@pytest.fixture
def temp_dir(tmp_path):
    """Temporary directory for test files"""
    return tmp_path


@pytest.fixture
def mock_excel_data():
    """Tiny mock data for testing"""
    imports_data = {
        "Source_Name": ["Places"],
        "Rel_Path": ["raw/MDM/Places"],
        "Pattern": ["*.xlsx"],
        "Sheet_Name": ["Sheet1"],
        "Staging_Table": ["[ODS].[Places]"],
        "Processing_Order": ["1.4"],
        "Is_Active": ["1"],
        "DW_Table_Name": ["[DW].[Dim_Places]"],
        "Merge_Proc_Name": ["[ETL].[SP_Merge_Dim_Places]"],
        "Source_Type": ["Dimension"],
        "Is_Conformed_Target": ["0"],
    }
    mapping_data = {
        "Source_Name": ["Places"] * 3,
        "Source_Column": ["Place code", "Place Name", "Inserted_Datetime"],
        "Target_Column": ["Place_Code", "Place_Name", "Inserted_Datetime"],
        "Data_Type": ["VARCHAR(50)", "VARCHAR(200)", "DATETIME"],
        "Is_PK": ["1", "0", "0"],
        "Is_Type2_Attribute": ["0", "0", "0"],
        "Is_Required": ["1", "1", "0"]
    }
    return imports_data, mapping_data