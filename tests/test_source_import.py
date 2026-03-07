import pytest
from unittest.mock import mock_open, patch, MagicMock
from pathlib import Path
import pandas as pd

from src.staging.source_import import process_source


@pytest.fixture
def mock_source_row():
    return pd.Series({
        "Rel_Path": "raw/MDM/Places",
        "Pattern": "*.xlsx",
        "Sheet_Name": "Sheet1"
    })


@patch("src.staging.source_import.get_connection")
@patch("src.staging.source_import.pd.read_excel")
def test_process_source_truncates_ods(mock_read_excel, mock_conn, mock_source_row):
    mock_conn.return_value.cursor.return_value.fetchone.return_value = ("[ODS].[Places]", "[DW].[Dim_Places]")
    
    with patch("src.staging.source_import.truncate_table") as mock_truncate:
        process_source("Places")
        mock_truncate.assert_called_with("ODS.Places")


@patch("src.staging.source_import.pd.read_excel")
def test_deduplication_logs_duplicates(mock_read_excel):
    df = pd.DataFrame({
        "Place_Code": [1, 1, 2],
        "Place_Name": ["A", "A", "B"]
    })
    mock_read_excel.return_value = df
    
    with patch("src.staging.source_import.get_source_pk_columns", return_value=["Place_Code"]):
        with patch("src.staging.source_import.open", mock_open()) as mock_file:
            process_source("Places")
            mock_file.write.assert_called()  # should have written duplicates


@pytest.mark.skip(reason="Requires real BCP - integration")
def test_bcp_called_correctly():
    pass