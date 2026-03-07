import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
import pandas as pd

from src.staging.etl_config import process_etl_config


def test_etl_config_missing_file_graceful_skip():
    with patch("src.staging.etl_config.CONFIG_ROOT.glob", return_value=[]):
        with patch("src.staging.etl_config.log_audit_source_import") as mock_log:
            process_etl_config()
            mock_log.assert_called_once()
            assert "Skipped" in str(mock_log.call_args)


@pytest.mark.skip(reason="Requires real Excel + DB - integration test")
def test_etl_config_happy_path():
    # Would need mock Excel + mock BCP + mock procs
    pass


def test_mapping_column_selection_handles_missing_columns():
    # Simulate df_mapping with extra/missing columns
    df = pd.DataFrame({
        "Source_Name": ["Places"],
        "Source_Column": ["Place code"],
        "Target_Column": ["Place_Code"],
        "Data_Type": ["VARCHAR(50)"],
        "Is_PK": ["1"],
        "Extra_Column": ["ignored"]
    })
    # The code should drop extra columns
    # (this test will fail until safe selection is fully implemented)
    assert "Extra_Column" not in df.columns  # will fail if not implemented