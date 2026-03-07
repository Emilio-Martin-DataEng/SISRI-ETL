import pytest
from unittest.mock import patch

from src.etl_orchestrator import run_etl


@patch("src.etl_orchestrator.process_source")
@patch("src.etl_orchestrator.get_connection")
def test_orchestrator_only_runs_business_sources(mock_conn, mock_process):
    mock_cursor = mock_conn.return_value.cursor.return_value
    mock_cursor.fetchall.return_value = [("Places",), ("Products",)]

    run_etl()

    assert mock_process.call_count == 2
    mock_process.assert_any_call("Places", ...)
    mock_process.assert_any_call("Products", ...)