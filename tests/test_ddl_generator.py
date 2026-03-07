from pathlib import Path

import pytest
from unittest.mock import patch

from src.utils.ddl_generator import generate_bcp_format_file


@patch("src.utils.ddl_generator.get_connection")
def test_generate_bcp_format_file_produces_valid_content(mock_conn):
    mock_cursor = mock_conn.return_value.cursor.return_value
    mock_cursor.fetchall.return_value = [
        ("Place_Code", "VARCHAR(50)"),
        ("Place_Name", "VARCHAR(200)"),
        ("Is_Active", "BIT")
    ]

    fake_path = Path("fake.fmt")
    generate_bcp_format_file("Places", fake_path)

    with open(fake_path, "r") as f:
        content = f.read()
        assert "14.0" in content
        assert "SQLCHAR" in content
        assert "Inserted_Datetime" in content
        assert '""' in content  # empty collation for bit + datetime