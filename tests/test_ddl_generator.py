from pathlib import Path

import pytest
from unittest.mock import patch

from src.utils.db_ops import generate_bcp_format_file


@patch("src.utils.db_ops.get_connection")
def test_generate_bcp_format_file_produces_valid_content(mock_conn):
    mock_cursor = mock_conn.return_value.cursor.return_value
    # (Ordinal_Position, Source_Column, Target_Column, Data_Type)
    mock_cursor.fetchall.return_value = [
        (1, "Place_Code", "Place_Code", "VARCHAR(50)"),
        (2, "Place_Name", "Place_Name", "VARCHAR(200)"),
        (3, "Is_Active", "Is_Active", "BIT"),
    ]
    mock_cursor.fetchone.return_value = ("Dimension",)

    fake_path = Path("fake.fmt")
    generate_bcp_format_file("Places", fake_path)

    with open(fake_path, "r") as f:
        content = f.read()
        assert "14.0" in content
        assert "SQLCHAR" in content
        assert "Inserted_Datetime" in content
        assert '""' in content  # empty collation for bit + datetime