import pytest
from unittest.mock import MagicMock
from src.utils.db_ops import generate_bcp_format_file, get_connection
from pathlib import Path

def test_get_connection():
    conn = get_connection()
    assert conn is not None
    conn.close()

def test_generate_bcp_format_file(mocker):
    # Mock the DB connection and cursor
    mock_conn = mocker.MagicMock()
    mock_cursor = mocker.MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Fake rows from SP: (Source_Column, Target_Column, Data_Type)
    mock_cursor.fetchall.return_value = [
        ('SourceID', 'Principal_ID', 'INT'),
        ('SourceName', 'Name', 'VARCHAR(100)'),
        ('SourceDate', 'Effective_Date', 'DATE')
    ]
    
    mocker.patch('src.utils.db_ops.get_connection', return_value=mock_conn)

    fmt_path = Path("test.fmt")
    generate_bcp_format_file("TestSource", fmt_path)
    
    assert fmt_path.exists()  # File should be created
    # Optional: read and check content
    content = fmt_path.read_text()
    assert "Principal_ID" in content
    assert "SQLINT" in content  # From INT
    assert "SQLCHAR" in content  # From VARCHAR
    assert "SQLDATETIME" in content  # From DATE

    fmt_path.unlink()  # Clean up