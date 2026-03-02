from pathlib import Path
import pytest
from src.utils.db_ops import get_connection, generate_bcp_format_file

def test_get_connection():
    conn = get_connection()
    assert conn is not None
    conn.close()

def test_generate_bcp_format_file():
    fmt_path = Path("test.fmt")
    generate_bcp_format_file("TestSource", fmt_path)
    assert fmt_path.exists()
    fmt_path.unlink()