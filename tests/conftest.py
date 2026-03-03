import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_cursor():
    return MagicMock()

@pytest.fixture
def mock_conn(mock_cursor):
    conn = MagicMock()
    conn.cursor.return_value = mock_cursor
    return conn