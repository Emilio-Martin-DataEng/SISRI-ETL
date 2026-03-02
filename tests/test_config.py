import pytest
from src.config import load_config, get_db_config

def test_load_config():
    config = load_config()
    assert 'database' in config

def test_get_db_config():
    db = get_db_config()
    assert db['server'] == 'Emilio'