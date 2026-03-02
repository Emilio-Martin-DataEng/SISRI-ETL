 
from src.config import load_config, get_db_config, SYSTEM_BASE_PATH

def test_load_config():
    config = load_config()
    assert 'database' in config

def test_get_db_config():
    db = get_db_config()
    assert db['server'] == 'Emilio'

def test_system_base_path():
    path = SYSTEM_BASE_PATH()
    assert path.exists()
    assert (path / "config.yaml").exists()