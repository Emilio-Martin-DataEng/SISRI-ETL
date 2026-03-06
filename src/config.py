import os
from typing import Any
import yaml
from pathlib import Path
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_PATH = PROJECT_ROOT / "config.yaml"

@lru_cache()
def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, encoding="utf-8") as f:
        config = yaml.safe_load(f)
        
        def interpolate(d):
            if isinstance(d, dict):
                return {k: interpolate(v) for k, v in d.items()}
            if isinstance(d, list):
                return [interpolate(item) for item in d]
            if isinstance(d, str) and d.startswith("${") and d.endswith("}"):
                env_key = d[2:-1]
                value = os.getenv(env_key)
                if value is None:
                    raise ValueError(f"Missing env var: {env_key}")
                return value
            return d
        
        return interpolate(config)

def get_config(section: str, key: str, default: Any = None) -> Any:
    config = load_config()
    return config.get(section, {}).get(key, default)

def get_db_config() -> dict:
    config = load_config()
    
    db_section = config.get("database", {})
    
    if not db_section:
        raise ValueError("No 'database' section found in config.yaml")
    
    required_keys = ["server", "database", "username", "password"]
    missing = [k for k in required_keys if k not in db_section or not db_section[k]]
    if missing:
        raise ValueError(f"Missing required database keys: {', '.join(missing)}")
    
    return {
        "server": db_section["server"],
        "database": db_section["database"],
        "username": db_section["username"],
        "password": db_section["password"],
        "type": db_section.get("type", "sqlserver")
    }

def SYSTEM_BASE_PATH() -> Path:
    base = get_config("system", "base_path", default="")
    if base:
        return Path(base)
    return PROJECT_ROOT

def DATA_BASE_PATH() -> Path:
    base = get_config("data", "base_path", default="")
    if base:
        return Path(base)
    return SYSTEM_BASE_PATH() / "data"

def get_logs_dir() -> Path:
    logs_rel = get_config("logs", "rel_path", default="logs")
    logs_dir = SYSTEM_BASE_PATH() / logs_rel
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir

BASE_PATH = SYSTEM_BASE_PATH