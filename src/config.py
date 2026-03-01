# src/config.py
"""
Central configuration loader for SISRI-ETL.
Loads config.yaml + .env, provides path helpers and DB config.
"""

import os
from typing import Any, Dict
import yaml
from pathlib import Path
from functools import lru_cache
from dotenv import load_dotenv

# Load .env early for ${VAR} interpolation
load_dotenv()
print("DEBUG: SQLSERVER_PASSWORD from env:", os.getenv("SQLSERVER_PASSWORD", "[NOT SET]"))

# Project root (one level up from src/)
PROJECT_ROOT = Path(__file__).parent.parent

# Config file location (fixed in project root)
CONFIG_PATH = PROJECT_ROOT / "config.yaml"

@lru_cache()
def load_config() -> Dict[str, Any]:
    print("DEBUG: Looking for config.yaml at:", CONFIG_PATH.absolute())
    
    if not CONFIG_PATH.exists():
        print("DEBUG: config.yaml NOT FOUND!")
        raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")
    
    with open(CONFIG_PATH, encoding="utf-8") as f:
        raw = f.read()
        print("DEBUG: Raw config.yaml content (first 500 chars):\n", raw[:500])
        
        config = yaml.safe_load(raw)
        
        # NEW: Recursively interpolate ${VAR} placeholders
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
                print(f"DEBUG: Interpolated {env_key} → {value[:3]}***")  # mask for security
                return value
            return d
        
        config = interpolate(config)
        print("DEBUG: Parsed & interpolated config keys:", list(config.keys()))
        return config

def get_config(section: str, key: str, default: Any = None) -> Any:
    """
    Get a value from config.yaml.
    Example: get_config("database", "server")
    """
    config = load_config()
    return config.get(section, {}).get(key, default)

def get_db_config() -> Dict[str, str]:
    """
    Returns database connection details from config.yaml.
    Uses the flat 'database' section structure.
    """
    config = load_config()
    print("DEBUG: Full config loaded in get_db_config:", config)
    
    db_section = config.get("database", {})
    print("DEBUG: db_section:", db_section)
    
    if not db_section:
        raise ValueError("No 'database' section found in config.yaml")
    
    required_keys = ["server", "database", "username", "password"]
    missing = [k for k in required_keys if k not in db_section or not db_section[k]]
    if missing:
        raise ValueError(f"Missing required database keys: {', '.join(missing)}")
    
    print("DEBUG: Using flat database structure")
    return {
        "server": db_section["server"],
        "database": db_section["database"],
        "username": db_section["username"],
        "password": db_section["password"],
        "type": db_section.get("type", "sqlserver")
    }

def SYSTEM_BASE_PATH() -> Path:
    """Fixed path for admin/system files (logs, temp, config, format, DDL)."""
    base = get_config("system", "base_path", default="")
    if base:
        return Path(base)
    return PROJECT_ROOT  # auto-detect project root

def DATA_BASE_PATH() -> Path:
    """Base path for data files (raw, archive, rejected) — flexible for data capturers."""
    base = get_config("data", "base_path", default="")
    if base:
        return Path(base)
    return SYSTEM_BASE_PATH() / "data"  # fallback

def get_logs_dir() -> Path:
    """Returns the logs directory (creates if missing)."""
    logs_rel = get_config("logs", "rel_path", default="logs")
    logs_dir = SYSTEM_BASE_PATH() / logs_rel
    logs_dir.mkdir(parents=True, exist_ok=True)
    return logs_dir

# Legacy compatibility (if any old code uses BASE_PATH)
BASE_PATH = SYSTEM_BASE_PATH  # alias for backward compatibility

# Optional: interpolation helper (already handled by dotenv + yaml)
def interpolate_value(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
        env_key = value[2:-1]
        env_value = os.getenv(env_key)
        if env_value is None:
            raise ValueError(f"Missing environment variable: {env_key}")
        return env_value
    return value