from pathlib import Path
import os
from typing import Dict, Any

import yaml
from dotenv import load_dotenv

# Load .env early
load_dotenv()
# print("Loaded .env → SQLSERVER_PASSWORD exists?", "SQLSERVER_PASSWORD" in os.environ)
# print("SQLSERVER_PASSWORD value:", os.getenv("SQLSERVER_PASSWORD", "[NOT SET]"))
PROJECT_ROOT = Path(__file__).parent.parent  # points to project root
CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


class Config:
    _data: Dict[str, Any] = {}

    @classmethod
    def load(cls):
        if cls._data:
            return

        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        # Simple environment variable interpolation
        def interpolate(d):
            if isinstance(d, dict):
                return {k: interpolate(v) for k, v in d.items()}
            elif isinstance(d, str) and d.startswith("${") and d.endswith("}"):
                env_key = d[2:-1]
                value = os.getenv(env_key)
                if value is None:
                    raise ValueError(f"Missing environment variable: {env_key}")
                return value
            else:
                return d

        cls._data = interpolate(raw)

    @classmethod
    def get(cls, *keys, default=None):
        if not cls._data:
            cls.load()

        data = cls._data
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, default)
            else:
                return default
        return data

    @classmethod
    def db_config(cls):
        db_type = cls.get("database", "type", default=cls.get("database", "default"))
        return cls.get("connections", db_type)
    
    @classmethod
    def get_logs_dir(cls) -> Path:
        """Returns the logs directory (creates it if missing)."""
        logs_rel = cls.get("logs", "rel_path", default="Logs")
        logs_dir = Path(logs_rel)  # relative to project root
        logs_dir.mkdir(parents=True, exist_ok=True)
        return logs_dir

# Convenience exports
get_logs_dir = Config.get_logs_dir


# Convenience shortcuts
get_config = Config.get
get_db_config = Config.db_config
BASE_PATH = lambda: Path(get_config("base", "file_root"))