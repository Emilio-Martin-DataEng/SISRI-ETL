"""Thin wrapper for integrated scenario tests. Prefer: python -m src.etl_orchestrator --test full"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tests.test_scenarios import main

if __name__ == "__main__":
    sys.exit(main())
