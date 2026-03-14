"""
CLI for mapping metadata validation.
Delegates to src.utils.check_mapping.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.check_mapping import run_check_mapping


def main():
    success, issues = run_check_mapping()
    if not success:
        print("Check Mapping: FAILED")
        print("Admin must correct ETL_Config.xlsx and re-run metadata refresh.")
        print()
        for i, msg in enumerate(issues, 1):
            print(f"  {i}. {msg}")
        sys.exit(1)
    print("Check Mapping: OK")
    sys.exit(0)


if __name__ == "__main__":
    main()
