"""Integrated scenario tests: Operator and Admin entry points.
Run via: python -m src.etl_orchestrator --test full
Or: python -m pytest tests/test_scenarios.py -v
"""
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SCENARIOS = [
    ("Operator --help", ["python", "-m", "src.etl_orchestrator", "--help"]),
    ("Admin --help", ["python", "-m", "src.admin.load_config", "--help"]),
    ("Operator --sources Brands", ["python", "-m", "src.etl_orchestrator", "--sources", "Brands"]),
    ("Admin --no-first-load", ["python", "-m", "src.admin.load_config", "--no-first-load"]),
    ("Admin --force-ddl --source Sales_Format_1 --no-first-load", [
        "python", "-m", "src.admin.load_config", "--force-ddl", "--source", "Sales_Format_1", "--no-first-load"
    ]),
    ("Operator Fact_Conformed", ["python", "-m", "src.etl_orchestrator", "--sources", "Staging_Fact_Sales_Conformed"]),
]


def _run(cmd: list[str]) -> tuple[bool, str]:
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, cwd=PROJECT_ROOT)
    out = (result.stdout or "") + (result.stderr or "")
    return result.returncode == 0, out


def run_scenario_tests() -> tuple[int, int]:
    """Run all scenario tests. Returns (passed, failed)."""
    passed = 0
    failed = 0
    for name, cmd in SCENARIOS:
        ok, out = _run(cmd)
        if ok:
            passed += 1
        else:
            failed += 1
    return passed, failed


def main():
    print("=== SISRI Integrated Scenario Tests ===\n")
    passed = 0
    failed = 0
    for name, cmd in SCENARIOS:
        print(f"Running: {name} ... ", end="", flush=True)
        ok, out = _run(cmd)
        if ok:
            print("PASS")
            passed += 1
        else:
            print("FAIL")
            failed += 1
            print("  Output (last 10 lines):")
            for line in out.strip().split("\n")[-10:]:
                print("    " + line)
    print(f"\nResult: {passed} passed, {failed} failed")
    return 0 if failed == 0 else 1


def test_scenarios_full():
    """Pytest: run full scenario suite. Requires DB."""
    passed, failed = run_scenario_tests()
    assert failed == 0, f"Scenario tests: {passed} passed, {failed} failed"


if __name__ == "__main__":
    sys.exit(main())
