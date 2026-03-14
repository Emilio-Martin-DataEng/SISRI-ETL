"""Query ETL reporting views to verify grains. Run after full ETL test."""
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_ops import get_connection


def run_query(title: str, sql: str):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    cursor.close()
    conn.close()
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"  Grain: {len(rows)} row(s)")
    print(f"{'='*60}")
    if not rows:
        print("  (no rows)")
        return
    # Header
    print("  | ".join(f"{c[:18]:18}" for c in cols[:min(8, len(cols))]))
    print("  " + "-" * (20 * min(8, len(cols))))
    for row in rows[:15]:
        vals = [str(v)[:18] if v is not None else "" for v in row[:min(8, len(cols))]]
        print("  | ".join(f"{v:18}" for v in vals))
    if len(rows) > 15:
        print(f"  ... and {len(rows) - 15} more rows")
    print()


if __name__ == "__main__":
    print("\nETL Reporting Views - Grain Check (3 tables)")
    print("Run after: python -m src.etl_orchestrator --test full\n")

    # 1. Run grain: one row per orchestrator execution
    run_query(
        "1. VW_Run_Summary (grain: 1 row per RUN)",
        "SELECT TOP 20 * FROM ETL.VW_Run_Summary ORDER BY [Run SK] DESC"
    )

    # 2. Source-audit grain: one row per source per run
    run_query(
        "2. VW_Run_Source_Audit (grain: 1 row per SOURCE per RUN)",
        "SELECT TOP 30 * FROM ETL.VW_Run_Source_Audit ORDER BY [Run SK] DESC, [Source Name]"
    )

    # 3. File grain: one row per file
    run_query(
        "3. VW_Source_File_Archive (grain: 1 row per FILE)",
        "SELECT TOP 30 * FROM ETL.VW_Source_File_Archive ORDER BY [Run SK] DESC, [Source File Archive SK]"
    )

    # Summary counts
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ETL.Fact_Run")
    run_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ETL.Fact_Audit_Source_Imports WHERE Is_Deleted = 0")
    audit_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM ETL.Fact_Source_File_Archive")
    file_count = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    print(f"{'='*60}")
    print("  GRAIN SUMMARY")
    print(f"{'='*60}")
    print(f"  Fact_Run:                 {run_count} rows (1 per run)")
    print(f"  Fact_Audit_Source_Imports: {audit_count} rows (1 per source per run)")
    print(f"  Fact_Source_File_Archive:  {file_count} rows (1 per file)")
    print()
