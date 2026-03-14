"""ETL Run Report: HTML report from 3 views (highest grain down), opens in browser after run."""
import sys
import webbrowser
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.db_ops import get_connection


def _fetch_view(conn, view_name: str, run_sk: int | None = None, limit: int = 50):
    """Fetch view data, optionally filtered by Run SK."""
    if run_sk is not None:
        sql = f"SELECT * FROM ETL.{view_name} WHERE [Run SK] = ?"
        cursor = conn.cursor()
        cursor.execute(sql, (run_sk,))
    else:
        sql = f"SELECT TOP {limit} * FROM ETL.{view_name} ORDER BY [Run SK] DESC"
        cursor = conn.cursor()
        cursor.execute(sql)
    rows = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    return cols, rows


def _table_html(title: str, grain: str, cols: list, rows: list) -> str:
    """Render a table section."""
    if not rows:
        return f"""
        <section>
            <h2>{title}</h2>
            <p class="grain">Grain: {grain}</p>
            <p class="empty">(no rows)</p>
        </section>"""
    header = "".join(f"<th>{c}</th>" for c in cols)
    body = ""
    for row in rows:
        body += "<tr>"
        for v in row:
            s = str(v)[:200] if v is not None else ""
            body += f"<td>{s}</td>"
        body += "</tr>"
    return f"""
        <section>
            <h2>{title}</h2>
            <p class="grain">Grain: {grain} &nbsp;|&nbsp; {len(rows)} row(s)</p>
            <div class="table-wrap">
                <table><thead><tr>{header}</tr></thead><tbody>{body}</tbody></table>
            </div>
        </section>"""


def generate_report(run_sk: int | None = None, last_n_runs: int = 3) -> Path:
    """Generate HTML report and return path. If run_sk given, filter to that run; else show last N runs."""
    conn = get_connection()
    cursor = conn.cursor()

    # Get latest Run SK if not specified
    if run_sk is None:
        cursor.execute("SELECT TOP 1 Run_SK FROM ETL.Fact_Run ORDER BY Run_SK DESC")
        row = cursor.fetchone()
        run_sk = row[0] if row else None

    # 1. Run Summary (highest grain)
    cols1, rows1 = _fetch_view(conn, "VW_Run_Summary", run_sk, limit=last_n_runs)
    if run_sk and not rows1:
        cols1, rows1 = _fetch_view(conn, "VW_Run_Summary", None, limit=last_n_runs)

    # 2. Run Source Audit (middle grain)
    cols2, rows2 = _fetch_view(conn, "VW_Run_Source_Audit", run_sk, limit=100)
    if run_sk and not rows2:
        cols2, rows2 = _fetch_view(conn, "VW_Run_Source_Audit", None, limit=50)

    # 3. Source File Archive (lowest grain)
    cols3, rows3 = _fetch_view(conn, "VW_Source_File_Archive", run_sk, limit=100)
    if run_sk and not rows3:
        cols3, rows3 = _fetch_view(conn, "VW_Source_File_Archive", None, limit=50)

    cursor.close()
    conn.close()

    html = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>ETL Run Report</title>
    <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', system-ui, sans-serif; margin: 24px; background: #1a1a2e; color: #eee; }}
        h1 {{ color: #0f3460; font-size: 1.5rem; margin-bottom: 4px; }}
        .meta {{ color: #888; font-size: 0.9rem; margin-bottom: 24px; }}
        section {{ margin-bottom: 32px; }}
        h2 {{ color: #e94560; font-size: 1.1rem; margin: 0 0 8px 0; }}
        .grain {{ color: #a0a0a0; font-size: 0.85rem; margin: 0 0 12px 0; }}
        .empty {{ color: #666; font-style: italic; }}
        .table-wrap {{ overflow-x: auto; border: 1px solid #333; border-radius: 6px; }}
        table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; }}
        th, td {{ padding: 8px 12px; text-align: left; border-bottom: 1px solid #333; }}
        th {{ background: #16213e; color: #e94560; font-weight: 600; white-space: nowrap; }}
        tr:hover {{ background: #16213e; }}
        td {{ max-width: 280px; overflow: hidden; text-overflow: ellipsis; }}
    </style>
</head>
<body>
    <h1>ETL Run Report</h1>
    <p class="meta">Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        {" | Run SK: " + str(run_sk) if run_sk else " | Last " + str(last_n_runs) + " runs"}
    </p>

    {_table_html("1. Run Summary", "1 row per run (orchestrator execution)", cols1, rows1)}
    {_table_html("2. Run Source Audit", "1 row per source per run", cols2, rows2)}
    {_table_html("3. Source File Archive", "1 row per file", cols3, rows3)}
</body>
</html>"""

    report_dir = PROJECT_ROOT / "temp" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    report_path = report_dir / f"etl_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    report_path.write_text(html, encoding="utf-8")
    return report_path


def show_report(run_sk: int | None = None, last_n_runs: int = 3):
    """Generate report and open in browser."""
    path = generate_report(run_sk=run_sk, last_n_runs=last_n_runs)
    webbrowser.open(path.as_uri())
    return path


if __name__ == "__main__":
    run_sk = int(sys.argv[1]) if len(sys.argv) > 1 else None
    path = show_report(run_sk=run_sk)
    print(f"Report opened: {path}")
