"""Simple console UI for SISRI ETL.

Roles:
- Admin (1, 2): Config load, DDL – rarely used (new source take-on)
- Operator (3, 4): Data processing, list sources – normal runs
"""

from datetime import datetime

from src.config import get_config
from src.etl_orchestrator import run_etl
from src.staging.etl_config import process_etl_config
from src.utils.db_ops import get_connection
from src.dw.script_executor import execute_run_folder_scripts


def _print_header():
    print("\n=== SISRI ETL Console ===")


def _list_sources():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT Source_Name,
               Source_Type,
               Processing_Order,
               Is_Active,
               Last_Successful_Load_Datetime
        FROM ETL.Dim_Source_Imports
        ORDER BY Processing_Order, Source_Name
        """
    )
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    if not rows:
        print("No sources found in ETL.Dim_Source_Imports.")
        return

    print("\nSources:")
    print("Name                Type         Active  Order  Last Successful Load")
    print("-" * 80)
    for name, s_type, order, is_active, last_load in rows:
        name_str = (name or "")[:18].ljust(18)
        t_str = (s_type or "Dimension")[:10].ljust(10)
        active_str = "Y" if is_active else "N"
        order_str = f"{order:.2f}" if isinstance(order, float) else str(order or "")
        last_str = last_load.strftime("%Y-%m-%d %H:%M:%S") if last_load else "-"
        print(f"{name_str}  {t_str}  {active_str:^6}  {order_str:>5}  {last_str}")


def _input_sources():
    raw = input("Enter source names (comma-separated) or leave blank for ALL: ").strip()
    if not raw:
        return None
    return [s.strip() for s in raw.split(",") if s.strip()]


def action_load_config():
    print("\n[1] Admin: Load config from Excel, update metadata, first load...")
    force_ddl = input("Force DDL (format files + apply)? [y/N]: ").strip().lower().startswith("y")
    start = datetime.now()
    process_etl_config(force_ddl=force_ddl)
    duration = (datetime.now() - start).total_seconds()
    print(f"Completed in {duration:.2f}s. Inspect logs for any errors.")


def action_apply_ddl():
    print("\n[2] Applying DDL scripts from DW_DDL/run/ ...")
    ok, errs = execute_run_folder_scripts()
    if ok:
        print(f"Executed and archived: {', '.join(ok)}")
    else:
        print("No .sql files found in run/ or nothing executed.")
    if errs:
        print("Some scripts failed:")
        for fname, msg in errs:
            print(f" - {fname}: {msg}")
    print("DDL apply step finished.")


def action_run_etl():
    print("\n[3] Operator: Run data processing (assumes metadata is correct).")
    sources = _input_sources()
    print("Starting ETL ...")
    start = datetime.now()
    run_etl(sources=sources)
    duration = (datetime.now() - start).total_seconds()
    print(f"ETL run finished in {duration:.2f}s. Check logs/etl_orchestrator.log for details.")


def main_menu():
    while True:
        _print_header()
        print("1) [Admin] Load config + metadata + first load")
        print("2) [Admin] Apply DDL from run/ folder")
        print("3) [Operator] Run data processing")
        print("4) List sources")
        print("0) Exit")

        choice = input("Select an option: ").strip()
        if choice == "1":
            action_load_config()
        elif choice == "2":
            action_apply_ddl()
        elif choice == "3":
            action_run_etl()
        elif choice == "4":
            _list_sources()
        elif choice == "0":
            print("Goodbye.")
            break
        else:
            print("Invalid option.")


if __name__ == "__main__":
    main_menu()

