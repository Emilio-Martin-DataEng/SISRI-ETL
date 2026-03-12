"""Simple console UI for running SISRI ETL flows.

This wraps the existing orchestrator and DDL helpers with a small
menu so you can:
- Refresh metadata / generate DDL
- Apply approved DDL scripts
- Run full ETL for all or selected sources
- Quickly inspect available sources
"""

from datetime import datetime

from src.config import get_config
from src.etl_orchestrator import run_etl
from src.staging.etl_config import process_etl_config
from src.utils.db_ops import get_connection
from src.dw.ddl_generator import generate_ddl_for_changed_sources
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


def action_refresh_metadata_and_generate_ddl():
    print("\n[1] Refreshing ETL config and generating DDL (if mappings changed)...")
    start = datetime.now()
    process_etl_config()
    changed = generate_ddl_for_changed_sources()
    duration = (datetime.now() - start).total_seconds()
    if changed:
        print(f"DDL generated for: {', '.join(changed)}")
        print("Review scripts in config/DW_DDL/generated/, then copy approved ones to config/DW_DDL/run/.")
    else:
        print("No mapping changes detected; no new DDL generated.")
    print(f"Completed in {duration:.2f}s.")


def action_apply_ddl():
    print("\n[2] Applying DDL scripts from config/DW_DDL/run/ ...")
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


def action_run_full_etl():
    print("\n[3] Run full ETL (config + all active business sources).")
    sources = _input_sources()
    force_ddl = input("Force DDL generation? [y/N]: ").strip().lower().startswith("y")
    refresh_meta = input("Refresh ETL metadata first? [Y/n]: ").strip().lower()
    refresh_meta_flag = refresh_meta != "n"

    print("Starting ETL ...")
    start = datetime.now()
    run_etl(sources, force_ddl, refresh_meta_flag)
    duration = (datetime.now() - start).total_seconds()
    print(f"ETL run finished in {duration:.2f}s. Check logs/etl_orchestrator.log for details.")


def main_menu():
    while True:
        _print_header()
        print("1) Refresh config + generate DDL")
        print("2) Apply DDL from run/ folder")
        print("3) Run full ETL (config + sources)")
        print("4) List sources")
        print("0) Exit")

        choice = input("Select an option: ").strip()
        if choice == "1":
            action_refresh_metadata_and_generate_ddl()
        elif choice == "2":
            action_apply_ddl()
        elif choice == "3":
            action_run_full_etl()
        elif choice == "4":
            _list_sources()
        elif choice == "0":
            print("Goodbye.")
            break
        else:
            print("Invalid option.")


if __name__ == "__main__":
    main_menu()

