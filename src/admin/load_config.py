"""Admin entry point: load config from Excel, update metadata, optional DDL, first load.
Use rarely – only for new dataset/source take-on."""
import argparse
from src.staging.etl_config import process_etl_config


def main():
    parser = argparse.ArgumentParser(
        description="SISRI Admin – Load config from Excel, update metadata, optional DDL, first load."
    )
    parser.add_argument("--force-ddl", action="store_true", help="Regenerate format files + DDL, apply")
    parser.add_argument("--source", type=str, default=None, help="Scope force-ddl to single source")
    parser.add_argument("--no-first-load", action="store_true", help="Skip first load after config")
    args = parser.parse_args()
    process_etl_config(
        force_ddl=args.force_ddl,
        source=args.source,
        run_first_load=not args.no_first_load,
    )


if __name__ == "__main__":
    main()
