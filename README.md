# SISRI-ETL

Metadata-driven Kimball ETL system for SISRI project.

## Setup
1. Install requirements: `pip install -r requirements.txt`
2. Set config.yaml and .env
3. Run migration (once): `scripts/migrations/001_add_scd_pk_to_mapping.sql`
4. Load config: `python -m src.staging.etl_config`
5. Run ETL: `python src/etl_orchestrator.py`

## Orchestrator
```bash
python src/etl_orchestrator.py --from config    # config + DDL gen if mapping changed
python src/etl_orchestrator.py --from staging  # config + source staging
python src/etl_orchestrator.py --from dims     # config + staging + dimension merges
python src/etl_orchestrator.py --from facts    # full pipeline
python src/etl_orchestrator.py --sources Principals
python src/etl_orchestrator.py --apply-ddl     # execute run/, archive on success
```

## DW DDL Workflow
1. Config load → writes .sql to `config/DW_DDL/generated/` when mapping changed
2. Engineer copies to `config/DW_DDL/run/`
3. `--apply-ddl` executes and archives

## Structure
- src/staging/etl_config.py → config bootstrap
- src/staging/source_import.py → per-source loader
- src/etl_orchestrator.py → full run coordinator