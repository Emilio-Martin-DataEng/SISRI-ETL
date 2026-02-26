# SISRI-ETL

Metadata-driven Kimball ETL system for SISRI project.

## Setup
1. Install requirements: pip install -r requirements.txt
2. Set config.yaml and .env
3. Run config loader: python -m src.staging.etl_config
4. Run full ETL: python src/etl_orchestrator.py

## Structure
- src/staging/etl_config.py → config bootstrap
- src/staging/source_import.py → per-source loader
- src/etl_orchestrator.py → full run coordinator