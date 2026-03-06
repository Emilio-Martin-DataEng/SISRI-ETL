# SISRI ETL System - Technical Overview

## Purpose
Automated ETL pipeline for loading Excel-based raw data into a SQL Server data warehouse.  
Supports dimension tables (SCD Type 1 & 2) with metadata-driven configuration.

## Implemented Features

- **Configuration & Metadata**
  - `ETL_Config.xlsx` (Source_Imports + Source_File_Mapping sheets)
  - Bootstrap loader: `etl_config.py` → BCP to staging → merge to ETL dimensions
  - Optional via `--refresh-metadata` flag
  - Graceful skip if file missing (audit log only)
  - Archive of config file with lineage

- **Raw Data Processing**
  - File discovery using `base.file_root` + `Rel_Path` / `Pattern`
  - Excel read (multi-file support, sheet fallback)
  - Sanitization (linebreaks, backslashes, whitespace collapse)
  - Deduplication on PK columns (logs rejected rows to file + DB)
  - Auto-truncate of ODS staging table before load
  - BCP insert to ODS with per-run error logging (`logs/bcp_errors_*.log`)
  - Reject handling (duplicates, BCP failures)

- **Orchestration**
  - `etl_orchestrator.py` — runs selected sources in order
  - Flags: `--sources`, `--force-ddl`, `--refresh-metadata`
  - Filters business sources (Processing_Order > 1)
  - Executes merge procs after load
  - Applies pending DDL scripts from `DW_DDL/run/`

- **DDL & Format Generation**
  - `ddl_generator.py`
  - Dynamic BCP format files (SQLCHAR, lengths from Data_Type, empty collation for bit/datetime)
  - ODS table DDL (PK from Is_PK)
  - DW dimension DDL (SCD1/SCD2 templates, metadata columns, active NK index)
  - Merge proc generation (SCD1/2 logic)

- **DB Helpers**
  - `db_ops.py`: connect, truncate, execute proc, audit log
  - `db.py`: `upload_via_bcp` with error logging (`-e`), masked password

## Architecture Summary

- **Input**: Excel raw files + config Excel
- **Staging**: ODS schema (truncate + BCP)
- **Transformation**: Python (sanitize, dedup) + SQL merge procs (SCD handling)
- **Output**: DW dimensions (SCD1/2)
- **Logging**: Audit tables, rejected files, BCP error logs
- **Config**: YAML (paths) + Excel (mapping)

## Still To Do – Fact Engine (fact-table-engine branch)

- Fact table DDL generation
- Fact mapping extension in Excel
- Fact-specific processing (aggregations, FK lookups, incremental)
- Merge/append logic for facts
- Fact auditing & reject handling
- Orchestrator integration for fact sources