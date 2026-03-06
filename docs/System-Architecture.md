# SISRI ETL System Architecture

## High-Level Components

1. **Configuration Layer**
   - `ETL_Config.xlsx` (metadata + source mapping)
   - `config.yaml` (paths, file_root, folders)

2. **Metadata Bootstrap**
   - `etl_config.py` → optional via `--refresh-metadata`
   - Loads metadata to `[ETL].[Dim_Source_Imports]` and `[Dim_Source_Imports_Mapping]`
   - Generates system format files

3. **Orchestration Layer**
   - `etl_orchestrator.py`
   - Controls execution order (Processing_Order > 1)
   - Flags: `--sources`, `--force-ddl`, `--refresh-metadata`
   - Calls per-source processing + merge + DDL apply

4. **Source Processing Layer**
   - `source_import.py`
   - File discovery → read → sanitize → dedup (log rejects) → truncate ODS → BCP load → merge

5. **DDL & Code Generation**
   - `ddl_generator.py`
   - Format files, ODS tables, DW dimensions, merge procs

6. **DB Access Layer**
   - `db_ops.py` & `db.py`
   - Connection, truncate, proc execution, BCP upload, audit logging

7. **Output / Artifacts**
   - ODS staging tables (transient)
   - DW dimension tables (SCD1/2)
   - Rejected rows files
   - BCP error logs
   - Audit trail

## Data Flow

Raw Excel → Temp TXT → BCP → ODS → Merge Proc → DW Dim

## Key Design Decisions

- Transient ODS tables → truncate before load
- Metadata-driven (Excel config)
- Deduplication at source level + PK constraint at ODS
- Optional metadata refresh
- Rejected rows logged for traceability
- Business sources only in normal runs (Processing_Order > 1)

## Next Phase – Fact Engine

Extend pipeline for fact tables:
- Fact mapping in Excel
- Aggregation / FK lookup logic
- Incremental vs full load
- Append or merge strategy
- Fact-specific auditing