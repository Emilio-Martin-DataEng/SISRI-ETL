# SISRI ETL – Agent Context

## Project Summary
Metadata-driven Kimball-style ETL for SQL Server. Excel/CSV sources, BCP loading. Schemas: `ODS` (staging), `DW` (dims/facts), `ETL` (metadata, audit, conformed staging).

## Entry Points (role separation)
- **Operator:** `python -m src.etl_orchestrator` – data processing only (assumes metadata correct)
- **Admin:** `python -m src.admin.load_config` – config load, metadata, DDL, first load (rare: new source take-on)

## Key Paths
- `src/etl_orchestrator.py` – Operator: data processing
- `src/admin/load_config.py` – Admin: config loader CLI
- `src/staging/etl_config.py` – Admin: config load logic
- `src/staging/source_import.py` – dimension sources
- `src/staging/fact_sales_import.py` – fact sources
- `src/dw/ddl_generator.py` – DDL generator (ODS, DW, merge procs, conformed merge)
- `config/ETL_Config.xlsx` – source config (Source_Imports, Source_File_Mapping, DW_Mapping_And_Transformations)
- `docs/Next-Steps-Plan.md` – roadmap

## Source Types
| Type | Flow |
|------|------|
| Dimension | ODS → DW.Dim_* via SP_Merge_Dim_* |
| Dimension_Conformed | Multiple ODS → Staging_Dim_*_Conformed → DW.Dim_* (planned for Dim_Products) |
| Fact_Sales | ODS → Staging_Fact_Sales_Conformed via SP_Merge_Fact_Sales_ODS_to_Conformed |
| Fact_Conformed | Staging_Fact_Sales_Conformed → DW.Fact_Sales via SP_Merge_Fact_Sales |
| System | Config/metadata only |

## Common Commands
```bash
# Operator: data processing (normal runs)
python -m src.etl_orchestrator
python -m src.etl_orchestrator --sources Brands Places

# Admin: config load (new source take-on only)
python -m src.admin.load_config
python -m src.admin.load_config --force-ddl
python -m src.admin.load_config --force-ddl --source Sales_Format_1

# Unit tests (no DB)
python -m pytest tests/test_smoke.py tests/test_ddl_generator.py -v

# Integrated scenario tests (requires DB) – run after any config sheet changes
python -m src.etl_orchestrator --test full
```

## DDL Rules
- **Tables:** Generate scripts into `config/DW_DDL/generated/`; ODS/staging auto-apply from `run/`
- **Procs:** OK to change; conformed merge procs auto-apply; DW merge procs need manual review

## Known Issues (see Next-Steps-Plan.md)
- Fact_Sales pipeline: fixed (temp table, dynamic format, Source_File_Archive_SK from ODS)
- Dim_Products conformed: planned (Phase 2)
