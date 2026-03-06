
### 4. `docs/Fact-Engine-Roadmap.md`

```markdown
# Fact Engine Roadmap – fact-table-engine branch

## Goals
Extend the dimension ETL pipeline to support fact tables (e.g. sales, dispensings, inventory).

## Phase 1 – Design & Metadata
- Define first fact table(s) (e.g. Fact_Dispensings, Fact_Sales)
- Grain, measures, degenerate dims, FKs
- Add Fact_File_Mapping sheet to ETL_Config.xlsx
- Extend `etl_config.py` to load/merge fact metadata

## Phase 2 – Processing & Loading
- New module or extend `source_import.py` for facts
- Aggregation logic (SUM, COUNT, etc.)
- FK lookup (join to dims for SKs)
- Incremental vs full load (watermarks, change tracking)
- Truncate/append strategy for ODS.Fact_*

## Phase 3 – Merge & Orchestration
- Generate fact merge procs (INSERT/UPDATE or MERGE)
- Extend orchestrator to process fact sources (Processing_Order > 10?)
- Run fact merges after dim merges

## Phase 4 – Auditing & Rejects
- Fact-specific reject reasons (missing FK, invalid measure)
- Audit fact row counts & measures aggregated

## Phase 5 – Testing & Deployment
- Sample raw data for first fact
- Full run with dims + facts
- Performance tuning for large facts

## Current Status
- Dimension ETL complete & stable
- Ready to define first fact table

Next step: Choose first fact table and define grain/measures.