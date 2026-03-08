# SISRI ETL System – Administrator Instruction Manual

## Overview
The SISRI ETL system loads Excel data into a SQL Server data warehouse. It has two main parts:

- **Metadata refresh** – updates the configuration tables (`Source_Imports` & `Source_File_Mapping`)
- **Data load** – processes raw Excel files into staging (ODS) tables and merges them into dimension tables (DW)

## Prerequisites

- Python 3.12 virtual environment (named `venv312`)
- SQL Server credentials configured
- `config.yaml` must be populated with the necessary paths (e.g. `file_root`)
- Excel files in correct folders (defined in `ETL_Config.xlsx`)

## 1. Activate the Environment

Activate the virtual environment before running any commands.

## 2. Refresh Metadata  
(Required after changing `ETL_Config.xlsx` – mapping, new sources, paths, etc.)

Run:  
`python -m src.staging.etl_config`

What it does:
- Checks for `ETL_Config.xlsx`
- Loads `Source_Imports` and `Source_File_Mapping` sheets via BCP
- Merges to `[ETL].[Dim_Source_Imports]` and `[Dim_Source_Imports_Mapping]`
- Regenerates system format files if needed
- Archives the config file
- Logs success or graceful skip if file missing

## 3. Run Normal ETL Load

Basic command (processes all active business sources):  
`python -m src.etl_orchestrator`

### Common options

| Flag                  | Description                                      | Example                                      |
|-----------------------|--------------------------------------------------|----------------------------------------------|
| `--sources`           | Specific sources only                            | `--sources Places Products`                  |
| `--force-ddl`         | Force regeneration of tables & procs             | `--force-ddl`                                |
| `--refresh-metadata`  | Also refresh metadata before data load           | `--refresh-metadata`                         |

### Examples

All sources (normal daily run):  
`python -m src.etl_orchestrator`

Only Places and Brands:  
`python -m src.etl_orchestrator --sources Places Brands`

Full refresh + force DDL:  
`python -m src.etl_orchestrator --refresh-metadata --force-ddl`

## 4. What Happens During a Run

1. Optional metadata refresh (if flag used)
2. Truncate each ODS staging table
3. Find raw Excel files
4. Read, clean, deduplicate (log duplicates)
5. Load via BCP to ODS
6. Run merge stored procedure to update DW dimension
7. Apply any pending DDL scripts from `DW_DDL/run/`
8. Log audit entry + rejects/duplicates

## 5. Monitoring & Troubleshooting

| Area               | Check Command / Location                                      | What to look for                              |
|--------------------|----------------------------------------------------------------|-----------------------------------------------|
| Audit trail        | `SELECT TOP 20 * FROM [ETL].[Audit_Source_Import] ORDER BY Start_Time DESC` | Run status, row counts, errors                |
| Rejected rows      | `rejected/*.txt` files                                         | Full duplicate rows or BCP rejects            |
| BCP errors         | `logs/bcp_errors_*.log`                                        | Rejected rows from BCP (bad data, nulls, etc.)|
| Row counts in DW   | `SELECT COUNT(*) FROM DW.Dim_Places` (etc.)                    | Expected vs loaded rows                       |
| Format files       | `config\format\system\*.fmt` and `config\format\sources\*.fmt` | Recent timestamps after refresh               |

### Common issues & fixes

- **No rows loaded** → Check rejected file or BCP log for rejects
- **PK violation** → ODS table was not truncated (should be automatic)
- **Format error** → Re-run metadata refresh
- **Missing sources** → Verify `Processing_Order > 1` and `Is_Active = 1` in `Dim_Source_Imports`

## 6. Maintenance Tasks

- **Update mapping/sources** → Edit `ETL_Config.xlsx` → Run metadata refresh
- **Clean up** → Periodically delete old files in `temp/`, `logs/`, `rejected/`
- **Backup** → Regularly back up `ETL_Config.xlsx` and `config.yaml`
- **Monitor disk** → Watch `logs/` and `rejected/` for growth

## 7. Support & Next Steps

Contact the development team for:
- Fact table engine rollout
- Custom stored procedures
- Scheduled runs (e.g. via Windows Task Scheduler)
- Email/Slack alerts on failures or high rejects

**Last updated**: March 2026