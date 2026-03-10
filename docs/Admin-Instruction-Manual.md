# SISRI ETL System – Administrator Instruction Manual

 **Production-Ready System with Complete SK Flow Implementation**

## Overview
The SISRI ETL system loads Excel data into a SQL Server data warehouse with **file-level granularity** and **complete audit trail**. It has two main parts:

- **Metadata refresh** – updates the configuration tables (`Source_Imports` & `Source_File_Mapping`)
- **Data load** – processes raw Excel files into staging (ODS) tables and merges them into dimension tables (DW)
- ** Individual file processing** with unique SK tracking per file
- ** Complete audit trail** from source file to final dimension

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
- ** Merges to `[ETL].[Dim_Source_Imports]` and `[Dim_Source_Imports_Mapping]`**
- ** Regenerates system format files with proper terminators**
- ** Archives the config file with lineage tracking**
- Logs success or graceful skip if file missing

## 3. Run Normal ETL Load

### ** Production-Ready Commands**

Basic command (processes all active business sources):  
`python -m src.etl_orchestrator`

### **Common Options**

| Flag                  | Description                                      | Example                                      |
|-----------------------|--------------------------------------------------|----------------------------------------------|
| `--sources`           | Specific sources only                            | `--sources Places Products`                  |
| `--force-ddl`         | Force regeneration of tables & procs             | `--force-ddl`                                |
| `--refresh-metadata`  | Also refresh metadata before data load           | `--refresh-metadata`                         |

### **Examples**

All sources (normal daily run):  
`python -m src.etl_orchestrator`

Only Places and Brands:  
`python -m src.etl_orchestrator --sources Places Brands`

Full refresh + force DDL:  
`python -m src.etl_orchestrator --refresh-metadata --force-ddl`

## 4. What Happens During a Run

### ** Current Implementation Flow**

1. Optional metadata refresh (if flag used)
2. ** File-by-file processing** for each source:
   - Truncate each ODS staging table
   - Find raw Excel files (supports multiple files per source)
   - Read, clean, deduplicate (log duplicates)
   - ** Create individual archive record per file**
   - Load via BCP to ODS with proper format files
   - ** Run merge procedure with individual SK parameters per file**
3. Apply any pending DDL scripts from `DW_DDL/run/`
4. ** Log complete audit trail** with file-level granularity
5. ** Log rejects/duplicates** with detailed error tracking

### **Key Improvements**
- ** File-Level SK Granularity**: Each file gets unique `Source_File_Archive_SK`
- ** Individual Merge Execution**: Separate merge calls per file with correct SK parameters
- ** Complete Audit Trail**: End-to-end tracking from file to dimension
- ** Error Isolation**: One file failure doesn't stop others

## 5. Monitoring & Troubleshooting

### ** Production Monitoring Commands**

| Area               | Check Command / Location                                      | What to look for                              |
|--------------------|----------------------------------------------------------------|-----------------------------------------------|
| ** Audit trail**        | `SELECT TOP 20 * FROM [ETL].[Audit_Source_Import] ORDER BY Start_Time DESC` | Run status, row counts, errors                |
| ** Archive records**     | `SELECT TOP 10 * FROM [ETL].[Fact_Source_File_Archive] ORDER BY Source_File_Archive_SK DESC` | File-level SK tracking, processing status       |
| Rejected rows      | `rejected/*.txt` files                                         | Full duplicate rows or BCP rejects            |
| BCP errors         | `logs/bcp_errors_*.log`                                        | Rejected rows from BCP (bad data, nulls, etc.)|
| ** Row counts in DW**   | `SELECT COUNT(*) FROM DW.Dim_Places` (etc.)                    | Expected vs loaded rows, SK verification      |
| Format files       | `config\format\system\*.fmt` and `config\format\sources\*.fmt` | Recent timestamps after refresh               |

### ** Current Production Status**

All dimensions are fully operational with perfect SK flow:
- **Principals**: 40 rows → Archive_SK=1087 → Individual merge executed
- **Brands**: 212 rows → Archive_SK=1088 → Individual merge executed
- **Wholesalers**: 20 rows → Archive_SK=1089 → Individual merge executed
- **Products**: 12 rows → Archive_SK=1090 → Individual merge executed
- **Places**: 1,022 rows → Archive_SK=1091, 1092 → 2 individual merges executed

### **Common Issues & Fixes**

- ** No more -1 SK values**: All dimension tables now have correct SK values
- ** Zero BCP errors**: All format files working correctly with proper terminators
- ** Perfect file tracking**: Each file has individual archive record and merge execution
- **PK violation** → ODS table was not truncated (should be automatic)
- **Format error** → Re-run metadata refresh (now fixed with proper terminators)
- **Missing sources** → Verify `Processing_Order > 1` and `Is_Active = 1` in `Dim_Source_Imports`

## 6. Maintenance Tasks

- **Update mapping/sources** → Edit `ETL_Config.xlsx` → Run metadata refresh
- **Clean up** → Periodically delete old files in `temp/`, `logs/`, `rejected/`
- **Backup** → Regularly back up `ETL_Config.xlsx` and `config.yaml`
- **Monitor disk** → Watch `logs/` and `rejected/` for growth
- ** SK verification** → Check dimension tables for proper SK values (no more -1)
- ** Archive monitoring** → Review `Fact_Source_File_Archive` for file processing history

## 7. Support & Next Steps

Contact development team for:
- ** Fact table engine rollout** (roadmap defined)
- Custom stored procedures
- Scheduled runs (e.g. via Windows Task Scheduler)
- Email/Slack alerts on failures or high rejects
- ** Performance monitoring** for file-level processing metrics

## 8. ** Production Success Verification**

### **Daily Health Check Commands**

```sql
-- Check dimension tables for proper SK values
SELECT 
    'Principals' as Source, COUNT(*) as RowCount, 
    COUNT(DISTINCT Audit_Source_Import_SK) as AuditSK_Count,
    COUNT(DISTINCT Source_File_Archive_SK) as ArchiveSK_Count,
    MIN(Audit_Source_Import_SK) as Min_Audit_SK,
    MIN(Source_File_Archive_SK) as Min_Archive_SK
FROM DW.Dim_Principals
UNION ALL
SELECT 
    'Brands' as Source, COUNT(*) as RowCount,
    COUNT(DISTINCT Audit_Source_Import_SK) as AuditSK_Count,
    COUNT(DISTINCT Source_File_Archive_SK) as ArchiveSK_Count,
    MIN(Audit_Source_Import_SK) as Min_Audit_SK,
    MIN(Source_File_Archive_SK) as Min_Archive_SK
FROM DW.Dim_Brands
UNION ALL
SELECT 
    'Places' as Source, COUNT(*) as RowCount,
    COUNT(DISTINCT Audit_Source_Import_SK) as AuditSK_Count,
    COUNT(DISTINCT Source_File_Archive_SK) as ArchiveSK_Count,
    MIN(Audit_Source_Import_SK) as Min_Audit_SK,
    MIN(Source_File_Archive_SK) as Min_Archive_SK
FROM DW.Dim_Places;
```

### **Expected Results**
- **RowCount**: > 0 for all active sources
- **AuditSK_Count**: 1 (same audit run)
- **ArchiveSK_Count**: 1+ (multiple files for sources like Places)
- **Min_Audit_SK**: > 0 (real audit ID, not -1)
- **Min_Archive_SK**: > 0 (real archive SK, not -1)

---

** Status: Production System with Complete SK Flow and File-Level Granularity!**

**Last updated**: March 2026 (Current Implementation Success)