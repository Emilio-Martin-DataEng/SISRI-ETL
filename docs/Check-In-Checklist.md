# Check-In Checklist

**Run these steps before every commit/check-in.**

---

## When to Run Scenario Tests

**Run scenario tests after any config sheet changes:**
- Source_Imports, Source_File_Mapping, DW_Mapping_And_Transformations
- New source added, column mapping changed, conformed mapping updated
- After `python -m src.admin.load_config` (metadata refresh)

---

## 1. Unit Tests

**Core tests (no DB, always run):**
```bash
python -m pytest tests/test_smoke.py tests/test_ddl_generator.py -v
```

- **Goal:** 2 tests pass (smoke + BCP format file generation)
- **No DB required**

**Full suite (has known failures):**
```bash
python -m pytest tests/ -v --tb=short
```
- 5 tests currently fail (mocks out of date, integration tests need DB). Fix or skip as needed.

---

## 2. Import Verification

```bash
python -c "
from src.etl_orchestrator import run_etl
from src.staging.etl_config import process_etl_config
from src.dw.ddl_generator import apply_ddl_from_run, generate_ods_table_ddl
from src.utils.db_ops import truncate_table, execute_proc
print('Imports OK')
"
```

- **Goal:** No import errors
- **Catches:** Broken refactors, missing modules

---

## 3. Lint (if configured)

```bash
# Optional - run if ruff/flake8/black is set up
ruff check src/
# or: flake8 src/
```

---

## 4. Integrated Scenario Tests (requires DB)

**Required after config sheet changes; recommended before every check-in:**

```bash
python -m src.etl_orchestrator --test full
```

- **Goal:** Operator + Admin entry points, selective sources, Fact_Conformed
- **Runs:** 6 scenarios (help, Brands, Admin config, force-ddl scope, Fact_Conformed)
- **When:** After editing ETL_Config.xlsx (Source_Imports, Source_File_Mapping, DW_Mapping_And_Transformations)
- **Alternative:** `python -m pytest tests/test_scenarios.py -v` (secondary; use when running with other pytest tests)

---

## 5. Monitoring Scripts (optional)

```bash
python tools/monitoring/check_ods.py
python tools/monitoring/check_mapping.py   # Validates config metadata; reports only, no fixes
python tools/monitoring/check_staging.py
python tools/monitoring/check_merge_procs.py
python tools/monitoring/check_merge_logic.py
```

- **check_mapping:** Validates Source_Imports, Source_File_Mapping, DW_Mapping_And_Transformations. Reports issues; admin must fix spreadsheet and re-run metadata refresh.
- **Goal:** ODS/staging/DW structure and merge procs are valid
- **Requires:** DB connection

---

## Summary – Minimum for Every Check-In

| Step | Command | Required |
|------|---------|----------|
| 1. Tests | `python -m pytest tests/test_smoke.py tests/test_ddl_generator.py -v` | Yes |
| 2. Imports | `python -c "from src.etl_orchestrator import run_etl; from src.staging.etl_config import process_etl_config; from src.dw.ddl_generator import apply_ddl_from_run; from src.utils.db_ops import truncate_table, execute_proc; print('Imports OK')"` | Yes |
| 3. Lint | `ruff check src/` (if configured) | Optional |
| 4. Scenarios | `python -m src.etl_orchestrator --test full` | Required after config changes |
| 5. Monitoring | `python tools/monitoring/check_*.py` | Optional (DB) |

---

## One-Liner (Tests + Imports)

```bash
python -m pytest tests/test_smoke.py tests/test_ddl_generator.py -q && python -c "from src.etl_orchestrator import run_etl; from src.staging.etl_config import process_etl_config; print('OK')"
```

---

## Known Test Failures (Full Suite)

| Test | Reason |
|------|--------|
| `test_etl_config_missing_file_graceful_skip` | Path.glob is read-only, mock approach needs change |
| `test_mapping_column_selection_handles_missing_columns` | Test expects extra columns dropped; not implemented |
| `test_orchestrator_only_runs_business_sources` | Mock returns 2-tuple; orchestrator expects 4+ columns |
| `test_process_source_truncates_ods` | Needs Excel + DB; integration test |
| `test_deduplication_logs_duplicates` | Mock DataFrame missing Source_Name column |
