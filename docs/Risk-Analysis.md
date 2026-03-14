# SISRI ETL – Risk Analysis

**Last updated:** Post-SQL-injection mitigations (2024)

---

## 1. Security

| Risk | Severity | Status | Notes |
|------|----------|--------|-------|
| **SQL injection** | ~~High~~ | **Mitigated** | `truncate_table` and `execute_proc` validate identifiers; `params_dict` used for parameterized execution. |
| **Legacy `params` string** | Low | Open | `execute_proc(..., params=...)` still accepts raw strings; no callers use it. Consider deprecation or removal. |
| **Credential exposure** | Medium | Open | `config.yaml` uses `${SQLSERVER_PASSWORD}`. If `.env` is committed or exposed, credentials leak. |
| **BCP / `upload_via_bcp`** | Low | Open | `table` parameter passed to shell; BCP uses `subprocess` with config. No direct SQL injection. |

---

## 2. Data Integrity

| Risk | Severity | Description | Mitigation |
|------|----------|-------------|------------|
| **Truncate before load** | Medium | `truncate_table` before BCP. Failed load leaves table empty. | Add pre-load backup or staging → swap pattern. |
| **No transaction around BCP** | Medium | BCP runs outside Python transactions. Partial load + failure leaves inconsistent state. | Document behavior; consider staging tables and swap. |
| **Rejected rows** | Low | `RejectedRowsHandler` logs rejects. No automatic retry or alerting. | Add alerts on reject count; review `ETL.Fact_Rejected_Rows` regularly. |
| **Checkpoint before merge** | Low | `Last_Successful_Load_Datetime` updated after merge. If merge fails, checkpoint may not reflect last good state. | Align checkpoint with actual success; consider pre-merge checkpoint. |

---

## 3. DDL and Schema Changes

| Risk | Severity | Description | Mitigation |
|------|----------|-------------|------------|
| **ODS DDL DROP TABLE** | High | `generate_ods_table_ddl` uses `DROP TABLE` before `CREATE`. Auto-run can drop and recreate ODS tables. | Already limited to ODS; ensure run folder is access-controlled. |
| **Auto-execute pattern matching** | Medium | `_is_auto_executable` uses string checks. Malformed or malicious SQL could be misclassified. | Add stricter checks (e.g. regex) or explicit allowlist of script names. |
| **Script order** | Low | Scripts run in filename order. Dependencies (e.g. proc before table) may be wrong. | Use naming convention (e.g. `01_`, `02_`) or explicit ordering. |
| **Generated DDL overwrites** | Low | `etl_config` clears `generated/` before writing. Old scripts are lost. | Archive before clear or use versioned filenames. |

---

## 4. Operational

| Risk | Severity | Description | Mitigation |
|------|----------|-------------|------------|
| **Fail-fast stops all** | Medium | Orchestrator stops on first source failure. Later sources are not processed. | Document restart behavior; consider `--from-source` or resume logic. |
| **Temp directory wipe** | Low | `shutil.rmtree(temp_dir)` at start. In-progress work in temp is lost. | Ensure temp is only for transient data; avoid long-running temp usage. |
| **Excel config lock** | Low | Open Excel file can cause read errors. | Document: close Excel before config load. |
| **BCP format drift** | Medium | Format file from `Dim_Source_Imports_Mapping`. Mapping changes can invalidate format. | Regenerate format on mapping change; add validation step. |

---

## 5. Configuration and Metadata

| Risk | Severity | Description | Mitigation |
|------|----------|-------------|------------|
| **Excel as source of truth** | Medium | Config in Excel; typos, bad formulas, or bad sheet names break loads. | Add validation (schema, required columns); consider versioned config. |
| **Config load truncates metadata** | High | `truncate_table` on `Source_Imports`, `Source_File_Mapping`, `DW_Mapping_And_Transformations` before BCP. Failed BCP leaves metadata empty. | Load to staging tables first, then merge; or use transactional load. |
| **Hardcoded paths** | Low | `file_root: "C:/SISRI"` – environment-specific. | Use env vars or per-environment config. |

---

## 6. Data Quality

| Risk | Severity | Description | Mitigation |
|------|----------|-------------|------------|
| **VARCHAR truncation** | Low | `fact_sales_import` truncates to max length. Data loss is silent. | Log truncation; consider reject or warning. |
| **DECIMAL/INT coercion** | Medium | Invalid values coerced to 0 or NaN. Reject count can be wrong (`invalid_mask` logic). | Tighten validation; ensure reject count is correct. |
| **Date format assumptions** | Low | Multiple formats (105, 23, 101, 111). Unusual formats may fail. | Document supported formats; add validation. |

---

## 7. Audit and Traceability

| Risk | Severity | Description | Mitigation |
|------|----------|-------------|------------|
| **Audit on failure** | Low | `log_audit_source_import` on failure. Success path may not log row counts for all steps. | Ensure all key steps write to audit. |
| **DDL execution not audited** | Low | `apply_ddl_from_run` / `script_executor` do not write to `Fact_Audit_Source_Imports`. | Add audit record for DDL runs. |
| **Archive retention** | Low | DDL archived with timestamp. No retention policy. | Define retention and cleanup. |

---

## 8. Summary – Highest Priority

| Priority | Risk | Action |
|----------|------|--------|
| 1 | Config truncate + BCP failure leaves metadata empty | Load config into staging first, then merge; or wrap in transaction. |
| 2 | Truncate-then-load data loss | Add staging/backup strategy for critical tables. |
| 3 | Auto-execute DDL pattern false positives | Tighten `_is_auto_executable`; consider explicit allowlist. |
| 4 | Credential exposure | Keep `.env` in `.gitignore`; use secrets manager in production. |
| 5 | DECIMAL/INT coercion and reject count | Tighten validation; ensure reject count is correct. |

---

## 9. Mitigations Implemented

| Item | Before | After |
|------|--------|-------|
| **SQL injection** | High | **Mitigated** – identifier validation and `params_dict` |
| **`truncate_table`** | f-string with table name | `_validate_sql_identifier()` before use |
| **`execute_proc`** | f-string with params | `params_dict` for parameterized execution |
| **Call sites** | `params=f"@SourceName='{source_name}', ..."` | `params_dict={"@SourceName": source_name, ...}` |
| **Priority 1 (original)** | SQL injection | **Resolved** |

---

## 10. DDL Auto-Execute Rules (Combo Approach)

| Category | Auto-executed? | Location |
|----------|----------------|----------|
| ODS tables | Yes | `CREATE TABLE [ODS].*` |
| ETL staging tables | Yes | `CREATE TABLE [ETL].[Staging_*]` |
| Conformed staging merge procs | Yes | `CREATE PROCEDURE` + `INSERT INTO [ETL].[Staging_Fact*]` |
| DW tables | No (human review) | `CREATE TABLE [DW].*` |
| DW merge procs | No (human review) | `SP_Merge_Dim_*` |
