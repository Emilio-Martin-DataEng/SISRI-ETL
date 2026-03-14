# SISRI ETL – Next Steps Plan

**Created:** 2026-02-28  
**Scope:** Fix Fact_Sales pipeline, introduce Dim_Products as conformed dimension, metadata alignment, documentation, and Cursor rules.

---

## 1. Current State Summary

### 1.1 Database & Metadata

| Area | Status | Notes |
|------|--------|-------|
| **ETL.Dim_Source_Imports** | ✅ | Source_Type: Dimension, Fact_Sales, Fact_Conformed, System |
| **ETL.Dim_DW_Mapping_And_Transformations** | ✅ | ODS→Conformed mapping for Fact_Sales (Sales_Format_1, Sales_Format_2) |
| **ODS tables** | ✅ | Products, Brands, Principals, Wholesalers, Places, Sales_Format_1, Sales_Format_2 |
| **DW dimensions** | ✅ | Dim_Principals, Dim_Brands, Dim_Wholesalers, Dim_Products, Dim_Places |
| **ETL.Staging_Fact_Sales_Conformed** | ✅ | 16,103 rows (from Sales_Format_1) |
| **DW.Fact_Sales** | ⚠️ | Not yet populated from conformed staging |

### 1.2 Known Issues

1. ~~**SP_Merge_Fact_Sales_ODS_to_Conformed**~~ – ✅ Fixed (temp table approach, dynamic format)
2. ~~**Date_SK conversion**~~ – ✅ Fixed (join on conformed types)
3. ~~**Staging_Fact_Sales_Conformed deduplication**~~ – ✅ Fixed (DELETE before INSERT per source)
4. **Dim_Products** – Currently single-source (ODS.Products); will become conformed (multiple sources → conformed staging → DW)

### 1.3 Metadata Tables

| Table | Purpose |
|-------|---------|
| ETL.Source_Imports | Staging for config load |
| ETL.Source_File_Mapping | Staging for column mapping |
| ETL.DW_Mapping_And_Transformations | Staging for conformed mapping (Fact + future Dim conformed) |
| ETL.Dim_Source_Imports | Merged source definitions |
| ETL.Dim_Source_Imports_Mapping | Merged column mapping |
| ETL.Dim_DW_Mapping_And_Transformations | Merged conformed mapping |

---

## 2. Phase 1 – Fix Fact_Sales Pipeline (Immediate)

### 2.1 Fix SP_Merge_Fact_Sales_ODS_to_Conformed

- **Root cause:** `COALESCE` syntax in dynamic SQL (e.g. `ISNULL(QUOTENAME(Default_Value), 'NULL')` produces `'NULL'` when Default_Value is empty; SQL expects literal NULL or valid expression)
- **Fix:** In `generate_fact_to_conformed_merge_ddl`, ensure:
  - `Default_Value` for numeric columns uses `0` not `'NULL'`
  - Expression type: `Transformation_Rule` must reference `s.[ODS_Column]` not bare `[ODS_Column]`
  - Validate `DW_Mapping_And_Transformations` rows for both Sales_Format_1 and Sales_Format_2

### 2.2 Fix Date_SK Conversion

- **Root cause:** Source dates in `dd-MM-yyyy` vs `yyyy/MM/dd`; format codes in config may not match
- **Fix:** Ensure `Transformation_Type = 'Date_SK'` uses correct format codes per source (105 for dd-MM-yyyy, 23 for yyyy-MM-dd, 111 for yyyy/MM/dd)
- **Validation:** Add `TRY_CONVERT` fallbacks for all formats in generated proc

### 2.3 Conformed Staging Deduplication

- **Issue:** NK_Staging_Fact_Sales_Conformed duplicate key when both formats load same logical row
- **Fix:** Ensure DELETE before INSERT uses correct join keys; consider truncate-per-source vs delete-by-source-batch

### 2.4 Activate Fact_Conformed

- **Current:** `Staging_Fact_Sales_Conformed` row exists in `Dim_Source_Imports` with `Is_Active = 0`
- **Action:** Set `Is_Active = 1` for `Staging_Fact_Sales_Conformed` so `ETL.SP_Merge_Fact_Sales` runs after fact sources

---

## 3. Phase 2 – Dim_Products as Conformed Dimension

### 3.1 Design

- **Pattern:** Same engine as Fact_Sales
- **Flow:** Multiple ODS sources → `ETL.Staging_Dim_Products_Conformed` → `DW.Dim_Products`
- **Sources:** `Products` (existing) + future sources (e.g. `Products_Format_2`, `Products_Alt`) with different column layouts

### 3.2 Metadata Extensions

| Change | Location | Details |
|--------|----------|---------|
| **Source_Type** | ETL_Config.xlsx, Dim_Source_Imports | Add `Dimension_Conformed` |
| **Conformed target** | Source_Imports | `Staging_Table` = `[ETL].[Staging_Dim_Products_Conformed]` for Products sources |
| **Conformed merge proc** | Source_Imports | `Merge_Proc_Name` = `[ETL].[SP_Merge_Dim_Products_ODS_to_Conformed]` |
| **Conformed mapping** | DW_Mapping_And_Transformations sheet | Add rows for Products → conformed columns (same schema as Fact) |

### 3.3 New Config Sheet / Reuse

- **Option A:** Reuse `DW_Mapping_And_Transformations` with `Source_Name` = `Products` (and future product sources)
- **Option B:** Add `Dim_Conformed_Mapping` sheet for dimension-specific conformed mappings
- **Recommendation:** Option A – single schema, filter by `Source_Name`; add `Conformed_Target` column if needed (Fact vs Dim)

### 3.4 DDL Generator Extensions

- **New function:** `generate_dim_to_conformed_merge_ddl(source_name, ods_table, conformed_table, mapping_rows)`
- **Logic:** Similar to `generate_fact_to_conformed_merge_ddl` but:
  - Target: `ETL.Staging_Dim_Products_Conformed`
  - Columns: conformed dimension attributes (no Date_SK; include SCD attributes if needed)
- **Staging table DDL:** `ETL.Staging_Dim_Products_Conformed` – create from conformed column list

### 3.5 Orchestrator Changes

- **New source type:** `Dimension_Conformed`
- **Processing:** `Dimension_Conformed` sources → run `SP_Merge_Dim_Products_ODS_to_Conformed` per source
- **Final step:** `Dimension_Conformed_Target` (like Fact_Conformed) → `SP_Merge_Dim_Products` (conformed staging → DW)

### 3.6 Processing Order

```text
1.0–1.2   Dimension (Principals, Brands, Wholesalers)
1.3       Dimension_Conformed sources (Products_Format_1, Products_Format_2, …)
1.31      Dimension_Conformed_Target (Staging_Dim_Products_Conformed → DW.Dim_Products)
2.0+      Fact_Sales
3.0       Fact_Conformed
```

---

## 4. Phase 3 – Metadata Alignment

### 4.1 ETL_Config.xlsx

- **Source_Imports:** Add columns if needed: `Conformed_Target_Type` (Fact | Dimension)
- **DW_Mapping_And_Transformations:** Ensure columns: `Source_Name`, `ODS_Column`, `Conformed_Column`, `Transformation_Type`, `Transformation_Rule`, `Is_Key`, `Is_Required`, `Default_Value`, `Sequence_Order`

### 4.2 Database Tables

- **Dim_Source_Imports:** Add `Conformed_Target_Type` (optional) to distinguish Fact vs Dim conformed targets
- **Dim_DW_Mapping_And_Transformations:** Ensure supports both Fact and Dimension conformed mappings

### 4.3 Format Files

- `config/format/system/dw_mapping_and_transformations.fmt` – verify column order matches Excel

---

## 5. Phase 4 – Documentation Updates

- **Next-Steps-Plan.md** – this document
- **README.md** – link to Next-Steps-Plan, mention Dim_Products conformed
- **App-Structure-Guide.md** – add Dimension_Conformed flow, source types
- **Technical-Overview.md** – add Dimension_Conformed, conformed dimension engine
- **System-Architecture.md** – add conformed dimension flow
- **Admin-Instruction-Manual.md** – add Dimension_Conformed monitoring
- **Adding-New-Sources-Guide.md** – add Dimension_Conformed, conformed dimension sources
- **Naming-Conventions-Standards.md** – add Dimension_Conformed, conformed staging naming
- **Kimball-Fact-Engine-Plan.md** – add conformed dimension extension
- **Conformed-Staging-Testing-Plan.md** – extend to Dim_Products conformed
- **Check-In-Checklist.md** – add conformed dimension smoke tests
- **Documentation-Summary.md** – add Next-Steps-Plan, conformed dimension

---

## 6. Phase 5 – Cursor Rules & AGENTS

### 6.1 `.cursor/rules/` (per create-rule skill)

| Rule | Purpose |
|------|---------|
| `sisri-etl-standards.mdc` | Core Python, SQL, logging, error handling |
| `sisri-ddl-conventions.mdc` | DDL generator, DW_DDL paths, naming |
| `sisri-docs-conventions.mdc` | Doc structure, naming, updates |

### 6.2 AGENTS.md

- Project context for AI
- Key paths, source types, conventions
- When to use `--refresh-metadata`, `--force-ddl`

---

## 7. Implementation Order

| Priority | Task | Deliverable | Status |
|----------|------|-------------|--------|
| P0 | Fix SP_Merge_Fact_Sales_ODS_to_Conformed | Temp table, dynamic format | ✅ Done |
| P0 | Fix Date_SK conversion | Join on conformed types | ✅ Done |
| P0 | Activate Fact_Conformed in config | SP_Merge_Fact_Sales runs | Pending |
| P1 | Create Next-Steps-Plan.md | This doc |
| P1 | Update all documentation | Updated docs |
| P1 | Create .cursor/rules/ and AGENTS.md | Cursor rules |
| P2 | Add Dimension_Conformed source type | Orchestrator + etl_config |
| P2 | Add Dim_Products conformed metadata | Config + DB |
| P2 | Implement generate_dim_to_conformed_merge_ddl | ddl_generator.py |
| P2 | Create Staging_Dim_Products_Conformed | DDL + proc |
| P3 | Add second Products source (e.g. Products_Format_2) | End-to-end test |

---

## 8. Success Criteria

- [x] SP_Merge_Fact_Sales_ODS_to_Conformed executes without syntax error
- [x] Sales_Format_1 and Sales_Format_2 both load to Staging_Fact_Sales_Conformed
- [ ] ETL.SP_Merge_Fact_Sales populates DW.Fact_Sales from conformed staging
- [ ] Dim_Products can be configured as conformed dimension
- [ ] Multiple product sources → conformed staging → DW.Dim_Products
- [ ] All documentation updated and linked
- [ ] .cursor rules and AGENTS.md in place

---

## 9. References

- `docs/Check-In-Checklist.md` – check-in steps
- `docs/Kimball-Fact-Engine-Plan.md` – fact engine design
- `docs/Conformed-Staging-Testing-Plan.md` – conformed testing
- `docs/Risk-Analysis.md` – risk considerations
- `src/dw/ddl_generator.py` – DDL generation
- `src/staging/etl_config.py` – config load and DDL trigger
