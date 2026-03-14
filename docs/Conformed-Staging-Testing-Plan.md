# Conformed Staging Testing Plan

## 🎯 **Objective**
Fix conformed staging merge procedure to enable complete end-to-end ETL pipeline for Sales_Format_2

## 📋 **Current Status**
- ✅ **ETL Config Loading** - Working perfectly (1306 rows processed)
- ✅ **BCP Operations** - All successful  
- ✅ **ODS Loading** - Working (34,623 rows loaded)
- ❌ **Conformed Merge** - Column name mismatches need fixing

## 🔍 **Root Cause Analysis**
The generated merge procedure has **invalid column names with spaces**:
- `'Product Name'` should be `'product_name'`
- `'Store Name'` should be `'store_name'`  
- `'total_amount'` should be `'total_amount_source'`
- `'cost'` should be `'unit_cost_price'`

## 📊 **Database vs Generated Column Mismatch**

### ETL.Staging_Fact_Sales_Conformed (Actual Columns)
1. Date_SK (NOT NULL)
2. Place_Code 
3. Product_Code
4. Barcode
5. Sales_Quantity
6. Unit_Price
7. Unit_Cost_Price
8. Total_Amount_Source
9. Validation_Message
10. Inserted_Datetime
11. Source_File_Archive_SK
12. Audit_Source_Import_SK
13. RawRowHash

### Generated Merge Procedure (Invalid Names)
- `'Product Name'` ❌ → should be `'product_name'` ✅
- `'Store Name'` ❌ → should be `'store_name'` ✅
- `'total_amount'` ❌ → should be `'total_amount_source'` ✅
- `'cost'` ❌ → should be `'unit_cost_price'` ✅

## 🚀 **Testing Plan Steps**

### Step 1: Fix Column Name Mapping
- [ ] Update `src/dw/ddl_generator.py` to use snake_case column names (if needed)
- [ ] Ensure all generated column names match `Staging_Fact_Sales_Conformed` structure
- [ ] Test merge procedure generation

### Step 2: Manual Merge Procedure Test
- [ ] Create simple working merge procedure manually
- [ ] Test with Sales_Format_2 ODS data
- [ ] Verify all columns populate correctly
- [ ] Check Date_SK handling (NOT NULL requirement)

### Step 3: End-to-End Pipeline Test
- [ ] Run full ETL pipeline: Config → ODS → Conformed
- [ ] Verify 34,623 rows flow through all stages
- [ ] Check conformed staging row counts
- [ ] Validate data quality and transformations

### Step 4: Production Readiness
- [ ] Test with multiple sources (Sales_Format_1 + Sales_Format_2)
- [ ] Verify merge procedure reusability
- [ ] Performance testing
- [ ] Documentation updates

## 🎯 **Success Criteria**
- [ ] Conformed staging merge executes without errors
- [ ] All 34,623 ODS rows load to conformed staging
- [ ] Column names match database schema exactly
- [ ] Date_SK populated correctly (NOT NULL)
- [ ] Full pipeline runs in under 10 seconds

## 📋 **Known Issues to Address**
1. **Date_SK Population** - Need proper date lookup from Dim_Date table
2. **Column Name Mismatch** - Generated names don't match database
3. **NULL Handling** - Date_SK doesn't allow NULLs
4. **Data Type Conversion** - Ensure proper decimal/varchar handling

## 🔧 **Technical Notes**
- **Target Table**: `[ETL].[Staging_Fact_Sales_Conformed]`
- **Source Table**: `[ODS].[Sales_Format_2]`
- **Key Columns**: Date_SK, Place_Code, Product_Code
- **Snake_case Required**: All column names must be snake_case

---
*Created: 2026-03-11*
*Status: Ready for Implementation*
*See also: docs/Next-Steps-Plan.md for Dim_Products conformed dimension extension*
