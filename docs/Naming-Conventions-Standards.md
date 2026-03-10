# Naming Conventions & Field Population Standards

## 🎯 Overview
This document defines naming conventions and field population standards for the SISRI ETL system to ensure consistency, automation, and proper object generation.

## 📋 Naming Conventions

### **🏗️ Database Objects**

#### **Source Names**
- **Format:** `PascalCase` (first letter capital, rest capital)
- **Examples:** `Sales_Format_1`, `Brands`, `Places`, `Avid`, `TestSource`
- **Pattern:** `[BusinessEntity]_[Format_Version]` or `[BusinessEntity]`
- **No spaces or special characters** except underscores

#### **ODS Tables**
- **Format:** `[ODS].[{Source_Name}]`
- **Examples:** `[ODS].[Sales_Format_1]`, `[ODS].[Avid]`, `[ODS].[Brands]`
- **Schema:** Always `ODS` (Operational Data Store)
- **Purpose:** Raw staging area for source data

#### **Conformed Staging Tables**
- **Format:** `[ETL].[Staging_Fact_{BusinessEntity}_Conformed]`
- **Examples:** `[ETL].[Staging_Fact_Sales_Conformed]`
- **Schema:** Always `ETL` (Extract-Transform-Load)
- **Purpose:** Cleaned, conformed data ready for DW

#### **DW Tables (Data Warehouse)**
- **Format:** `[DW].[Fact_{BusinessEntity}]` or `[DW].[Dim_{BusinessEntity}]`
- **Examples:** `[DW].[Fact_Sales]`, `[DW].[Dim_Products]`
- **Schema:** Always `DW` (Data Warehouse)
- **Purpose:** Final dimensional/fact tables

#### **Stored Procedures**
- **Format:** `[ETL].[SP_Merge_{Source}_{Source}_{Target}]`
- **Examples:** `[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]`
- **Prefix:** `SP_` (Stored Procedure)
- **Naming:** Verb_Object_From_To_Target

### **📁 File Paths**

#### **Relative Paths**
- **Format:** `raw/DATA/{Category}/{Subcategory}/{Source_Name}`
- **Examples:** 
  - `raw/DATA/SALES/CLICKS/AVID`
  - `raw/DATA/DIMENSIONS/BRANDS/Brands`
  - `raw/DATA/SALES/FORMATS/Sales_Format_1`
- **Backslashes:** Use `\` for Windows paths
- **Case:** Match actual folder structure exactly

#### **File Patterns**
- **Excel:** `*.xlsx`
- **CSV:** `*.csv`
- **All files:** `*.*`

#### **Sheet Names**
- **Default:** `Sheet1`
- **Alternatives:** `Data`, `RawData`, `SalesData`
- **Case-sensitive:** Must match Excel sheet exactly

## 📊 Field Population Standards

### **🎯 Source_Imports Sheet - Required Fields**

#### **🔥 Critical Fields (Must Populate)**
| Field | Required | Default | Auto-Generate | Notes |
|--------|-----------|---------|----------------|-------|
| **Source_Name** | ✅ | Manual | Unique identifier, PascalCase |
| **Rel_Path** | ✅ | Manual | File path from raw/ folder |
| **Staging_Table** | ✅ | ✅ `[ODS].[{Source_Name}]` | **CRITICAL for DDL generation** |
| **Source_Type** | ✅ | Manual | `Dimension` or `Fact_Sales` |

#### **🔥 Fact_Sales Additional Required Fields**
| Field | Required | Default | Auto-Generate | Notes |
|--------|-----------|---------|----------------|-------|
| **DW_Table_Name** | ✅ | ✅ `[ETL].[Staging_Fact_Sales_Conformed]` | Conformed staging target |
| **Merge_Proc_Name** | ✅ | ✅ `[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]` | Merge procedure |
| **Is_Conformed_Target** | ✅ | ✅ `1` for Fact_Sales | Goes to conformed staging |

#### **⚙️ Standard Defaults (Auto-Generate)**
| Field | Default | Notes |
|--------|---------|-------|
| **Pattern** | `*.xlsx` | Most common file type |
| **Sheet_Name** | `Sheet1` | Most Excel default |
| **Processing_Order** | `MAX + 0.01` | Auto-increment with decimals |
| **Is_Active** | `1` | Enable source |
| **Is_Deleted** | `0` | Not deleted |
| **Archive_Action** | `COPY` | Copy files to archive |
| **Force_DDL_Generation** | `1` | Generate table DDL |
| **Inserted_Datetime** | `GETDATE()` | Timestamp |
| **Description** | `{Source_Name} Source` | Auto-description |

#### **🔧 Optional Fields (Can be NULL)**
| Field | Default | When to Use |
|--------|---------|-------------|
| **Archive_Rel_Path** | `NULL` | Custom archive location |
| **Rejected_Rel_Path** | `NULL` | Custom reject location |
| **Wholesaler_Code** | `NULL` | Wholesaler mapping |
| **Updated_Datetime** | `NULL` | Last update timestamp |
| **Last_Successful_Load_Datetime** | `NULL` | Success tracking |

### **🔄 DW_Mapping_And_Transformations Sheet**

#### **🎯 Required Fields**
| Field | Required | Notes |
|--------|-----------|-------|
| **Source_Name** | ✅ | Must match Source_Imports |
| **ODS_Column** | ✅ | Source column name |
| **Conformed_Column** | ✅ | Target column name |
| **Transformation_Type** | ✅ | `Direct` or `Expression` |
| **Is_Key** | ✅ | `1` for hash keys, `0` otherwise |
| **Is_Required** | ✅ | `1` for required, `0` otherwise |

#### **⚙️ Standard Defaults**
| Field | Default | Notes |
|--------|---------|-------|
| **Transformation_Rule** | `[Column_Name]` | For Direct type |
| **Default_Value** | `UNKNOWN` or `0` | Depends on data type |
| **Validation_Rule** | `Required` or empty | Validation logic |
| **Sequence_Order** | Auto-increment | Processing order |
| **Description** | Auto-generated | Transformation description |
| **Inserted_Datetime** | `GETDATE()` | Timestamp |

## 🏷️ Column Naming Standards

### **📊 ODS Table Columns**
- **Source Columns:** Match source file exactly (preserve original names)
- **Audit Columns:** Always add these 5 columns:
  - `Inserted_Datetime` (DATETIME2) - When loaded
  - `Audit_Source_Import_SK` (INT) - Load batch ID  
  - `Source_File_Archive_SK` (INT) - File archive ID
- **Primary Keys:** Usually composite of business keys
- **Data Types:** Match source data (VARCHAR for text, DECIMAL for numbers)

### **📊 Conformed Staging Columns**
| Column | Data Type | Purpose |
|---------|-----------|---------|
| **Date_SK** | INT | Date surrogate key (yyyyMMdd) |
| **Place_Code** | VARCHAR(50) | Store/location identifier |
| **Product_Code** | VARCHAR(50) | Product identifier |
| **Sales_Quantity** | DECIMAL(18,4) | Sales quantity |
| **Total_Amount_Source** | DECIMAL(18,4) | Sales amount |
| **Unit_Cost_Price** | DECIMAL(18,4) | Cost per unit |
| **Barcode** | VARCHAR(50) | Product barcode |
| **RawRowHash** | VARBINARY(32) | SHA2_256 hash of keys |
| **Validation_Message** | VARCHAR(MAX) | Validation status/errors |
| **Inserted_Datetime** | DATETIME2 | Row creation timestamp |
| **Audit_Source_Import_SK** | INT | Load batch identifier |
| **Source_File_Archive_SK** | INT | File archive identifier |

## 🔄 Processing Order Logic

### **📊 Standard Sequence**
```
0.01-0.99: System tables (Source_Imports, etc.)
1.01-1.99: Dimension sources (Brands, Places, etc.)
2.01-2.99: Fact_Sales sources (Sales_Format_1, Avid, etc.)
3.01-3.99: Conformed targets (Staging_Fact_Sales_Conformed)
4.01+:   DW targets (Fact_Sales, etc.)
```

### **🔢 Auto-Generation Rules**
- **Find MAX:** `MAX(CAST(Processing_Order AS DECIMAL(10,2)))`
- **Add 0.01:** Simple increment for next available slot
- **Format:** Always 2 decimal places (`{next_order:.2f}`)

## ✅ Validation Rules

### **🔍 Field Validation**
- **Source_Name:** Must be unique, PascalCase, no special chars
- **Rel_Path:** Must exist, use backslashes, start with `raw/`
- **Staging_Table:** Must follow `[ODS].[{Source_Name}]` pattern
- **Source_Type:** Must be `Dimension` or `Fact_Sales`
- **Processing_Order:** Must be numeric, unique, follow sequence

### **🚨 Error Prevention**
- **NULL Staging_Table:** Prevents ODS table creation
- **Missing DW_Table_Name:** Prevents conformed staging for Fact_Sales
- **Wrong Processing_Order:** Can cause execution sequence issues
- **Invalid Source_Type:** Breaks pipeline logic

## 📋 Quick Reference

### **🎯 Minimum New Source Checklist**
- [ ] **Source_Name:** PascalCase, unique
- [ ] **Rel_Path:** Exists, starts with `raw/`
- [ ] **Source_Type:** `Dimension` or `Fact_Sales`
- [ ] **Staging_Table:** `[ODS].[{Source_Name}]` (auto-gen)
- [ ] **Processing_Order:** Next in sequence (auto-gen)

### **🎯 Fact_Sales Additional Checklist**
- [ ] **DW_Table_Name:** `[ETL].[Staging_Fact_Sales_Conformed]`
- [ ] **Merge_Proc_Name:** `[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]`
- [ ] **Is_Conformed_Target:** `1`

### **🎯 Dimension Additional Checklist**
- [ ] **DW_Table_Name:** `[DW].[Dim_{Source_Name}]`
- [ ] **Merge_Proc_Name:** `[ETL].[SP_Merge_{Source_Name}_ODS_to_DW]`
- [ ] **Is_Conformed_Target:** `0`

## 🔄 Automation Rules

### **🤖 Script Behavior**
- **Preserve Schema:** Never modify existing column structure
- **Add Rows Only:** Insert new data, don't change table structure
- **Backup First:** Always create timestamped backup before changes
- **Validate:** Check required fields before processing
- **Rollback:** Restore from backup on error

### **📁 File Handling**
- **Lock Detection:** Handle Excel file locks gracefully
- **Permission Errors:** Retry after short delay
- **Backup Strategy:** Multiple timestamped backups
- **Sheet Preservation:** Maintain all existing sheets

## 🚨 Troubleshooting

### **🔍 Common Issues**
1. **Staging_Table = NULL:** ODS table won't be created
2. **Wrong Processing_Order:** Sources execute in wrong sequence
3. **Missing DW_Table_Name:** Fact_Sales won't go to conformed staging
4. **Schema Mismatch:** Excel vs database column differences
5. **File Locks:** Excel open during script execution

### **🛠️ Quick Fixes**
```sql
-- Fix missing Staging_Table
UPDATE [ETL].[Dim_Source_Imports] 
SET Staging_Table = '[ODS].[Sales_Format_2]' 
WHERE Source_Name = 'Sales_Format_2';

-- Fix wrong Processing_Order
UPDATE [ETL].[Dim_Source_Imports] 
SET Processing_Order = '2.02' 
WHERE Source_Name = 'Sales_Format_2';
```

## 📞 Support

### **🔧 Debug Commands**
```bash
# Check current sources
python -c "import pandas as pd; print(pd.read_excel('config/etl_config.xlsx', sheet_name='Source_Imports', engine='openpyxl')[['Source_Name', 'Staging_Table', 'Processing_Order']])"

# Validate processing order
python -c "import pandas as pd; df = pd.read_excel('config/etl_config.xlsx', sheet_name='Source_Imports', engine='openpyxl'); print(df['Processing_Order'].sort_values())"
```

### **📋 Documentation Updates**
- Update this document when adding new conventions
- Maintain version history of changes
- Include examples for new source types
- Document any special cases or exceptions

---

**Version:** 1.0  
**Created:** 2026-03-11  
**Purpose:** Standardize naming conventions and field population for consistent automation
