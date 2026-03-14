# Adding New Sources Guide

## 🎯 Overview
This guide provides the minimum requirements and automated approach for adding new data sources to the SISRI ETL system.

## 📋 Minimum Requirements

### **Absolute Minimum for New Source:**
1. **Source_Name** - Unique identifier (e.g., "Avid", "NewBrand")
2. **Rel_Path** - File path from raw folder (e.g., "raw/DATA/SALES/CLICKS/AVID")
3. **Source_Type** - "Dimension", "Dimension_Conformed", "Fact_Sales", or "Fact_Conformed"

### **Everything Else Can Be Auto-Generated!**

## 🚀 Quick Start - Add New Source in 3 Commands

### **Step 1: Add Source to Database (Minimum Info)**
```python
# Run this Python script with just 3 parameters
python -c "
import sys
sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection

conn = get_connection()
cursor = conn.cursor()

# MINIMUM REQUIRED INFO - CHANGE THESE 3 VALUES:
source_name = 'YourSourceName'      # ← CHANGE
rel_path = 'raw/DATA/YOUR/PATH'     # ← CHANGE  
source_type = 'Fact_Sales'           # ← CHANGE (Dimension or Fact_Sales)

# Auto-generated defaults
staging_table = f'[ODS].[{source_name}]'
processing_order = str(int(cursor.execute('SELECT MAX(CAST(Processing_Order AS INT)) FROM ETL.Dim_Source_Imports').fetchone()[0] or 0) + 1)

cursor.execute(f'''
INSERT INTO ETL.Dim_Source_Imports (
    Source_Name, Rel_Path, Pattern, Sheet_Name, Staging_Table, Processing_Order,
    Is_Active, Is_Deleted, Description, Inserted_Datetime, Updated_Datetime,
    Archive_Action, DW_Table_Name, Merge_Proc_Name, Force_DDL_Generation,
    Source_Type, Is_Conformed_Target
) VALUES (
    '{source_name}', '{rel_path}', '*.xlsx', 'Sheet1', '{staging_table}', '{processing_order}',
    '1', '0', '{source_name} Source', GETDATE(), NULL,
    'COPY', 
    CASE WHEN '{source_type}' = 'Fact_Sales' THEN '[ETL].[Staging_Fact_Sales_Conformed]' ELSE NULL END,
    CASE WHEN '{source_type}' = 'Fact_Sales' THEN '[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]' ELSE NULL END,
    '1', '{source_type}', 
    CASE WHEN '{source_type}' = 'Fact_Sales' THEN '1' ELSE '0' END
)
''')

conn.commit()
print(f'Added source: {source_name}')
cursor.close()
conn.close()
"
```

### **Step 2: Generate Excel Configuration**
```python
# Auto-generate Excel entries for the new source
python -c "
import sys
sys.path.append('c:/Users/Emilio/SISRI')
import pandas as pd
from datetime import datetime

# CHANGE THIS:
source_name = 'YourSourceName'  # ← CHANGE
source_type = 'Fact_Sales'       # ← CHANGE

# Load existing config
df_imports = pd.read_excel('c:/Users/Emilio/SISRI/config/ETL_Config.xlsx', sheet_name='Source_Imports', engine='openpyxl')

# Add new source with defaults
new_import = {
    'Source_Name': source_name,
    'Rel_Path': f'raw/DATA/SALES/CLICKS/{source_name}',
    'Pattern': '*.xlsx',
    'Sheet_Name': 'Sheet1', 
    'Staging_Table': f'[ODS].[{source_name}]',
    'Processing_Order': str(len(df_imports) + 1),
    'Is_Active': '1',
    'Is_Deleted': '0',
    'Description': f'{source_name} Source',
    'Inserted_Datetime': datetime.now(),
    'Archive_Action': 'COPY',
    'Source_Type': source_type,
    'Is_Conformed_Target': '1' if source_type == 'Fact_Sales' else '0',
    'DW_Table_Name': '[ETL].[Staging_Fact_Sales_Conformed]' if source_type == 'Fact_Sales' else None,
    'Merge_Proc_Name': '[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]' if source_type == 'Fact_Sales' else None,
    'Force_DDL_Generation': '1'
}

# Add to DataFrame
df_imports = pd.concat([df_imports, pd.DataFrame([new_import])], ignore_index=True)

# Save with timestamp backup
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
backup_file = f'c:/Users/Emilio/SISRI/config/etl_config_backup_{timestamp}.xlsx'
df_imports.to_excel(backup_file, sheet_name='Source_Imports', index=False)
print(f'Backup created: {backup_file}')

# Update main config
df_imports.to_excel('c:/Users/Emilio/SISRI/config/ETL_Config.xlsx', sheet_name='Source_Imports', index=False)
print(f'Added {source_name} to Excel config')
"
```

### **Step 3: Load Config and Test**
```bash
# Admin: load config, apply DDL, first load
python -m src.admin.load_config --force-ddl

# Run integrated scenario tests (required after config changes)
python -m src.etl_orchestrator --test full

# If successful, add conformed staging mappings (for Fact_Sales only)
# See Step 4 below...
```

## 📊 Step 4: Add Conformed Staging Mappings (Fact_Sales Only)

### **Auto-Generate Default Mappings**
```python
# Generate default conformed mappings for Fact_Sales sources
python -c "
import sys
sys.path.append('c:/Users/Emilio/SISRI')
import pandas as pd
from datetime import datetime

# CHANGE THIS:
source_name = 'YourSourceName'  # ← CHANGE

# Default Fact_Sales mappings (customize as needed)
default_mappings = [
    {
        'Source_Name': source_name,
        'ODS_Column': 'Barcode',
        'Conformed_Column': 'Barcode', 
        'Transformation_Type': 'Direct',
        'Transformation_Rule': '[Barcode]',
        'Is_Key': 1,
        'Is_Required': 1,
        'Default_Value': 'UNKNOWN',
        'Validation_Rule': 'Required',
        'Sequence_Order': 1,
        'Description': 'Direct pass-through'
    },
    {
        'Source_Name': source_name,
        'ODS_Column': 'Cost',
        'Conformed_Column': 'Unit_Cost_Price',
        'Transformation_Type': 'Direct', 
        'Transformation_Rule': '[Cost]',
        'Is_Key': 0,
        'Is_Required': 0,
        'Default_Value': '0',
        'Validation_Rule': '',
        'Sequence_Order': 2,
        'Description': 'Direct cost mapping'
    },
    {
        'Source_Name': source_name,
        'ODS_Column': 'Product Name',
        'Conformed_Column': 'Product_Code',
        'Transformation_Type': 'Expression',
        'Transformation_Rule': 'LEFT([Product Name], CHARINDEX(\" \", [Product Name] + \" \") - 1)',
        'Is_Key': 0,
        'Is_Required': 0,
        'Default_Value': 'UNKNOWN', 
        'Validation_Rule': '',
        'Sequence_Order': 3,
        'Description': 'Extract first word'
    },
    {
        'Source_Name': source_name,
        'ODS_Column': 'Sales',
        'Conformed_Column': 'Total_Amount_Source',
        'Transformation_Type': 'Direct',
        'Transformation_Rule': '[Sales]',
        'Is_Key': 0,
        'Is_Required': 1,
        'Default_Value': '0',
        'Validation_Rule': 'Required',
        'Sequence_Order': 4,
        'Description': 'Direct sales mapping'
    },
    {
        'Source_Name': source_name,
        'ODS_Column': 'Date',
        'Conformed_Column': 'Date_SK',
        'Transformation_Type': 'Expression',
        'Transformation_Rule': 'CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE,[Date], 105), 112))',
        'Is_Key': 1,
        'Is_Required': 1,
        'Default_Value': '19000101',
        'Validation_Rule': 'Valid date required',
        'Sequence_Order': 5,
        'Description': 'Date to integer key'
    },
    {
        'Source_Name': source_name,
        'ODS_Column': 'Sales Qty',
        'Conformed_Column': 'Sales_Quantity',
        'Transformation_Type': 'Direct',
        'Transformation_Rule': '[Sales Qty]',
        'Is_Key': 0,
        'Is_Required': 1,
        'Default_Value': '0',
        'Validation_Rule': 'Required',
        'Sequence_Order': 6,
        'Description': 'Direct quantity mapping'
    },
    {
        'Source_Name': source_name,
        'ODS_Column': 'Store Name',
        'Conformed_Column': 'Place_Code',
        'Transformation_Type': 'Direct',
        'Transformation_Rule': '[Store Name]',
        'Is_Key': 1,
        'Is_Required': 1,
        'Default_Value': 'UNKNOWN',
        'Validation_Rule': 'Required',
        'Sequence_Order': 7,
        'Description': 'Direct store mapping'
    }
]

# Load existing mappings
try:
    df_mappings = pd.read_excel('c:/Users/Emilio/SISRI/config/ETL_Config.xlsx', sheet_name='DW_Mapping_And_Transformations', engine='openpyxl')
except:
    df_mappings = pd.DataFrame()

# Add new mappings
df_mappings = pd.concat([df_mappings, pd.DataFrame(default_mappings)], ignore_index=True)

# Backup and save
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
df_mappings.to_excel(f'c:/Users/Emilio/SISRI/config/etl_config_mappings_backup_{timestamp}.xlsx', index=False)
df_mappings.to_excel('c:/Users/Emilio/SISRI/config/ETL_Config.xlsx', sheet_name='DW_Mapping_And_Transformations', index=False)

print(f'Added {len(default_mappings)} mappings for {source_name}')
"
```

## 🔄 Excel Archive Strategy

### **Automatic Backup System**
The system automatically archives Excel configs with timestamps:
- **Before run:** `etl_config_backup_YYYYMMDD_HHMMSS.xlsx`
- **After run:** Preserves with SK tracking

### **SK Tracking**
- **Audit_Source_Import_SK** tracks each config load
- **Source_Import_SK** links to source definitions
- **Archive preserves SK relationships**

## 📋 Column Mapping Reference

### **Common Fact_Sales Column Patterns:**
| Source Column | Conformed Column | Transformation |
|---------------|------------------|----------------|
| Date | Date_SK | Expression (date conversion) |
| Store Name | Place_Code | Direct |
| Product Name | Product_Code | Expression (first word) |
| Barcode | Barcode | Direct (Key) |
| Sales | Total_Amount_Source | Direct |
| Sales Qty | Sales_Quantity | Direct |
| Cost | Unit_Cost_Price | Direct |

### **Common Dimension Column Patterns:**
| Source Column | Target Column | Data Type |
|---------------|---------------|-----------|
| Code | Code | VARCHAR(50) |
| Name | Name | VARCHAR(255) |
| Description | Description | VARCHAR(MAX) |
| Active | Is_Active | BIT |

## ✅ Validation Checklist

### **Before Running:**
- [ ] Source files exist in specified path
- [ ] Excel config backed up
- [ ] Database entry created
- [ ] Format file will be auto-generated

### **After Running:**
- [ ] ODS table created successfully
- [ ] Data loaded without errors
- [ ] For Fact_Sales: Conformed staging works
- [ ] All SKs properly tracked
- [ ] **Run scenario tests:** `python -m src.etl_orchestrator --test full`

## 🚨 Troubleshooting

### **Common Issues:**
1. **File not found:** Check Rel_Path matches actual folder structure
2. **BCP errors:** Format file auto-generated, check column alignment
3. **Transformation errors:** Verify column names in mapping rules
4. **SK issues:** Ensure metadata refresh runs before processing

### **Quick Fixes:**
```bash
# Admin: load config, force DDL
python -m src.admin.load_config --force-ddl

# Run scenario tests (required after config changes)
python -m src.etl_orchestrator --test full

# Operator: test single source
python -m src.etl_orchestrator --sources YourSourceName

# Clear and retry
python -c "
import sys; sys.path.append('c:/Users/Emilio/SISRI')
from src.utils.db_ops import get_connection
conn = get_connection()
cursor = conn.cursor()
cursor.execute('DELETE FROM [ODS].[YourSourceName]')
conn.commit()
cursor.close()
conn.close()
"
```

## 📞 Support

For issues or questions:
1. Check this guide first
2. Review error logs in `logs/` folder
3. Verify SK tracking in audit tables
4. Test with sample data first
