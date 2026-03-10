#!/usr/bin/env python3
"""
Add New Source Script - Minimal Input Required
Usage: python add_new_source.py --name SourceName --path raw/DATA/SALES/CLICKS/SOURCE --type Fact_Sales
"""

import argparse
import sys
import pandas as pd
from datetime import datetime
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).parent.parent))
from src.utils.db_ops import get_connection

def backup_excel_config():
    """Create timestamped backup of Excel config with ALL sheets"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    config_file = Path('config/etl_config.xlsx')
    backup_file = Path(f'config/etl_config_backup_{timestamp}.xlsx')
    
    if config_file.exists():
        try:
            # Load ALL available sheets
            xl_file = pd.ExcelFile(config_file, engine='openpyxl')
            available_sheets = xl_file.sheet_names
            print(f"📁 Found sheets: {available_sheets}")
            
            all_sheets = {}
            for sheet_name in available_sheets:
                df = pd.read_excel(config_file, sheet_name=sheet_name, engine='openpyxl')
                all_sheets[sheet_name] = df
                print(f"✅ Backed up {sheet_name}: {len(df)} rows")
            
            # Create complete backup
            with pd.ExcelWriter(backup_file, engine='openpyxl') as writer:
                for sheet_name, df in all_sheets.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
            
            print(f"✅ Complete backup: {backup_file}")
            print(f"✅ File size: {backup_file.stat().st_size} bytes")
            return backup_file
            
        except Exception as e:
            print(f"❌ Backup error: {e}")
            return None
    else:
        print("❌ Config file not found")
        return None

def add_source_to_database(source_name, rel_path, source_type):
    """Add source to ETL.Dim_Source_Imports with defaults"""
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # Get next processing order (handle decimal string values)
        cursor.execute("SELECT Processing_Order FROM [ETL].[Dim_Source_Imports] WHERE ISNUMERIC(Processing_Order) = 1 ORDER BY CAST(Processing_Order AS DECIMAL(10,2)) DESC")
        max_order_row = cursor.fetchone()
        if max_order_row and max_order_row[0]:
            max_order = float(max_order_row[0])  # Convert decimal string to float
        else:
            max_order = 0.0
        
        # Find next available order (simple increment)
        next_order = max_order + 0.01
        processing_order = f"{next_order:.2f}"  # Format as 2 decimal places
        
        print(f"📊 Max order: {max_order}, Next order: {processing_order}")
        
        # Auto-generated values
        staging_table = f'[ODS].[{source_name}]'
        dw_table = '[ETL].[Staging_Fact_Sales_Conformed]' if source_type == 'Fact_Sales' else None
        merge_proc = '[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]' if source_type == 'Fact_Sales' else None
        is_conformed = '1' if source_type == 'Fact_Sales' else '0'
        
        cursor.execute(f'''
            INSERT INTO [ETL].[Dim_Source_Imports] (
                Source_Name, Rel_Path, Pattern, Sheet_Name, Staging_Table, Processing_Order,
                Is_Active, Is_Deleted, Description, Inserted_Datetime, Updated_Datetime,
                Archive_Action, DW_Table_Name, Merge_Proc_Name, Force_DDL_Generation,
                Source_Type, Is_Conformed_Target
            ) VALUES (
                '{source_name}', '{rel_path}', '*.xlsx', 'Sheet1', '{staging_table}', '{processing_order}',
                '1', '0', '{source_name} Source', GETDATE(), NULL,
                'COPY', 
                {'NULL' if dw_table is None else f"'{dw_table}'"}, 
                {'NULL' if merge_proc is None else f"'{merge_proc}'"},
                '1', '{source_type}', {is_conformed}
            )
        ''')
        
        conn.commit()
        print(f"✅ Added to database: {source_name}")
        return True
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return False
    finally:
        cursor.close()
        conn.close()

def add_to_excel_config(source_name, rel_path, source_type):
    """Add source to Excel configuration preserving ALL sheets"""
    config_file = Path('config/etl_config.xlsx')
    
    try:
        # Load ALL existing sheets
        xl_file = pd.ExcelFile(config_file, engine='openpyxl')
        all_sheets = {}
        for sheet_name in xl_file.sheet_names:
            df = pd.read_excel(config_file, sheet_name=sheet_name, engine='openpyxl')
            all_sheets[sheet_name] = df
            print(f"📁 Loaded {sheet_name}: {len(df)} rows")
        
        # Update Source_Imports sheet
        if 'Source_Imports' in all_sheets:
            df_imports = all_sheets['Source_Imports']
        else:
            df_imports = pd.DataFrame()
        
        # Create new entry with defaults
        new_import = {
            'Source_Name': source_name,
            'Rel_Path': rel_path,
            'Pattern': '*.xlsx',
            'Sheet_Name': 'Sheet1',
            'Staging_Table': f'[ODS].[{source_name}]',
            'Processing_Order': str(len(df_imports) + 1),
            'Is_Active': '1',
            'Is_Deleted': '0',
            'Description': f'{source_name} Source',
            'Inserted_Datetime': datetime.now(),
            'Updated_Datetime': None,
            'Archive_Action': 'COPY',
            'Archive_Rel_Path': None,
            'Rejected_Rel_Path': None,
            'DW_Table_Name': '[ETL].[Staging_Fact_Sales_Conformed]' if source_type == 'Fact_Sales' else None,
            'Merge_Proc_Name': '[ETL].[SP_Merge_Fact_Sales_ODS_to_Conformed]' if source_type == 'Fact_Sales' else None,
            'Force_DDL_Generation': '1',
            'Last_Successful_Load_Datetime': None,
            'Source_Type': source_type,
            'Is_Conformed_Target': '1' if source_type == 'Fact_Sales' else '0',
            'Wholesaler_Code': None
        }
        
        # Add new source
        df_imports = pd.concat([df_imports, pd.DataFrame([new_import])], ignore_index=True)
        all_sheets['Source_Imports'] = df_imports
        
        # Save ALL sheets
        with pd.ExcelWriter(config_file, engine='openpyxl') as writer:
            for sheet_name, df in all_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Updated Excel config: {source_name}")
        print(f"✅ Total sources: {len(df_imports)}")
        return True
        
    except Exception as e:
        print(f"❌ Excel error: {e}")
        return False

def add_default_mappings(source_name):
    """Add default conformed staging mappings for Fact_Sales preserving ALL sheets"""
    config_file = Path('config/etl_config.xlsx')
    
    try:
        # Load ALL existing sheets
        xl_file = pd.ExcelFile(config_file, engine='openpyxl')
        all_sheets = {}
        for sheet_name in xl_file.sheet_names:
            df = pd.read_excel(config_file, sheet_name=sheet_name, engine='openpyxl')
            all_sheets[sheet_name] = df
            print(f"📁 Loaded {sheet_name}: {len(df)} rows")
        
        # Get existing mappings or create empty
        if 'DW_Mapping_And_Transformations' in all_sheets:
            df_mappings = all_sheets['DW_Mapping_And_Transformations']
        else:
            df_mappings = pd.DataFrame()
            print("⚠️  Creating new DW_Mapping_And_Transformations sheet")
        
        # Default Fact_Sales mappings
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
                'Transformation_Type': 'Direct',  # Simplified - customize as needed
                'Transformation_Rule': '[Product Name]',
                'Is_Key': 0,
                'Is_Required': 0,
                'Default_Value': 'UNKNOWN',
                'Validation_Rule': '',
                'Sequence_Order': 3,
                'Description': 'Direct product mapping'
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
        
        # Add new mappings
        df_mappings = pd.concat([df_mappings, pd.DataFrame(default_mappings)], ignore_index=True)
        all_sheets['DW_Mapping_And_Transformations'] = df_mappings
        
        # Save ALL sheets
        with pd.ExcelWriter(config_file, engine='openpyxl') as writer:
            for sheet_name, df in all_sheets.items():
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Added {len(default_mappings)} mappings for {source_name}")
        print(f"✅ Total mappings: {len(df_mappings)}")
        return True
        
    except Exception as e:
        print(f"❌ Mapping error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description='Add new source with minimum input')
    parser.add_argument('--name', required=True, help='Source name (e.g., Avid)')
    parser.add_argument('--path', required=True, help='Relative path (e.g., raw/DATA/SALES/CLICKS/AVID)')
    parser.add_argument('--type', required=True, choices=['Dimension', 'Fact_Sales'], help='Source type')
    parser.add_argument('--skip-mappings', action='store_true', help='Skip default mappings (for Dimension sources)')
    
    args = parser.parse_args()
    
    print(f"🚀 Adding new source: {args.name}")
    print(f"   Path: {args.path}")
    print(f"   Type: {args.type}")
    print()
    
    # Step 1: Backup Excel config
    backup_excel_config()
    
    # Step 2: Add to database
    if not add_source_to_database(args.name, args.path, args.type):
        sys.exit(1)
    
    # Step 3: Add to Excel config
    if not add_to_excel_config(args.name, args.path, args.type):
        sys.exit(1)
    
    # Step 4: Add default mappings (Fact_Sales only)
    if args.type == 'Fact_Sales' and not args.skip_mappings:
        add_default_mappings(args.name)
    
    print()
    print("🎉 Source added successfully!")
    print()
    print("📋 Next steps:")
    print(f"   1. Place files in: {args.path}")
    print(f"   2. Test with: python -m src.etl_orchestrator --sources {args.name} --refresh-metadata --force-ddl")
    print(f"   3. Verify ODS table: [ODS].[{args.name}]")
    if args.type == 'Fact_Sales':
        print(f"   4. Test conformed staging: [ETL].[Staging_Fact_Sales_Conformed]")
    print()
    print("📊 Check logs in logs/ folder for any issues")

if __name__ == '__main__':
    main()
