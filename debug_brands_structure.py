import pandas as pd

# Read the actual Brands.xlsx file
try:
    df = pd.read_excel("C:\\SISRI\\raw\\MDM\\Brands.xlsx", sheet_name="Sheet1")
    print("Brands.xlsx structure:")
    print(f"Columns: {list(df.columns)}")
    print(f"Shape: {df.shape}")
    print(f"First row: {df.iloc[0].to_dict()}")
except Exception as e:
    print(f"Error reading Brands.xlsx: {e}")

# Read our clean file
try:
    df_clean = pd.read_excel("C:\\SISRI\\raw\\MDM\\Brands_Clean.xlsx", sheet_name="Sheet1")
    print("\nBrands_Clean.xlsx structure:")
    print(f"Columns: {list(df_clean.columns)}")
    print(f"Shape: {df_clean.shape}")
    print(f"First row: {df_clean.iloc[0].to_dict()}")
except Exception as e:
    print(f"Error reading Brands_Clean.xlsx: {e}")
