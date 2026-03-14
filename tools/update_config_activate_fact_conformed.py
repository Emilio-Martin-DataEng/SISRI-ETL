"""Set Is_Active=1 for Staging_Fact_Sales_Conformed in ETL_Config.xlsx."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import pandas as pd

config_path = Path(__file__).resolve().parents[1] / "config" / "etl_config.xlsx"
df = pd.read_excel(config_path, sheet_name="Source_Imports", dtype=str)
mask = df["Source_Name"] == "Staging_Fact_Sales_Conformed"
if mask.any():
    df.loc[mask, "Is_Active"] = "1"
    df.to_excel(config_path, sheet_name="Source_Imports", index=False)
    print("Updated: Staging_Fact_Sales_Conformed Is_Active = 1")
else:
    print("Staging_Fact_Sales_Conformed not found in Source_Imports")
    sys.exit(1)
