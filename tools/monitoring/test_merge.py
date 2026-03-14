"""Test SP_Merge_Fact_Sales_ODS_to_Conformed."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.db_ops import get_connection

conn = get_connection()
c = conn.cursor()
c.execute("EXEC ETL.SP_Merge_Fact_Sales_ODS_to_Conformed @SourceName='Sales_Format_1'")
conn.commit()
print("Success")
c.close()
conn.close()
