# src/utils/rejected_rows.py

import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

from src.config import SYSTEM_BASE_PATH, get_config
from src.utils.db_ops import get_connection
from src.utils.logging_config import setup_logging

class RejectedRowsHandler:
    """
    Handles rejected rows following Kimball best practices.
    Writes to both database table [ETL].[Fact_Rejected_Rows] and flat files.
    """
    
    def __init__(self, source_name: str, audit_id: int):
        self.source_name = source_name
        self.audit_id = audit_id
        self.logger = setup_logging("rejected_rows")
        
        # Setup rejected directory
        self.rejected_dir = SYSTEM_BASE_PATH() / "rejected"
        self.rejected_dir.mkdir(exist_ok=True)
        
        # Setup rejected file (overwrite existing for current run)
        self.rejected_file = self.rejected_dir / f"{source_name}_rejected.csv"
        
        # Initialize rejected file with header
        self._initialize_rejected_file()
    
    def _initialize_rejected_file(self):
        """Initialize the rejected CSV file with headers."""
        if self.rejected_file.exists():
            self.rejected_file.unlink()
        
        # Create header with reason column
        header = "Inserted_Datetime,Rejected_Reason,Raw_Data\n"
        with open(self.rejected_file, 'w', encoding='utf-8') as f:
            f.write(header)
    
    def log_rejected_row(self, file_name: str, row_number: int, 
                        rejected_reason: str, raw_data: Dict[str, Any]):
        """
        Log a rejected row to both database and file.
        
        Args:
            file_name: Source file name
            row_number: Row number in source file
            rejected_reason: Reason for rejection
            raw_data: Dictionary of the raw row data
        """
        try:
            # Convert raw_data to JSON string for database storage
            raw_data_json = json.dumps(raw_data, default=str, ensure_ascii=False)
            
            # Log to database using existing procedure
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                EXEC [ETL].[SP_Log_Rejected_Row]
                    @Audit_Source_Import_SK = ?,
                    @Source_Name = ?,
                    @File_Name = ?,
                    @Row_Number = ?,
                    @Rejected_Reason = ?,
                    @Raw_Data = ?
            """, self.audit_id, self.source_name, file_name, row_number, 
                 rejected_reason, raw_data_json)
            conn.commit()
            cursor.close()
            conn.close()
            
            # Log to flat file
            self._write_to_file(rejected_reason, raw_data_json)
            
            self.logger.debug(f"Logged rejected row: {file_name}:{row_number} - {rejected_reason}")
            
        except Exception as e:
            self.logger.error(f"Failed to log rejected row: {str(e)}")
    
    def log_duplicate_rows(self, file_name: str, duplicates_df: pd.DataFrame, 
                           pk_columns: List[str]):
        """
        Log duplicate records found in source data.
        
        Args:
            file_name: Source file name
            duplicates_df: DataFrame containing duplicate records
            pk_columns: Primary key columns that identify duplicates
        """
        if duplicates_df.empty:
            return
        
        self.logger.warning(f"Found {len(duplicates_df)} duplicate records in {file_name}")
        
        for idx, row in duplicates_df.iterrows():
            # Convert row to dict for consistent handling
            raw_data = row.to_dict()
            row_number = idx + 2  # Excel row numbers start at 2 (1 = header)
            
            # Create detailed reason for duplicates
            pk_values = {col: raw_data.get(col, 'NULL') for col in pk_columns}
            pk_string = ", ".join([f"{k}={v}" for k, v in pk_values.items()])
            rejected_reason = f"DUPLICATE_RECORD: PK {pk_string}"
            
            self.log_rejected_row(file_name, row_number, rejected_reason, raw_data)
    
    def log_bcp_rejected_rows(self, file_name: str, bcp_log_content: str):
        """
        Log rows rejected during BCP operation.
        
        Args:
            file_name: Source file name
            bcp_log_content: Content from BCP error log
        """
        if not bcp_log_content.strip():
            return
        
        self.logger.warning(f"Processing BCP rejects from {file_name}")
        
        # Parse BCP log for individual row errors
        lines = bcp_log_content.strip().split('\n')
        
        for line_num, line in enumerate(lines, 1):
            if line.strip() and not line.startswith('@') and not line.startswith('SQLState'):
                # Extract meaningful error information
                rejected_reason = f"BCP_REJECTED: {line.strip()[:200]}"
                
                # Create minimal raw_data for BCP errors
                raw_data = {
                    "error_line": line_num,
                    "bcp_error": line.strip(),
                    "file_name": file_name
                }
                
                self.log_rejected_row(file_name, line_num, rejected_reason, raw_data)
    
    def _write_to_file(self, rejected_reason: str, raw_data_json: str):
        """Write rejected row data to CSV file."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Escape quotes and newlines in CSV format
        escaped_reason = rejected_reason.replace('"', '""').replace('\n', '\\n')
        escaped_data = raw_data_json.replace('"', '""').replace('\n', '\\n')
        
        line = f'{timestamp},"{escaped_reason}","{escaped_data}"\n'
        
        with open(self.rejected_file, 'a', encoding='utf-8') as f:
            f.write(line)
    
    def get_rejected_count(self) -> int:
        """Get total count of rejected rows for this source."""
        try:
            conn = get_connection()
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) 
                FROM [ETL].[Fact_Rejected_Rows] 
                WHERE Source_Name = ? AND Audit_Source_Import_SK = ?
            """, self.source_name, self.audit_id)
            count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            return count
        except Exception as e:
            self.logger.error(f"Failed to get rejected count: {str(e)}")
            return 0
