# src/utils/bcp_type_mapper.py

from dataclasses import dataclass
from typing import Tuple, Optional
import re


@dataclass
class BCPTypeMapping:
    """
    Reusable data object to map SQL data types to BCP format types.
    Handles precision, scale, and length calculations for various SQL types.
    """
    sql_type: str          # BCP SQL type (e.g., 'SQLCHAR', 'SQLINT', 'SQLDECIMAL')
    length: str            # Length/precision string for BCP format file
    description: str       # Description of the mapping logic


class BCPTypeMapper:
    """
    Utility class for mapping SQL data types to BCP format types.
    Provides reusable methods to handle different data types with proper precision/scale calculations.
    """
    
    @staticmethod
    def map_sql_to_bcp(data_type: str) -> BCPTypeMapping:
        """
        Map a SQL data type to BCP format type with proper length/precision.
        
        Args:
            data_type: SQL data type string (e.g., 'VARCHAR(255)', 'DECIMAL(18,4)', 'FLOAT')
            
        Returns:
            BCPTypeMapping: Complete mapping information for BCP format file
        """
        data_type_upper = data_type.upper().strip()
        
        # Handle VARCHAR types
        if data_type_upper.startswith('VARCHAR'):
            return BCPTypeMapper._map_varchar(data_type_upper)
        
        # Handle DECIMAL/NUMERIC types
        elif data_type_upper.startswith(('DECIMAL', 'NUMERIC')):
            return BCPTypeMapper._map_decimal(data_type_upper)
        
        # Handle FLOAT types
        elif data_type_upper == 'FLOAT':
            return BCPTypeMapper._map_float()
        
        # Handle INT types
        elif data_type_upper in ('INT', 'INTEGER', 'BIGINT', 'SMALLINT', 'TINYINT'):
            return BCPTypeMapper._map_integer(data_type_upper)
        
        # Handle DATETIME types
        elif data_type_upper.startswith(('DATETIME', 'DATE', 'TIME', 'DATETIME2')):
            return BCPTypeMapper._map_datetime(data_type_upper)
        
        # Handle BIT/BOOLEAN types
        elif data_type_upper in ('BIT', 'BOOLEAN'):
            return BCPTypeMapper._map_bit()
        
        # Handle CHAR types (fixed length)
        elif data_type_upper.startswith('CHAR'):
            return BCPTypeMapper._map_char(data_type_upper)
        
        # Handle NCHAR/NVARCHAR (Unicode)
        elif data_type_upper.startswith(('NCHAR', 'NVARCHAR')):
            return BCPTypeMapper._map_unicode(data_type_upper)
        
        # Default fallback
        else:
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length='500',
                description=f"Default mapping for unsupported type: {data_type}"
            )
    
    @staticmethod
    def _map_varchar(data_type: str) -> BCPTypeMapping:
        """Map VARCHAR(n) to SQLCHAR with length n."""
        match = re.match(r'VARCHAR\((\d+)\)', data_type)
        if match:
            length = match.group(1)
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length=length,
                description=f"VARCHAR({length}) -> SQLCHAR with length {length}"
            )
        else:
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length='255',
                description="VARCHAR without length -> SQLCHAR with default length 255"
            )
    
    @staticmethod
    def _map_char(data_type: str) -> BCPTypeMapping:
        """Map CHAR(n) to SQLCHAR with length n."""
        match = re.match(r'CHAR\((\d+)\)', data_type)
        if match:
            length = match.group(1)
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length=length,
                description=f"CHAR({length}) -> SQLCHAR with length {length}"
            )
        else:
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length='10',
                description="CHAR without length -> SQLCHAR with default length 10"
            )
    
    @staticmethod
    def _map_decimal(data_type: str) -> BCPTypeMapping:
        """
        Map DECIMAL(p,s) to SQLCHAR with generous length for string representation.
        DECIMAL values in text files need to be handled as strings in BCP format.
        """
        match = re.match(r'(?:DECIMAL|NUMERIC)\((\d+),\s*(\d+)\)', data_type)
        if match:
            precision = int(match.group(1))
            scale = int(match.group(2))
            # Calculate generous length: precision + decimal point + sign + padding
            length = str(precision + 5)
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length=length,
                description=f"DECIMAL({precision},{scale}) -> SQLCHAR with length {length} for string representation"
            )
        else:
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length='25',
                description="DECIMAL without precision/scale -> SQLCHAR with default length 25"
            )
    
    @staticmethod
    def _map_float() -> BCPTypeMapping:
        """Map FLOAT to SQLCHAR with generous length for floating point strings."""
        return BCPTypeMapping(
            sql_type='SQLCHAR',
            length='50',
            description="FLOAT -> SQLCHAR with length 50 for floating point string representation"
        )
    
    @staticmethod
    def _map_integer(data_type: str) -> BCPTypeMapping:
        """Map integer types to appropriate BCP types."""
        if data_type_upper == data_type.upper():  # Fix undefined variable
            data_type_upper = data_type.upper()
        else:
            data_type_upper = data_type.upper()
            
        if data_type_upper in ('INT', 'INTEGER'):
            return BCPTypeMapping(
                sql_type='SQLINT',
                length='4',
                description="INT -> SQLINT with 4 bytes"
            )
        elif data_type_upper == 'BIGINT':
            return BCPTypeMapping(
                sql_type='SQLBIGINT',
                length='8',
                description="BIGINT -> SQLBIGINT with 8 bytes"
            )
        elif data_type_upper == 'SMALLINT':
            return BCPTypeMapping(
                sql_type='SQLSMALLINT',
                length='2',
                description="SMALLINT -> SQLSMALLINT with 2 bytes"
            )
        elif data_type_upper == 'TINYINT':
            return BCPTypeMapping(
                sql_type='SQLTINYINT',
                length='1',
                description="TINYINT -> SQLTINYINT with 1 byte"
            )
        else:
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length='20',
                description=f"Integer type {data_type} -> SQLCHAR with length 20 as fallback"
            )
    
    @staticmethod
    def _map_datetime(data_type: str) -> BCPTypeMapping:
        """Map datetime types to SQLCHAR with appropriate length."""
        return BCPTypeMapping(
            sql_type='SQLCHAR',
            length='30',
            description=f"{data_type} -> SQLCHAR with length 30 for datetime string representation"
        )
    
    @staticmethod
    def _map_bit() -> BCPTypeMapping:
        """Map BIT to SQLCHAR for string representation."""
        return BCPTypeMapping(
            sql_type='SQLCHAR',
            length='5',
            description="BIT -> SQLCHAR with length 5 for 'TRUE'/'FALSE' strings"
        )
    
    @staticmethod
    def _map_unicode(data_type: str) -> BCPTypeMapping:
        """Map NCHAR/NVARCHAR to SQLCHAR (BCP handles Unicode automatically)."""
        match = re.match(r'N?(?:CHAR|VARCHAR)\((\d+)\)', data_type)
        if match:
            length = match.group(1)
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length=length,
                description=f"{data_type} -> SQLCHAR with length {length} (Unicode handled automatically)"
            )
        else:
            return BCPTypeMapping(
                sql_type='SQLCHAR',
                length='255',
                description=f"Unicode type {data_type} -> SQLCHAR with default length 255"
            )
    
    @staticmethod
    def get_format_file_entry(
        field_number: int,
        sql_type: str,
        length: str,
        terminator: str,
        column_name: str,
        collation: str = "SQL_Latin1_General_CP1_CI_AS"
    ) -> str:
        """
        Generate a single BCP format file entry.
        
        Args:
            field_number: Field number (starting from 1)
            sql_type: BCP SQL type (e.g., 'SQLCHAR', 'SQLINT')
            length: Length/precision for the field
            terminator: Field terminator (e.g., '"\\t"', '"\\r\\n"')
            column_name: Target column name
            collation: Collation for character fields
            
        Returns:
            str: Complete format file entry line
        """
        return f"{field_number}\t{sql_type}\t0\t{length}\t{terminator}\t{field_number}\t{column_name}\t{collation}"
    
    @staticmethod
    def get_terminator(is_last_field: bool = False) -> str:
        """Get the appropriate terminator for a BCP field."""
        return '"\\r\\n"' if is_last_field else '"\\t"'


# Convenience function for direct mapping
def map_sql_type_to_bcp(data_type: str) -> Tuple[str, str]:
    """
    Convenience function to map SQL type to BCP format.
    Returns tuple of (sql_type, length) for backward compatibility.
    
    Args:
        data_type: SQL data type string
        
    Returns:
        Tuple[str, str]: (bcp_sql_type, length_string)
    """
    mapping = BCPTypeMapper.map_sql_to_bcp(data_type)
    return mapping.sql_type, mapping.length
