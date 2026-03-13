"""
Date_SK Transformer Module

Intelligent date format detection and conversion to integer key (yyyyMMdd).
Handles multiple date formats automatically with graceful error handling.
"""

import re
from datetime import datetime
from typing import Optional, Union


class DateSKTransformer:
    """
    Transform any date string to integer key (yyyyMMdd format).
    
    Supports multiple date formats:
    - dd-MM-YYYY (e.g., 01-12-2025)
    - YYYY-MM-dd (e.g., 2025-12-01)
    - MM/dd/YYYY (e.g., 12/01/2025)
    - dd/MM/YYYY (e.g., 01/12/2025)
    - Natural language (e.g., 1 Dec 2025, December 1, 2025)
    """
    
    # Common date format patterns (ordered by specificity)
    DATE_PATTERNS = [
        # Natural language patterns (most specific first)
        (r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})$', '%b %d, %Y'),
        (r'^(\d{1,2})(?:st|nd|rd|th)?\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})$', '%d %b %Y'),
        # dd-MM-YYYY (most common in our data)
        (r'^(\d{1,2})-(\d{1,2})-(\d{4})$', '%d-%m-%Y'),
        # YYYY-MM-dd (ISO format)
        (r'^(\d{4})-(\d{1,2})-(\d{1,2})$', '%Y-%m-%d'),
        # YYYY/MM/dd
        (r'^(\d{4})/(\d{1,2})/(\d{1,2})$', '%Y/%m/%d'),
    ]
    
    @staticmethod
    def transform_to_date_sk(date_string: Union[str, None], default_value: str = '19000101') -> int:
        """
        Transform any date string to integer key (yyyyMMdd).
        
        Args:
            date_string: Input date string in any format
            default_value: Default integer key for invalid dates
            
        Returns:
            Integer in yyyyMMdd format
        """
        if not date_string or str(date_string).strip() == '':
            return int(default_value)
        
        date_string = str(date_string).strip()
        
        # Special handling for slash format ambiguity (dd/MM vs MM/dd)
        if '/' in date_string and len(date_string.split('/')) == 3:
            parts = date_string.split('/')
            if len(parts[2]) == 4:  # 4-digit year
                day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                
                # If first part > 12, it must be dd/MM format (day first)
                if day > 12:
                    try:
                        parsed_date = datetime(year, month, day)
                        return int(parsed_date.strftime('%Y%m%d'))
                    except ValueError:
                        pass
                # If second part > 12, it must be MM/dd format (month first)
                elif month > 12:
                    try:
                        parsed_date = datetime(year, day, month)  # Swap: month=day, day=month
                        return int(parsed_date.strftime('%Y%m%d'))
                    except ValueError:
                        pass
                # If both <= 12, assume dd/MM format (European standard)
                else:
                    try:
                        parsed_date = datetime(year, month, day)
                        return int(parsed_date.strftime('%Y%m%d'))
                    except ValueError:
                        pass
        
        # Try each pattern
        for pattern, format_str in DateSKTransformer.DATE_PATTERNS:
            try:
                match = re.match(pattern, date_string, re.IGNORECASE)
                if match:
                    # Parse using the detected format
                    if format_str in ['%d %b %Y', '%b %d, %Y']:
                        # Handle month names case-insensitively
                        parsed_date = datetime.strptime(date_string, format_str)
                    else:
                        parsed_date = datetime.strptime(date_string, format_str)
                    
                    return int(parsed_date.strftime('%Y%m%d'))
            except ValueError:
                continue
        
        # Try Python's built-in parser as fallback
        try:
            # Try common SQL Server formats
            for sql_format in [23, 105, 101, 111]:  # YYYY-MM-dd, dd-MM-YYYY, MM/dd/YYYY, YYYY/MM/dd
                try:
                    parsed_date = datetime.strptime(date_string, {
                        23: '%Y-%m-%d',
                        105: '%d-%m-%Y', 
                        101: '%m/%d/%Y',
                        111: '%Y/%m/%d'
                    }[sql_format])
                    return int(parsed_date.strftime('%Y%m%d'))
                except ValueError:
                    continue
        except:
            pass
        
        # If all else fails, return default
        return int(default_value)
    
    @staticmethod
    def get_sql_transformation_rule(column_name: str, default_value: str = '19000101') -> str:
        """
        Generate SQL transformation rule for Date_SK transformation.
        
        Args:
            column_name: Source column name
            default_value: Default value for invalid dates
            
        Returns:
            SQL expression for transformation
        """
        return f"""
        CASE 
            WHEN TRY_CONVERT(DATE, [{column_name}], 23) IS NOT NULL 
                THEN CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE, [{column_name}], 23), 112))
            WHEN TRY_CONVERT(DATE, [{column_name}], 105) IS NOT NULL 
                THEN CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE, [{column_name}], 105), 112))
            WHEN TRY_CONVERT(DATE, [{column_name}], 101) IS NOT NULL 
                THEN CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE, [{column_name}], 101), 112))
            WHEN TRY_CONVERT(DATE, [{column_name}], 111) IS NOT NULL 
                THEN CONVERT(INT, CONVERT(VARCHAR(8), TRY_CONVERT(DATE, [{column_name}], 111), 112))
            ELSE {default_value}
        END
        """.strip()


# Test function for development
def test_date_sk_transformer():
    """Test the Date_SK transformer with various date formats."""
    test_cases = [
        ('01-12-2025', 20251201),  # dd-MM-YYYY
        ('2025-12-01', 20251201),  # YYYY-MM-dd
        ('12/01/2025', 20251201),  # MM/dd/YYYY
        ('01/12/2025', 20251201),  # dd/MM/YYYY
        ('2025/12/01', 20251201),  # YYYY/MM/dd
        ('1 Dec 2025', 20251201),  # Natural language
        ('December 1, 2025', 20251201),  # Natural language
        ('invalid-date', 19000101),  # Invalid
        ('', 19000101),  # Empty
        (None, 19000101),  # Null
    ]
    
    transformer = DateSKTransformer()
    
    print("Testing Date_SK Transformer:")
    print("-" * 50)
    
    for input_date, expected in test_cases:
        result = transformer.transform_to_date_sk(input_date)
        status = "✅" if result == expected else "❌"
        print(f"{status} '{input_date}' -> {result} (expected: {expected})")
    
    print("-" * 50)
    print("SQL Transformation Rule:")
    print(transformer.get_sql_transformation_rule('Sales_Date'))


if __name__ == "__main__":
    test_date_sk_transformer()
