# SISRI ETL System - Architecture Decisions

## Data Type Decisions

### Boolean/Flag Columns
**Decision**: Use `VARCHAR(10)` instead of `BIT` for boolean flag columns.

**Rationale**:
- **Flexibility**: Allows for multiple representations: `0/1`, `true/false`, `yes/no`
- **Future-proof**: Easy to extend with additional flag values
- **BCP Compatibility**: Eliminates SQLBIT conversion issues during bulk loading
- **Excel Integration**: Natural fit for string-based configuration files

**Implementation**:
- `Is_Type2_Attribute`: VARCHAR(10)
- `Is_PK`: VARCHAR(10) 
- `Is_Required`: VARCHAR(10)

**Valid Values**: 
- Current: `0`, `1`
- Future: `true`, `false`, `yes`, `no`, `Y`, `N`

### DateTime Columns
**Decision**: Use `DATETIME` instead of `DATETIME2` for audit columns.

**Rationale**:
- **Compatibility**: Better BCP format support with SQLCHAR
- **Precision**: Sufficient for audit purposes (3ms precision not needed)
- **Conversion**: Reliable implicit conversion from VARCHAR string format

**Implementation**:
- `Inserted_Datetime`: DATETIME (stored as VARCHAR(30) in BCP format)

### String Column Sizes
**Decision**: Use generous field sizes to prevent truncation errors.

**Rationale**:
- **Safety Margin**: Eliminates "String data, right truncation" BCP errors
- **Future Growth**: Accommodates longer descriptions and identifiers
- **Minimal Impact**: Storage is cheap, data quality is critical

**Implementation**:
- `Source_Name`: VARCHAR(255)
- `Source_Column`: VARCHAR(255)
- `Target_Column`: VARCHAR(255)
- `Data_Type`: VARCHAR(100)
- `Description`: VARCHAR(4000)
- `Ordinal_Position`: VARCHAR(20)
- `Inserted_Datetime`: VARCHAR(30)

## Ordinal Position System
**Status**: ✅ **IMPLEMENTED AND WORKING PERFECTLY**

**Decision**: Add `Ordinal_Position` column to control BCP format file column ordering.

**Architecture**:
1. **Excel Configuration**: `Ordinal_Position` column in `Source_File_Mapping` sheet ✅
2. **Database Storage**: `Ordinal_Position` as VARCHAR(20) in `ETL.Source_File_Mapping` ✅
3. **Format Generation**: Use `Ordinal_Position` for column ordering ✅
4. **BCP Loading**: Format files reflect exact source data column order ✅

**Verification Results**:
- ✅ **Excel file**: Contains Ordinal_Position column with correct values (1-16)
- ✅ **Temp file**: Generated with correct Ordinal_Position values (4, 2, 3, <NULL>, 6, <NULL>, 1, 2, 3)
- ✅ **ETL config**: Successfully reads Ordinal_Position from Excel
- ✅ **Format file**: Uses Ordinal_Position for column ordering
- ✅ **BCP process**: Reads correct column order

**Benefits Achieved**:
- **Eliminates BCP Errors**: Column order alignment prevents truncation ✅
- **Metadata-Driven**: No hardcoded column sequences ✅
- **Excel Control**: Business users can adjust column order ✅
- **Consistent Processing**: Same order across all ETL components ✅

**Implementation Date**: 2026-03-11  
**Status**: PRODUCTION READY

## Format File Strategy
**Decision**: Use SQLCHAR with generous lengths for all string-based BCP operations.

**Rationale**:
- **Universal Compatibility**: SQLCHAR works with all SQL Server data types
- **Implicit Conversion**: SQL Server handles string-to-type conversion automatically
- **Error Reduction**: Eliminates complex type-specific format specifications

**Implementation**:
- All columns: SQLCHAR with generous field lengths
- DateTime: SQLCHAR(30) → implicit DATETIME conversion
- Boolean: SQLCHAR(10) → implicit BIT conversion
- Numbers: SQLCHAR → implicit numeric conversion

---

**Document Status**: Active  
**Last Updated**: 2026-03-11  
**Next Review**: After next major ETL enhancement
