# Kimball Fact Engine Implementation Plan

## Overview
Extend the SISRI ETL system to support Kimball fact tables, starting with Fact_Sales. This implementation will reuse existing dimension ETL infrastructure while adding fact-specific processing, aggregation, and loading capabilities.

---

## Phase 1: High-Level Qualification & Design

### 1.1 Fact Table Definition
**Core Questions:**
- **Grain Definition**: What is the grain of Fact_Sales?
  - Transaction-level (each individual sale)
  - Daily summary (product-store-day aggregates)
  - Hourly summary
  - Other granularity?
- **Measures Confirmation**:
  - Unit_Cost (numeric, precision?)
  - Sales_Quantity (integer, can be negative for returns?)
  - Total_Sales_Amount (numeric, calculated vs. source?)
- **Degenerate Dimensions**: 
  - Product_Code (business key)
  - Store_Code (business key) 
  - Barcode (product identifier)
  - Other transaction identifiers?
- **Foreign Keys**: 
  - All existing dimension SKs (Dim_Product_SK, Dim_Store_SK, etc.)
  - New Dim_Date_SK
  - Any other dimension relationships?
- **Data Types**: Precision and scale for numeric measures
- **Constraints**: Primary key strategy (surrogate vs. composite)

**Future Qualification Questions:**
- How do we handle returns, refunds, or adjustments?
- Are there seasonal or promotional attributes to consider?
- Do we need to support multiple currencies?
- How are discounts, taxes, or other adjustments handled?

### 1.2 Source Analysis
**Core Questions:**
- **Source Files**: What Excel/CSV files contain sales data?
  - File names and locations
  - File formats and structures
  - Delivered frequency (daily, weekly, monthly, ad-hoc?)
- **Source Structure**: 
  - Do all sources have the same columns?
  - Are there source-specific transformations needed?
  - How do we handle different source formats?
- **Business Key Mapping**:
  - How do source fields map to dimension natural keys?
  - What lookup logic is required?
  - How are missing or invalid keys handled?
- **Data Volume**:
  - Expected records per load
  - Historical data requirements
  - Growth projections

**Future Qualification Questions:**
- Are there real-time vs. batch sources?
- How do we handle source system changes over time?
- Are there source-specific business rules?
- How do we handle partial file loads or corruption?

### 1.3 Integration Strategy
**Core Questions:**
- **Processing Order**: Should facts process after all dimensions?
  - Processing_Order > 100 for fact sources?
  - Dependencies on dimension refresh?
- **Load Strategy**:
  - Incremental vs. full loads?
  - How to identify new/changed records?
  - Watermark or CDC approach?
- **Staging Architecture**:
  - Transient ODS tables (truncate before load)
  - Persistent staging layer for audit trail
  - Hybrid approach?

**Future Qualification Questions:**
- How do we handle late-arriving facts?
- What are the SLA requirements for fact availability?
- How do we handle fact table maintenance (archiving, purging)?
- Are there fact table partitioning requirements?

---

## Phase 2: Metadata & Configuration

### 2.1 Extend ETL_Config.xlsx
**Implementation Tasks:**
- Add `Fact_File_Mapping` sheet
- Define fact-specific column mappings:
  - Measure columns (Unit_Cost, Sales_Quantity, Total_Sales_Amount)
  - Foreign key lookup columns
  - Business key columns
  - Degenerate dimension columns
- Configure aggregation rules
- Define business logic and validation rules

**Configuration Columns:**
- Source_Name
- Source_Column
- Target_Column
- Column_Type (Measure, FK_Lookup, Business_Key, Degenerate_Dim)
- Data_Type
- Is_Required
- Lookup_Dimension (for FK columns)
- Business_Rule (validation logic)
- Aggregation_Type (SUM, COUNT, AVG, etc.)

### 2.2 Extend Metadata Tables
**New ETL Schema Tables:**
- `[ETL].[Dim_Fact_Imports]` - Fact source definitions
- `[ETL].[Dim_Fact_Imports_Mapping]` - Fact column mappings
- `[ETL].[Fact_Source_File_Archive]` - Fact source file lineage

**Table Structure Considerations:**
- Fact-specific metadata columns
- Business rule definitions
- Aggregation configurations
- Load strategy flags

### 2.3 Update etl_config.py
**Enhancement Tasks:**
- Extend `process_etl_config()` to handle fact metadata
- Load fact mappings alongside dimension mappings
- Generate fact-specific DDL
- Create fact merge procedures
- Handle fact source file archiving

---

## Phase 3: Processing Engine

### 3.1 Fact Source Processing
**Implementation Options:**
- Extend `source_import.py` with fact-specific logic
- Create new `fact_import.py` module
- Shared utilities for common operations

**Processing Steps:**
1. Source file discovery and validation
2. Data reading and sanitization
3. Fact-specific validation rules
4. Business key extraction
5. Measure validation (negative values, limits)
6. Load to transient staging table

**Validation Rules:**
- Measure ranges and constraints
- Required field validation
- Business key format validation
- Cross-field validation (e.g., Total_Sales_Amount = Unit_Cost * Sales_Quantity)

### 3.2 Dimension Lookup Service
**Core Functionality:**
- FK resolution for all dimension SKs
- Business key to natural key mapping
- Missing dimension handling strategy
- Lookup performance optimization

**Lookup Strategy:**
- Batch dimension lookups for performance
- Caching frequently accessed dimensions
- Handling missing or invalid keys
- Logging lookup failures

**Error Handling:**
- Reject records with missing FKs
- Create missing dimensions (configurable)
- Fuzzy matching for near-matches
- Audit trail of lookups performed

### 3.3 Persistent Staging Layer
**Design Considerations:**
- Generic fact staging table structure
- Source lineage tracking
- Data quality indicators
- Historical audit trail

**Staging Table Design:**
- All possible fact columns (wide table)
- Source metadata columns
- Processing status flags
- Quality validation results
- Insert timestamps

**Data Flow:**
- Transient staging → persistent staging → fact table
- Validation and enrichment in persistent staging
- Source-to-target lineage tracking

---

## Phase 4: Fact Loading & Merging

### 4.1 Fact Table DDL Generation
**Extension to src/dw/ddl_generator.py:**
- Fact table DDL templates
- Constraint generation (PK, FK, CHECK)
- Index strategy for fact queries
- Partitioning considerations

**DDL Components:**
- Fact table structure
- Primary key definition
- Foreign key constraints
- Indexes (clustered, non-clustered)
- Check constraints for measures

**Performance Considerations:**
- Clustered index strategy
- Partitioning on date keys
- Compression options
- Statistics management

### 4.2 Fact Merge Logic
**Load Strategies:**
- Append-only (simple facts)
- Upsert (updateable facts)
- Merge with change detection
- Incremental with watermarks

**Merge Implementation:**
- Stored procedure generation
- Change detection logic
- Conflict resolution
- Performance optimization

**Incremental Load Strategy:**
- Watermark columns (modified dates)
- Change Data Capture (CDC)
- Source system timestamps
- File processing tracking

### 4.3 Dim_Date Integration
**Date Dimension Requirements:**
- Auto-generate Dim_Date if missing
- Populate calendar attributes
- Handle fiscal vs. calendar calendars
- Date granularity (daily, weekly, monthly)

**Date Logic:**
- Transaction date extraction
- Date dimension lookup
- Calendar attribute population
- Holiday and special date handling

---

## Phase 5: Orchestration & Auditing

### 5.1 Extend Orchestrator
**Orchestration Enhancements:**
- Fact source processing order
- Dependency management (dimensions first)
- Fact-specific error handling
- Performance monitoring

**Processing Flow:**
1. Dimension processing (existing)
2. Fact metadata refresh
3. Fact source processing
4. Fact table loading
5. Post-load validation

**Error Handling:**
- Fact-specific reject reasons
- Partial load recovery
- Rollback strategies
- Notification mechanisms

### 5.2 Fact-Specific Auditing
**Audit Requirements:**
- Fact row counts by source
- Measure validation and reconciliation
- Source-to-target totals matching
- Performance metrics

**Audit Tables:**
- Extend existing audit framework
- Fact-specific audit columns
- Measure aggregation tracking
- Data quality metrics

**Reconciliation Logic:**
- Source totals vs. fact totals
- Measure aggregation validation
- Dimension FK consistency checks
- Data completeness validation

---

## Phase 6: Testing & Optimization

### 6.1 Sample Data Testing
**Testing Strategy:**
- Sample sales data creation
- End-to-end processing validation
- Error scenario testing
- Performance baseline establishment

**Test Scenarios:**
- Valid data processing
- Invalid data rejection
- Missing dimension handling
- Large volume processing
- Concurrent processing

### 6.2 Performance Tuning
**Optimization Areas:**
- Bulk loading optimization
- Index strategy refinement
- Memory usage optimization
- Parallel processing opportunities

**Monitoring:**
- Processing time metrics
- Resource utilization tracking
- Error rate monitoring
- Performance regression detection

---

## Conformed Dimension Extension (Dim_Products)

Dim_Products will use the same conformed engine as Fact_Sales:
- **Flow:** Multiple ODS sources → `ETL.Staging_Dim_Products_Conformed` → `DW.Dim_Products`
- **Source_Type:** `Dimension_Conformed` for product sources
- **Metadata:** Reuse `DW_Mapping_And_Transformations` with `Source_Name` = Products (and future product sources)
- **See:** `docs/Next-Steps-Plan.md` for full implementation plan

---

## Future Considerations

### Additional Fact Tables
- Fact_Inventory
- Fact_Purchases  
- Fact_Returns
- Fact_Promotions

### Advanced Features
- Real-time fact loading
- Fact table partitioning
- Advanced aggregations
- Machine learning integration

### Scalability
- Distributed processing
- Cloud migration
- Multi-tenant support
- High availability

---

## Implementation Priority

### High Priority (Phase 1-2)
1. Fact_Sales definition and source analysis
2. ETL_Config.xlsx extensions
3. Metadata table creation

### Medium Priority (Phase 3-4)
1. Fact processing engine
2. Dimension lookup service
3. Fact table loading

### Low Priority (Phase 5-6)
1. Advanced orchestration
2. Performance optimization
3. Additional fact tables

---

## Success Criteria

### Functional Requirements
- [ ] Successfully load Fact_Sales from multiple sources
- [ ] Proper FK resolution to all dimensions
- [ ] Accurate measure calculation and validation
- [ ] Complete audit trail and lineage

### Non-Functional Requirements
- [ ] Performance meets SLA requirements
- [ ] Error handling and recovery mechanisms
- [ ] Data quality validation
- [ ] Documentation and maintainability

### Integration Requirements
- [ ] Seamless integration with existing dimension ETL
- [ ] Reuse of existing infrastructure
- [ ] Consistent logging and error handling
- [ ] Compatible with existing orchestration
