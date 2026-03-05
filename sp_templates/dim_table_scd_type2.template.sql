-- Safe regeneration template for {schema}.{table_name}
-- Generated at {generated_time}

-- Drop old backup if it exists (prevents rename conflict)
IF OBJECT_ID('{schema}.{table_name}_backup_{timestamp}', 'U') IS NOT NULL
    DROP TABLE [{schema}].[{table_name}_backup_{timestamp}];
GO

-- Preserve data and rename old table if it exists
IF OBJECT_ID('{schema}.{table_name}', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes safely
    IF EXISTS (SELECT * FROM sys.key_constraints 
               WHERE name = 'PK_{table_name}' 
               AND parent_object_id = OBJECT_ID('{schema}.{table_name}'))
        ALTER TABLE [{schema}].[{table_name}] DROP CONSTRAINT PK_{table_name};
    
    IF EXISTS (SELECT * FROM sys.indexes 
               WHERE name = 'UIX_NK_{table_name}_Active' 
               AND object_id = OBJECT_ID('{schema}.{table_name}'))
        DROP INDEX [UIX_NK_{table_name}_Active] ON [{schema}].[{table_name}];
    
    -- Rename old table to backup (preserves data)
    EXEC sp_rename '{schema}.{table_name}', '{table_name}_backup_{timestamp}';
END
GO

-- Create new table
CREATE TABLE [{schema}].[{table_name}] (
    [{sk_col}] INT IDENTITY(1,1) NOT NULL CONSTRAINT [PK_{table_name}] PRIMARY KEY,
    [Row_Is_Current] BIT NOT NULL DEFAULT 1,
    [Row_Effective_Datetime] DATETIME NOT NULL DEFAULT GETDATE(),
    [Row_Expiry_Datetime] DATETIME NULL,
    [Inserted_Datetime] DATETIME2 NOT NULL DEFAULT GETDATE(),
    [Updated_Datetime] DATETIME NULL,
    [Is_Deleted] BIT NOT NULL DEFAULT 0,
    [Row_Change_Reason] VARCHAR(50) NULL
    {column_defs}
);
GO



-- Restore data with IDENTITY_INSERT to preserve SKs
IF OBJECT_ID('{schema}.{table_name}_backup_{timestamp}', 'U') IS NOT NULL
BEGIN
    SET IDENTITY_INSERT [{schema}].[{table_name}] ON;
    
    INSERT INTO [{schema}].[{table_name}] ([{sk_col}], [Row_Is_Current], [Row_Effective_Datetime], [Row_Expiry_Datetime], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], {insert_columns})
    SELECT [{sk_col}],  [Row_Is_Current], [Row_Effective_Datetime], [Row_Expiry_Datetime], [Inserted_Datetime], [Updated_Datetime], [Is_Deleted], [Row_Change_Reason], {select_columns}
    FROM [{schema}].[{table_name}_backup_{timestamp}];
    
    SET IDENTITY_INSERT [{schema}].[{table_name}] OFF;
END
GO

-- Add unique index on active natural keys
{unique_index_clause}
GO