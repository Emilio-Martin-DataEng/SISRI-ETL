-- Safe regeneration for {schema}.{table_name}

IF OBJECT_ID('{schema}.{table_name}', 'U') IS NOT NULL
BEGIN
    -- Drop constraints and indexes
    IF EXISTS (SELECT * FROM sys.key_constraints WHERE name = 'PK_{table_name}' AND parent_object_id = OBJECT_ID('{schema}.{table_name}'))
        ALTER TABLE [{schema}].[{table_name}] DROP CONSTRAINT PK_{table_name};
    
    IF EXISTS (SELECT * FROM sys.indexes WHERE name = 'UIX_NK_{table_name}_Active' AND object_id = OBJECT_ID('{schema}.{table_name}'))
        DROP INDEX [UIX_NK_{table_name}_Active] ON [{schema}].[{table_name}];
    
    -- Preserve data
    SELECT {insert_list} INTO #Temp_{table_name} FROM [{schema}].[{table_name}];
    
    -- Drop old table
    DROP TABLE [{schema}].[{table_name}];
END
GO

-- Create new table
CREATE TABLE [{schema}].[{table_name}] (
{',\n'.join(column_defs)}
);
GO

-- Restore data if old table existed
IF OBJECT_ID('tempdb..#Temp_{table_name}') IS NOT NULL
BEGIN
    INSERT INTO [{schema}].[{table_name}] ({insert_list})
    SELECT {insert_list} FROM #Temp_{table_name};
    DROP TABLE #Temp_{table_name};
END
GO

{nk_clause}
GO