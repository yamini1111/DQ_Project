CREATE OR ALTER PROCEDURE dqr.sp_UpdateWatermark
    @schemaname VARCHAR(50),
    @tablename  VARCHAR(128),
    @watermarkcol VARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);
    DECLARE @fulltablename NVARCHAR(50);

    -- Build full schema.table name
    SET @fulltablename = @schemaname + '.' + @tablename;

    -- Build dynamic SQL with schema + table
    SET @sql = '
        UPDATE dqr.incremental_load_mappings
        SET watermarkvalue = (
            SELECT ISNULL(MAX(' + QUOTENAME(@watermarkcol) + '), ''2020-01-01'')
            FROM '  + @fulltablename + '
        )
        WHERE tablename = @fulltablename_param;';

    -- Execute with parameter substitution for mapping filter
    EXEC sp_executesql 
        @sql, 
        N'@fulltablename_param SYSNAME',
        @fulltablename_param = @fulltablename;


    -- Debug (optional)
     PRINT @sql;
END;
GO


--EXEC sp_UpdateWatermark 'dqr','dqchecks','lastmodifieddate'
