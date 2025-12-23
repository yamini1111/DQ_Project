select * from dqr.dqresults
GO

CREATE FUNCTION dqr.fn_GetRuleType (@rulename varchar(50))
RETURNS VARCHAR(100)
AS
BEGIN
    DECLARE @dqruletype VARCHAR(100);
    SELECT @dqruletype = dqruletype
    FROM [dqr].[dqchecks]
    WHERE dqrulename = @rulename;
    RETURN @dqruletype;
END

GO

ALTER TABLE dqr.dqresults
ADD dqruletype AS dqr.fn_GetRuleType(rulename);


select  dqr.fn_GetRuleType('SumCheck') as ruletype

ALTER TABLE dqr.dqresults
ADD RuleStatus AS 
    CASE 
            WHEN TargetResult IS NULL AND SourceResult = 1 THEN 'Pass'
            WHEN TargetResult IS NULL AND SourceResult <> 1 THEN 'Fail'
            WHEN TargetResult IS NOT NULL AND SourceResult = TargetResult THEN 'Pass'
            WHEN TargetResult IS NOT NULL AND SourceResult <> TargetResult THEN 'Fail'
            ELSE 'Unknown'
        END

-- truncate table dqr.dqresults

-- insert into dqr.dqresults (dqresultid,objectname,rulename,sourceresult,targetresult,rundatetime)
-- values
-- --(newid(),'costcenter','SumCheck',100,200,'2025-08-29 18:00:00'),
-- --(newid(),'costcenter','PrimaryKey',1,null,'2025-08-29 18:00:00'),
-- (newid(),'salesline','SumCheck',100,100,'2025-08-29 19:10:00'),
-- (newid(),'salesline','PrimaryKey',0,null,'2025-08-29 19:20:00')





select * from dqr.dqresults where rundatetime>(select watermarkvalue from dqr.incremental_load_mappings where tablename='dqr.dqresults')
