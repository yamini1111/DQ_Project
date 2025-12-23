DROP TABLE IF EXISTS dqr.incremental_load_mappings

CREATE TABLE dqr.incremental_load_mappings(
	tablename varchar(50),
	watermarkcolumn varchar(50),
	watermarkvalue datetime2,
	sqlquery varchar(1000),
	active bit
)


GO

INSERT INTO dqr.incremental_load_mappings(tablename,watermarkcolumn,watermarkvalue,sqlquery,active) VALUES('dqr.dqchecks','lastmodifieddate','2025-01-01 00:00:00','SELECT dqcheckid,dqrulename,dqruletype,lastmodifieddate FROM dqr.dqchecks WHERE lastmodifieddate>',1);
INSERT INTO dqr.incremental_load_mappings(tablename,watermarkcolumn,watermarkvalue,sqlquery,active) VALUES('dqr.dqobjects','lastmodifieddate','2025-01-01 00:00:00','SELECT dqobjectid,dqobjectname,active,lastmodifieddate FROM dqr.dqobjects WHERE lastmodifieddate>',1);
INSERT INTO dqr.incremental_load_mappings(tablename,watermarkcolumn,watermarkvalue,sqlquery,active) VALUES('dqr.dqrules','lastmodifieddate','2025-01-01 00:00:00','SELECT dqruleid,dqcheckid,dqobjectid,sourcelayer,targetlayer,dqattribute1,dqattribute2,sqlquery,lastmodifieddate,Active FROM dqr.dqrules WHERE lastmodifieddate>',1);

select * from dqr.incremental_load_mappings


truncate table dqr.incremental_load_mappings

SELECT * FROM dqr.dqchecks
SELECT * FROM dqr.dqobjects
SELECT * FROM dqr.dqrules


INSERT INTO dqr.incremental_load_mappings(tablename,watermarkcolumn,watermarkvalue,sqlquery,active) VALUES
('dqr.dqresults','rundatetime','2025-01-01 00:00:00',null,1);
