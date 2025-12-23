
--drop table if exists dqr.dqchecks
--drop table if exists dqr.dqobjects
--drop table if exists dqr.dqrules

GO

CREATE SCHEMA dqr

GO


CREATE TABLE dqr.dqchecks(
	dqcheckid int primary key,	
	dqrulename varchar(100),	
	dqruletype varchar(100),
	lastmodifieddate datetime2
)

GO

INSERT INTO dqr.dqchecks(dqcheckid,dqrulename,dqruletype,lastmodifieddate) VALUES (1,'PrimaryKey','Single','2025-08-25 12:29:00')
INSERT INTO dqr.dqchecks(dqcheckid,dqrulename,dqruletype,lastmodifieddate) VALUES (2,'RecordCount','Comparison','2025-08-25 12:29:00')
INSERT INTO dqr.dqchecks(dqcheckid,dqrulename,dqruletype,lastmodifieddate) VALUES (3,'SumCheck','Comparison','2025-08-25 12:29:00')
INSERT INTO dqr.dqchecks(dqcheckid,dqrulename,dqruletype,lastmodifieddate) VALUES (4,'Nullcheck','Single','2025-08-25 12:29:00')

GO

CREATE TABLE dqr.dqobjects(
	dqobjectid int primary key,	
	dqobjectname varchar(100),	
	active bit,
	lastmodifieddate datetime2
)



INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (1,'costcenter',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (2,'currency',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (3,'custtable',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (4,'fiscalperiod',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (5,'parties',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (6,'partyaddress',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (7,'promotable',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (8,'purchaseorder',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (9,'purchcategory',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (10,'purchcontracts',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (11,'purchitem',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (12,'salesorderline',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (13,'vendtable',1,'2025-08-25 12:23:00')
INSERT INTO dqr.dqobjects(dqobjectid,dqobjectname,active,lastmodifieddate) VALUES (14,'workertable',1,'2025-08-25 12:23:00')


CREATE TABLE dqr.dqrules(
	dqruleid INT PRIMARY KEY,
	dqcheckid INT FOREIGN KEY REFERENCES dqr.dqchecks(dqcheckid),
	dqobjectid INT FOREIGN KEY REFERENCES dqr.dqobjects(dqobjectid),
	sourcelayer	varchar(50),
	targetlayer	varchar(50),
	dqattribute1	varchar(50),
	dqattribute2	varchar(50),
	sqlquery	varchar(max),
	lastmodifieddate	datetime2,
	Active bit

)

INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (1,1,1,'bronze','NA','CostCenterNumber','',
'SELECT CostCenterNumber, COUNT(*) AS DuplicateCount FROM costcenter GROUP BY CostCenterNumber HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1)

INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (2,1,2,'bronze','NA','CurrencyId','','SELECT CurrencyId, COUNT(*) AS DuplicateCount 
FROM currency 
GROUP BY CurrencyId 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (3,1,3,'bronze','NA','CustomerId','','SELECT CustomerId, COUNT(*) AS DuplicateCount 
FROM custtable 
GROUP BY CustomerId 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (4,1,4,'bronze','NA','FiscalPeriodName','','SELECT FiscalPeriodName, COUNT(*) AS DuplicateCount 
FROM fiscalperiod 
GROUP BY FiscalPeriodName 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (5,1,5,'bronze','NA','PartyId','','SELECT PartyId, COUNT(*) AS DuplicateCount 
FROM parties 
GROUP BY PartyId 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (6,1,6,'bronze','NA','PartyNumber','','SELECT PartyNumber, COUNT(*) AS DuplicateCount 
FROM partyaddress 
GROUP BY PartyNumber 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (7,1,7,'bronze','NA','PromotionId','','SELECT PromotionId, COUNT(*) AS DuplicateCount 
FROM promotable 
GROUP BY PromotionId 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (8,1,8,'bronze','NA','PoNumber,LineItem','','SELECT PoNumber,LineItem, COUNT(*) AS DuplicateCount 
FROM purchaseorder 
GROUP BY PoNumber,LineItem 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (9,1,9,'bronze','NA','CategoryId','','SELECT CategoryId, COUNT(*) AS DuplicateCount 
FROM purchcategory 
GROUP BY CategoryId 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (10,1,10,'bronze','NA','ContractId','','SELECT ContractId, COUNT(*) AS DuplicateCount 
FROM purchcontracts 
GROUP BY ContractId 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (11,1,11,'bronze','NA','ItemId','','SELECT ItemId, COUNT(*) AS DuplicateCount 
FROM purchitem 
GROUP BY ItemId 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (12,1,12,'bronze','NA','SalesOrderNumber,SalesOrderLine','','SELECT SalesOrderNumber,SalesOrderLine, COUNT(*) AS DuplicateCount 
FROM salesorderline 
GROUP BY SalesOrderNumber,SalesOrderLine 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (13,1,13,'bronze','NA','VendId','','SELECT VendId, COUNT(*) AS DuplicateCount 
FROM vendtable 
GROUP BY VendId 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);
INSERT INTO dqr.dqrules(dqruleid,	dqcheckid,	dqobjectid,	sourcelayer	,targetlayer,	dqattribute1,	dqattribute2,	sqlquery,	lastmodifieddate	,Active) VALUES (14,1,14,'bronze','NA','WorkerID','','SELECT WorkerID, COUNT(*) AS DuplicateCount 
FROM workertable 
GROUP BY WorkerID 
HAVING COUNT(*) > 1;','2025-08-25 13:36:00',1);

update dqr.dqchecks set dqrulename = 'PrimaryKeyCheck' where dqcheckid = 1

SELECT * FROM dqr.dqchecks
SELECT * FROM dqr.dqobjects
SELECT * FROM dqr.dqrules