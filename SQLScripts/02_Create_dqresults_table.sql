
drop table dqr.dqresults

CREATE TABLE dqr.dqresults(
	dqresultid uniqueidentifier primary key default newid(),
	objectname varchar(100),
	sourcelayer varchar(100),
	targetlayer varchar(100),
	rulename varchar(50),
	sourceresult decimal (20,4),
	targetresult decimal (20,4),
	rundatetime datetime2

)


select * from dqr.dqresults