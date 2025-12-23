
CREATE VIEW dqr.Vw_Rules
AS
SELECT R.*,C.dqrulename,C.dqruletype,O.dqobjectname FROM dqr.dqrules R JOIN dqr.dqchecks C on R.dqcheckid = C.dqcheckid
JOIN dqr.dqobjects O ON R.dqobjectid = O.dqobjectid
WHERE R.Active = 1

go

SELECT * FROM dqr.Vw_Rules