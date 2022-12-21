from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.logmnrDao import logmnrdao

# This job is used to drop deprecated logmnr tables under wbxbackup schema in BTGDIAGS db
# we have a archive log minor script, MSO team execute the script to analyze oracle archive log into BTGDIAGS DB;
# But more and more tables are created, it occupy more and more disk usage
# so we have this job to drop 5 days ago tables
class LogmnrJob(WbxJob):

    def start(self):
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        logmnrdb = None
        try:
            auditdao.connect()
            auditdao.startTransaction()
            dbvolist = auditdao.getDBConnectionList("BTS_PROD", "DIAGNS", "GSB", "DFW01", "SYSTEM")
            logmnrdb = dbvolist[0]
            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
        finally:
            auditdao.close()

        try:
            connectionurl = logmnrdb.getConnectionurl()
            dao = logmnrdao(connectionurl)
            dao.connect()
            dao.startTransaction()
            dropSQLList = dao.listDeprecatedLomnrTable()

            for dropSQL in dropSQLList:
                dao.dropTable(dropSQL)
                break
            dao.commit()
        except Exception as e:
            if dao is not None:
                dao.rollback()
            hasError = True
        finally:
            dao.close()