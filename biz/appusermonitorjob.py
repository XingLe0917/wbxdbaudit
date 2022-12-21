import logging
import uuid
import cx_Oracle

from biz.wbxjob import WbxJob
from common.config import Config
from dao.vo.wbxautotaskjobvo import wbxautotaskvo, wbxautotaskjobvo
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

class AppuserMonitorJob(WbxJob):

    def start(self):
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        rows = None
        try:
            auditdao.connect()
            auditdao.startTransaction()
            rows = auditdao.listdbappuser()
            auditdao.commit()
            logger.info("WBXINFO: verify %s db user password profile" % len(rows))
        except Exception as e:
            auditdao.rollback()
            logger.error(e)
        finally:
            auditdao.close()

        faileddbcount = 0

        if rows is not None:
            for row in rows:
                if row[3] is not None:
                    logger.info("verify app user password profile for db %s (users: %s)" % (row[1], row[3]))
                    issucceed = self.verifyUserProfile(row[1], row[2], row[3])
                    faileddbcount = faileddbcount + 0 if issucceed else 1
        if faileddbcount > 0:
            logger.error("%d db failed verification" % faileddbcount)
            return False
        return True

    def verifyUserProfile(self, db_name, connectionurl, users):
        conn = None
        csr = None
        issucceed = True
        try:
            usernames = ''
            for username in users.split(","):
                usernames = '''%s,'%s' ''' % (usernames, username)
            SQL = '''
                  SELECT  du.username, dp.resource_name, dp.limit 
                  FROM dba_users du, dba_profiles dp  
                  WHERE du.profile=dp.profile(+) 
                  and dp.resource_name(+)='PASSWORD_LIFE_TIME'
                  and du.username in (%s)
                ''' % usernames.strip(",")
            conn = cx_Oracle.connect(user="system", password="sysnotallow", dsn=connectionurl)
            csr = conn.cursor()
            rows = csr.execute(SQL).fetchall()
            for row in rows:
                if row[1] is None or row[2] != "UNLIMITED":
                    logger.error("WBXERROR: user %spassword profile PASSWORD_LIFE_TIME value is %s in db %s, but should be UNLIMITED" %(row[0], db_name, row[2]))
                    issucceed = False
            conn.commit()
        except Exception as e:
            logger.error("AppuserMonitorJob.verifyUserProfile(%s, %s) failed: " % (db_name, connectionurl), exc_info = e)
        finally:
            try:
                if csr is not None:
                    csr.close()
                if conn is not None:
                    conn.close()
            except Exception as e:
                pass
        return  issucceed