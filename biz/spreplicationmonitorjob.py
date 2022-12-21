import logging

from biz.wbxjob import WbxJob
from common.wbxutil import wbxutil
from common.config import Config
from db.wbxdatabase import wbxdbmanager

logger = logging.getLogger("dbaudit")

class SPReplicationMonitorJob(WbxJob):

    def initialize(self):
        config = Config()
        schemaname, schemapwd, connectionurl = config.getDepotConnectionurl()
        self.depot_connectionurl = "%s:%s@%s" % (schemaname, schemapwd, connectionurl)
        self._dbNameList = config.getShareplexMonitorDBList()
        self._interval = config.getShareplexMonitorInterval()
        self._env = config.getShareplexMonitorEnv()

    def start(self):
        dbmanager = wbxdbmanager()
        depotdb = dbmanager.getDefaultDatabase()
        conn = csr = None
        try:
            depotdb = dbmanager.getDatabase("DEFAULT")
            conn, csr = depotdb.getConnnection()
            depotdb.startTransaction(conn)
            depotdb.verifyConnection(conn, csr)
            depotdb.commit(conn)
            logger.info("verify depotdb connection info successfully")
        except Exception as e:
            logger.error("verify depotdb connection info failed", exc_info=e)
            if depotdb is not None:
                depotdb.rollback()
        finally:
            depotdb.close(conn, csr)
        dbInfoDict = self.getDBInfo()
        for db_name, dbInfo in dbInfoDict.items():
            pass

    def getDBInfo(self):
        dbmanager = wbxdbmanager()
        dbinfoDict = {}
        conn = csr = None
        for db_name in self._dbNameList:
            bizdb = dbmanager.getDatabase(db_name)
            try:
                conn, csr = bizdb.getConnnection()
                bizdb.startTransaction(conn)
                bizdb.verifyConnection(conn, csr)
                bizdb.commit(conn)
                dbinfoDict["db_name"]["connectioninfo"] = bizdb
                # logger.info("verify %s connection info successfully" % db_name)
            except Exception as e:
                # logger.error("verify %s connection info failed" % db_name)
                bizdb.rollback(conn)
                raise e
            finally:
                bizdb.close(conn, csr)

        depotdb = dbmanager.getDatabase("DEFAULT")
        try:
            conn, csr = depotdb.getConnnection()
            depotdb.startTransaction(conn)
            for db_name in self._dbNameList:
                spportlist = depotdb.getShareplexPortList(db_name, self._env)
                dbinfoDict["db_name"]["spportlist"] = spportlist
            depotdb.commit(conn)
            logger.info("verify depotdb connection info successfully")
        except Exception as e:
            logger.error("verify depotdb connection info failed", exc_info=e)
            if depotdb is not None:
                depotdb.rollback()
        finally:
            depotdb.close(conn, csr)
        return dbinfoDict



