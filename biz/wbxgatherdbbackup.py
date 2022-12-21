from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from concurrent.futures.thread import ThreadPoolExecutor
import logging
from concurrent.futures import as_completed
logger = logging.getLogger("dbaudit")
'''
Desc:  Gather All DataBase Rman Backup Info 
'''
class wbxgatherdbbackup(WbxJob):

    def start(self):
        hasError = False
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            logger.info("DataBase Backup Info Collect Task Start ...")
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getDBConnectionList("ALL", "ALL", "ALL", "ALL", "SYSTEM")
            self.auditdao.commit()


            logger.info("Process Will Run On Each Database...")

            Errmsg=[]
            executor = ThreadPoolExecutor(3)
            fs = [executor.submit(self.runOnEachDBSchema, dbvo) for dbvo in dbvolist]
            for f in as_completed(fs):
                res = f.result()
                if res["status"]=="SUCCEED":
                    self.auditdao.startTransaction()
                    self.loadintooracle(**res)
                    self.auditdao.commit()
                else:
                    Errmsg.append(res)
            if len(Errmsg)>0:
                logger.info("Error count:%s++++++++++++++++++++++++++++++++++++++++++++++++++" %str(len(Errmsg)))
                for item in Errmsg:
                    logger.info(item)

            logger.info("DataBase Backup Info Collect Task End .")
        except Exception as e:
            logger.error(str(e))
            self.auditdao.rollback()
            hasError = True
        finally:
            self.auditdao.close()
        return hasError

    def runOnEachDBSchema(self,dbvo):
        dao = None
        trim_host=dbvo.getTrimHost()
        db_name=dbvo.getDBName()
        res = {"status": "SUCCEED"}
        res["trim_host"] = trim_host
        res["db_name"] = db_name
        try:
            connectionurl = dbvo.getConnectionurl()
            dao = dbdatamonitordao(connectionurl)
            dao.connect()
            dao.startTransaction()
            rows = dao.wbxgatherdbbackupinfo()
            res["data"]=rows
            dao.commit()
        except Exception as e:
            logger.error("trim_host:%s , db_name:%s job meet an error:%s" %(trim_host,db_name,str(e)))
            res["status"]= "FAILED"
            if dao is not None:
                dao.rollback()
        finally:
            dao.close()
        return res

    def loadintooracle(self,**kargs):
        trim_host=kargs["trim_host"]
        db_name=kargs["db_name"]
        logger.info("%s %s begin,rowcount %s ..." %(trim_host,db_name,str(len(kargs["data"]))))
        for backupinfo in kargs["data"]:
            backupinfo["db_name"]=db_name
            backupinfo["trim_host"]=trim_host
            self.auditdao.backupinfoloadintooracle(**backupinfo)
        logger.info("%s %s end ." %(trim_host,db_name))

if __name__ == '__main__':
    job = wbxgatherdbbackup()
    job.initialize()
    job.start()
