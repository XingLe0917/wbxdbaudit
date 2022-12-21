import logging
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from dao.vo.wbxcrmonitorvo import WbxcrlogVo
from common.wbxutil import wbxutil


logger = logging.getLogger("DBAMONITOR")

class MonitorCRLogOnWebDBJob(WbxJob):

    def start(self):
        dblist = []
        # splexportlist = []
        global splexportlist
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            web_db = self.auditdao.getDBConnectionList("ALL", "WEB", "ALL", "ALL", "SYSTEM")
            csp_db = self.auditdao.getDBConnectionList("ALL", "CSP", "ALL", "ALL", "SYSTEM")
            dblist = web_db+csp_db
            host_name = ""
            splex_port = ""
            splexportlist = self.auditdao.getWEBDBShareplexPort(host_name,splex_port)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

        with ThreadPoolExecutor(max_workers=5) as t:
            all_task = []
            for item in dblist:
                logger.info("========================  {0} =========================== ".format(item))
                obj = t.submit(self.checkCRLog, item)
                all_task.append(obj)
            num = 0
            for future in as_completed(all_task):
                num += 1
                res = future.result()
                if num == len(dblist):
                    logger.info("All tasks finish!")


    def checkCRLog(self,dbitem):
        print("Start to get shareplex wbxcrlog count...")
        error_list = []
        datamonitordao = None
        try:
            wbxcrlogvo = WbxcrlogVo()
            wbxcrlogvo.db_name = dbitem.getDBName()
            wbxcrlogvo.trim_host = dbitem.getTrimHost()
            wbxcrlogvo.conflictdate = wbxutil.getcurrenttime(86400)
            wbxcrlogvo.collecttime = wbxutil.getcurrenttime()
            splex_port = None
            host_name = ""
            for sl in splexportlist:
                if wbxcrlogvo.db_name == sl["db_name"]:
                    splex_port = sl["splex_port"]
                    host_name = sl["host_name"]

            if splex_port:
                splex_schema = "splex%s" % splex_port
                datamonitordao = dbdatamonitordao(dbitem.getConnectionurl())
                print("DB connectionurl: %s" % dbitem.getConnectionurl())
                datamonitordao.connect()
                datamonitordao.startTransaction()
                crlognum = datamonitordao.getwbxcrlognum(splex_schema)
                datamonitordao.commit()

                wbxcrlogvo.splex_port = str(splex_port)
                wbxcrlogvo.host_name = host_name
                wbxcrlogvo.crcount = str(crlognum)
                # self.deletewbxcrlog(wbxcrlogvo)
                self.insertwbxcrlog(wbxcrlogvo)
        except Exception as e:
            datamonitordao.rollback()
            print(e)
        finally:
            if datamonitordao:
                datamonitordao.close()
            if self.auditdao:
                self.auditdao.close()

    def insertwbxcrlog(self,wbxcrlogvo):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            # self.auditdao.delete_wbxcrlog_one(wbxcrlogvo)
            # self.auditdao.commit()
            self.auditdao.add_wbxcrlog_one(wbxcrlogvo)
            self.auditdao.commit()
            print("Insert %s %s data to wbxcrlog, crlog count is: %s " % (
            wbxcrlogvo.db_name, wbxcrlogvo.conflictdate, wbxcrlogvo.crcount))
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    def deletewbxcrlog(self,wbxcrlogvo):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.delete_wbxcrlog_one(wbxcrlogvo)
            self.auditdao.commit()
            print("Delete %s %s data to wbxcrlog, crlog count is: %s " % (
            wbxcrlogvo.db_name, wbxcrlogvo.conflictdate, wbxcrlogvo.crcount))
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

if __name__ == "__main__":
    job = MonitorCRLogOnWebDBJob()
    job.initialize()
    job.start()
