import logging
import re
import time

from concurrent.futures.thread import ThreadPoolExecutor
from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from common.wbxssh import wbxssh
from dao.wbxauditdbdao import wbxauditdbdao
from dao.wbxdao import wbxdao
from common.splexparamdetail import splexdetailVo
from datetime import datetime
from prettytable import PrettyTable

logger = logging.getLogger("DBAMONITOR")


class MergeSplexDenyIntoDB(WbxJob):

    error = False
    success = True
    paramnameList = []

    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._errorchannelList = []
        gserverList = []
        kargs = {}
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            gserverList = self.auditdao.getShplexParamsServerList()
            logger.info("There are total %s servers" % len(gserverList))


        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

        try:
            # Muti-thread call
            executor = ThreadPoolExecutor(max_workers=1)
            data = []
            start_time = datetime.now()
            print("Now start to insert data, time is %s" % (start_time))
            i = 1
            for res1 in executor.map(self.mergeintodb, gserverList):
                print("in server {}: Call mergeintodb result is {}".format(i, res1))
                i += 1
            print("Insert data completed, elapsed time is %s" % (datetime.now() - start_time))
            executor.shutdown(wait=True)

        except Exception as e:
            print(e)
        return self.success

    def mergeintodb(self,server_list):
        hostname = server_list["host_name"]
        password = server_list["password"]
        username = server_list["user_name"]
        res = {"status": "SUCCESS", "errormsg": "", "data": None}
        splexdetailVo_list = []
        shplex_port = server_list["port_list"].split(",")
        if len(shplex_port) == 0:
            logger.info("No SharePlex ports on Server %s" % hostname)
            return self.error

        try:

            for port in shplex_port:
                print(port)
                logger.info("*************** insert record of hostname: port: %s ***************" % hostname,port)
                splexdetailVo_list = self.insertParamDataToDB(hostname,port)
                print("Insert server: %s record parameters to DB successfully!" % (hostname))
        except Exception as e:
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred,parse parameters , host_name={0}, e={1}, ".format(hostname, str(e))
            return res
        return self.success

    def insertParamDataToDB(self, hostname,port):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.mergeIntoSplexDenyInfo(hostname,port,"wbxsplex")
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

if __name__ == "__main__":
    job = MergeSplexDenyIntoDB()
    job.initialize()
    job.start()
