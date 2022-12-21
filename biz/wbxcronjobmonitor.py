from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from common.wbxchatbot import wbxchatbot
from concurrent.futures.thread import ThreadPoolExecutor
from common.wbxssh import wbxssh
import sys
import logging
'''
Desc:   Run Server Host Shell Script
Input : Shell script Name
OutPut: Log Each Object Status
'''

logger = logging.getLogger("dbaudit")
class WbxCronjobMonitor(WbxJob):

    def start(self):
        jobname = self.args[0]
        hasError = False
        hostvolist = []
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            logger.info('Get All Job Host Now')
            auditdao.connect()
            auditdao.startTransaction()
            hostvolist = auditdao.getCronjobhostbyjob(jobname)
            auditdao.commit()

            executor = ThreadPoolExecutor(max_workers=20)
            data=[]
            for res in executor.map(self.runJobOnHost, hostvolist):
                if len(res)>0:
                    [data.append(i) for i in res]

            auditdao.startTransaction()
            self.persistentIntoOracle(jobname,auditdao,*data)
            auditdao.commit()

            logger.info('Get All DB Svr ExcuteSQL End')
        except Exception as e:
            auditdao.rollback()
            logger.error(e)
            hasError = True
        finally:
            auditdao.close()
        return hasError

    def runJobOnHost(self,hostvo):
        host_name = hostvo.getHostName()
        pwd = hostvo.getpwd()
        paramsList = hostvo.getParams().split(',')
        commandstr = hostvo.getCommandstr()
        server = wbxssh(host_name, 22, "oracle", pwd)
        res=[]
        try:
            server.connect()
            for param in paramsList:
                cmd = commandstr + " " + param
                retcode, status = server.exec_command(cmd)
                status = "SUCCESS" if status else "FAILED"
                logger.info("Target on [ %s ] [ %s ] is Done! End With [ %s ] !,[%s]" % (host_name,param, status,cmd))
                data={"host_name":host_name,"target":param,"status":status}
                res.append(data)
        except Exception as e:
            logger.error(e)
        finally:
            server.close()
        return res

    def persistentIntoOracle(self,jobname,auditdao,*args):
        for item in args:
            paramList = [jobname,item["host_name"],item["target"],item["status"]]
            auditdao.updateWbxCronJobMonStatus(*paramList)

if __name__ == '__main__':
    job = WbxCronjobMonitor()
    job.initialize("send alert email")
    job.start()
