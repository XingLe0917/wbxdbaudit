import logging
import paramiko
from paramiko import SSHClient
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from common.wbxssh import wbxssh

logger = logging.getLogger("dbaudit")

class DepotDBMonitorJob(WbxJob):

    def start(self):
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._errorchannelList = []
        serverList = []
        try:
            auditdao.connect()
            auditdao.startTransaction()
            serverList = auditdao.listOracleServer()
            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
        finally:
            auditdao.close()

        logger.info("There are total %s servers" % len(serverList))
        hasError = False
        for (host_name, loginpwd) in serverList:
            status = self.checkServerInfo(host_name, loginpwd)
            if status != 0:
                hasError = True
        return hasError

    def checkServerInfo(self, host_name, login_pwd):
        client = None
        try:
            logger.info("start to check depotdb data on host %s" % host_name)
            client = wbxssh(host_name, ssh_port=22, login_user="oracle", login_pwd=login_pwd)
            client.connect()
            vRes, status = client.exec_command("sh /staging/gates/depotdbdata_monitor.sh")
            for line in vRes.splitlines():
                if line.find("WBXERROR") >= 0 or line.find("WBXWARNING") > 0:
                    logger.info(line)
            return status
        except Exception as e:
            logger.error("error occured at host %s with password %s" % (host_name, login_pwd), exc_info = e)
        finally:
            if client is not None:
                client.close()