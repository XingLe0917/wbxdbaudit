import logging
import os

from prettytable import PrettyTable
from concurrent.futures.thread import ThreadPoolExecutor
from common.wbxmonitoralertdetail import geWbxmonitoralertdetailVo

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from common.wbxssh import wbxssh
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("DBAMONITOR")

class ShplexDenyMonitorJobAutoFix(WbxJob):
    gserverList = []
    AllProblemServerList = []
    def start(self):
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._errorchannelList = []
        # serverList = []
        uniqueServerList = []
        kargs = {}
        hasError = False
        try:
            auditdao.connect()
            auditdao.startTransaction()
            global gserverList
            gserverList = auditdao.getShplexMonitorServerList()
            logger.info("There are total %s servers" % len(uniqueServerList))

            global AllProblemServerList
            AllProblemServerList = auditdao.getProblemShplexDenyMonForAutoFix()
            needFix = []
            needFix = self.getNeedFixShplexServer(AllProblemServerList)
            self.autoFixShplexPort(needFix)

            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
        finally:
            auditdao.close()
        return hasError

    def getNeedFixShplexServer(self,problem_list):
        needFixList = []
        if len(problem_list) > 0:
            for ls in problem_list:
                if ls["userid_sp"] == "0x00000000" or "no":
                    print("this is null record %s %s"% (ls["host_name"],ls["port_number"]))
                    needFixList.append(ls)

        # uniqueNeedFixServerList = self.getUniqueProblemHostname(needFixList)
        # self.autoFixShplexPort(uniqueNeedFixServerList)
        return needFixList

    def autoFixShplexPort(self, needfix):
        server = None
        hostname = None
        password = None

        if len(needfix) > 0:
            for list in needfix:
                hostname = list["host_name"]
                password = list["F_GET_DEENCRYPT(UI.PWD)"]
                splexport = list["port_number"]
                srcsplexsid = list["src_splex_sid"]
                print("Connect Server:%s, splex port: %s,splex_sid: %s"%(hostname,splexport,srcsplexsid))

                logger.info("Start to Connect Server to fix splex problem %s..." % hostname)
                print("Start to Connect Server to fix splex problem %s..." % hostname)
                try:
                    server = wbxssh(hostname, ssh_port=22, login_user="oracle", login_pwd=password)
                    server.connect()
                    server.verifyConnection()

                    cmd2 = "sh /u00/app/admin/dbarea/bin/splex_deny_monitor_autofix.sh %s %s %s" % (splexport, hostname, srcsplexsid)
                    res2, status2 = server.exec_command(cmd2)

                    # cmd3 = "cat /tmp/sql_output_autofix_%s.log" % splexport
                    # res3, status3 = server.exec_command(cmd3)
                    print("************* fix server %s: port %s ****************" % (hostname, splexport))
                    # print("{0}".format(res3))

                except Exception as e:
                    logger.error("error occured at host %s with password %s" % (hostname, password), exc_info=e)
                    continue
                finally:
                    if server is not None:
                        server.close()

    #get unique hostname from all server list
    def getUniqueProblemHostname(self,serverList):
        l1 = serverList
        l2 = []
        l2.append(l1[0])
        for dict in l1:
            # print len(l4)å
            k = 0
            for item in l2:
                # print 'item'
                if dict['host_name'] != item['host_name']:
                    k = k + 1
                    # continue
                else:
                    break
                if k == len(l2):
                    l2.append(dict)
        return l2

    #get ports by need fix server hostname
    def getPortByNeedFixHostname(self,host_name):
        port = []
        for list1 in self.AllProblemServerList:
            if host_name == list1["src_host"]:
                port.append(list1["shplex_port"])
                print("hostname is %s, port: %s"%(host_name,list1["shplex_port"]))

        return port



if __name__ == "__main__":
    job = ShplexDenyMonitorJobAutoFix()
    job.initialize()
    job.start()
    # job.checkNeedFixShplexPort()