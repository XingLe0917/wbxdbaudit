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
from common.wbxmonitoralertdetail import geWbxmonitoralertdetailVo

logger = logging.getLogger("DBAMONITOR")

class CheckConflictResolution(WbxJob):
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
            self.auditdao.commit()

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
            for res1 in executor.map(self.checkFile, gserverList):
                print("in server {}: Call getShareplexVardir result is {}".format(i, res1))
                i += 1
            print("CheckFile completed, elapsed time is %s" % (datetime.now() - start_time))
            executor.shutdown(wait=True)
        except Exception as e:
            print(e)
        return self.success
        pass

    def checkFile(self,serverList):
        hostname = serverList["host_name"]
        password = serverList["password"]
        username = serverList["user_name"]
        res = {"status": "SUCCESS", "errormsg": "", "data": None}

        shplex_port = serverList["port_list"].split(",")
        if len(shplex_port) == 0:
            logger.info("No SharePlex ports on Server %s" % hostname)
            return self.error

        try:
            logger.info("Start to Connect Server %s..." % hostname)
            print("Start to Connect Server %s..." % hostname)
            server = wbxssh(hostname, ssh_port=22, login_user=username, login_pwd=password)
            server.connect()
            if server.isconnected == False:
                logger.info("Server %s can't access" % hostname)
                print("Server %s can't access" % hostname)
                return self.error

            vardir = self.getShareplexVardir(server)

            cmd = "ps -efa | grep sp_cop | grep -v grep | awk {'print $NF'} | egrep -v '20001|20002'"
            content, status = server.exec_command(cmd)
            if status != True:
                res["status"] = "FAILED"
                res["errormsg"] = "Can't find any port on server: host_name={0}".format(hostname)
                return res

            res_port = content.split('\n')

            splexdetailVo_list_insert = []
            step = 4
            for i in range(0, len(res_port), step):
                # print(shplex_port[i:i + step])
                divide_shplex_port = res_port[i:i + step]
                for port in divide_shplex_port:
                    # print(port)
                    vardir_port = "/%s/vardir_%s/data" % (vardir, port)
                    filename = "%s/conflict_resolution.*" % (vardir_port)
                    cmd1 = "ls %s|awk -F '.' '{print $2}'" % (filename)
                    res1, status1 = server.exec_command(cmd1)

                    cmd2 = "cat %s | wc -l" % (filename)
                    res2, status2 = server.exec_command(cmd2)
                    if status2:
                        print("successfuly get rs content")
                        if res2 == '0':
                            check_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            kargs = {
                                "task_type": "CONFLICT_RESOLUTION",
                                "db_name": "",
                                "host_name": hostname,
                                "user_name": username,
                                "splex_port": port,
                                "check_stat": "N",
                                "check_time": check_time,
                                "splex_sid": res1,
                                "describe": "This conflict resolution file is empty!"
                            }
                            wbxmonitoralertdetailVo = geWbxmonitoralertdetailVo(**kargs)
                            # self.insertParamDataToDB(wbxmonitoralertdetailVo)
                            splexdetailVo_list_insert.append(wbxmonitoralertdetailVo)
                            print("This conflict resolution file is empty!")
                        else:
                            print("This conflict resolution file has rules!")
                    logger.info("*************** get conflict resolution file of hostname:%s, port: %s ***************" % (hostname, port))
                    print("*************** get conflict resolution file of hostname:%s, port: %s ***************" % (hostname, port))
                    # logger.info("*************** get parameters of port: %s ***************" % port)
                    # splexdetailVo_list = self.getSplexParamDatail(server, param_prefix, param_dict, hostname, port)
                    # splexdetailVo_list_insert += splexdetailVo_list

            if len(splexdetailVo_list_insert) > 0:
                self.insertParamDataToDB(splexdetailVo_list_insert)
                splexdetailVo_list_insert.clear()
                print("Insert server: %s shareplex parameters to DB successfully!" % (hostname))

        except Exception as e:
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred,parse parameters , host_name={0}, e={1}, ".format(hostname, str(e))
            return res
        return self.success


    def getShareplexVardir(self,server):
        dir = ""
        res = {"status": "SUCCESS", "errormsg": "", "bin_path": None, "content": ""}
        cmd1 = "ps -ef | grep sp_cop | grep -v grep | awk '{print $8}' | sed -n '1p'"
        logger.info(cmd1)
        res1, status1 = server.exec_command(cmd1)
        re1 = re.match(r'(.*).app-modules(.*)', res1)
        re2 = res1.split('/')
        dir = re2[1]
        return dir

    def insertParamDataToDB(self, wbxmonitoralertdetailVo_l):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.insert_wbxmonitoralertdetail_all(wbxmonitoralertdetailVo_l)
            # self.auditdao.add_splexparamdetail(server_params_list)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()


if __name__ == "__main__":
    job = CheckConflictResolution()
    job.initialize()
    job.start()
