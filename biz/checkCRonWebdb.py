import logging
import re
import os
import time
from datetime import datetime

from prettytable import PrettyTable
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.vo.wbxcrmonitorvo import WbxcrmonitordetailVo
from common.wbxssh import wbxssh
from common.wbxutil import wbxutil
from common.wbxmonitoralertdetail import geWbxmonitoralertdetailVo
from common.wbxchatbot import wbxchatbot



logger = logging.getLogger("DBAMONITOR")

class CheckCRonWebDBJob(WbxJob):
    error = False
    success = True
    paramnameList = []

    def start(self):
        self.check_host_name = ""
        self.check_port_number = ""
        if len(self.args) < 1:
            print("please input 'ALL' or correct hostname")
            return self.error
        if self.args[0] != "ALL":
            self.check_host_name = self.args[0]
            logger.info("check_host_name=%s" % (self.check_host_name))
            self.check_port_number = self.args[1]
            logger.info("check_port_number=%s" % (self.check_port_number))
        self.checkserverlist = []
        self.main()

    def main(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._errorchannelList = []
        gserverList = []
        if self.check_host_name:
            try:
                self.auditdao.connect()
                self.auditdao.startTransaction()
                serverinfo = self.auditdao.getWEBDBShareplexPort(self.check_host_name,self.check_port_number)
                self.auditdao.commit()
                self.checkCR(*serverinfo)
                logger.info("Check CR of servers %s port: %s" % (self.check_host_name,self.check_port_number))

            except Exception as e:
                self.auditdao.rollback()
                print(e)
            finally:
                self.auditdao.close()
        else:
            try:
                self.auditdao.connect()
                self.auditdao.startTransaction()
                gserverList = self.auditdao.getWEBDBShareplexPort(self.check_host_name,self.check_port_number)
                logger.info("There are total %s servers" % len(gserverList))
                self.auditdao.commit()

            except Exception as e:
                self.auditdao.rollback()
                print(e)
            finally:
                self.auditdao.close()

            with ThreadPoolExecutor(max_workers=5) as t:
                all_task = []
                for item in gserverList:
                    logger.info("========================  {0} =========================== ".format(item))
                    obj = t.submit(self.checkCR, item)
                    all_task.append(obj)
                num = 0

                for future in as_completed(all_task):
                    num += 1
                    res = future.result()
                    if num == len(gserverList):
                        logger.info("All tasks finish!")
                        print("All tasks finish!")

                failed = []
                try:
                    self.auditdao.connect()
                    self.auditdao.startTransaction()
                    failed = self.auditdao.get_wbxcrmonitordetail_failed()
                    self.auditdao.commit()
                except Exception as e:
                    self.auditdao.rollback()
                    logger.error(e)
                finally:
                    self.auditdao.close()
                self.sendAlertMsg("ChatBot",failed)

    #check SP_OPO_SUPPRESSED_OOS
    def checkCR(self,serverList):
        hostname = serverList["host_name"]
        password = serverList["password"]
        splex_port = serverList["splex_port"]
        splex_sid = serverList["splex_sid"]
        username = "oracle"
        result = {"status": "SUCCESS", "errormsg_rule": "", "errormsg_param": "", "data": None}
        param_postfile = "post_params_%s.log" % splex_port

        parameters_file = os.path.join("/tmp", param_postfile)
        if os.path.isfile(parameters_file):
            os.remove(parameters_file)

        self.deletecrmonitorDB(hostname,splex_port)
        splexdetailVo_list_insert = []
        kargs = {}
        logger.info("Start to Connect Server %s..." % hostname)
        print("Start to Connect Server %s..." % hostname)
        server = wbxssh(hostname, 22, username, password)
        try:
            var_dir = ""
            prod_dir = ""
            server.connect()
            # cmd = '''ps aux | grep  sp_cop | grep %s | sed "s/-u//g" | awk '{print $((NF-1))}' | awk -F/ '{print "/"$2"/"$3"/bin"}' ''' % port
            cmd = '''ps aux | grep  sp_cop | grep %s | sed "s/-u//g" | awk '{print $((NF-1))}' | awk -F/ '{print "/"$2"/"$3"/bin/.profile_%s"}' | xargs cat | grep -E 'SP_SYS_VARDIR|SP_SYS_PRODDIR' | awk -F\; '{print $1}' ''' % (
            splex_port, splex_port)
            res, status = server.exec_command(cmd)
            if res is not None:
                for line in res.split("\n"):
                    if line.find("SP_SYS_VARDIR") >= 0:
                        var_dir = line.split("=")[1]
                    elif line.find("SP_SYS_PRODDIR") >= 0:
                        prod_dir = line.split("=")[1]

                crfile = "%s/data/conflict_resolution.%s" % (var_dir, splex_sid)
                profile_name = "%s/bin/.profile_u%s" % (prod_dir, splex_port)

                if server.isFile(crfile):
                    cmd1 = "cat %s | wc -l" % (crfile)
                    res1, status1 = server.exec_command(cmd1)
                    if status1:
                        # print("successfuly get rs content")
                        if res1 == '0':
                            check_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                            kargs = {
                                "task_type": "CONFLICT_RESOLUTION",
                                "db_name": "",
                                "host_name": hostname,
                                "user_name": username,
                                "splex_port": splex_port,
                                "check_stat": "N",
                                "check_time": check_time,
                                "splex_sid": splex_sid,
                                "describe": "This conflict resolution file is empty!"
                            }
                            wbxmonitoralertdetailVo = geWbxmonitoralertdetailVo(**kargs)
                            # self.insertParamDataToDB(wbxmonitoralertdetailVo)
                            splexdetailVo_list_insert.append(wbxmonitoralertdetailVo)
                            self.insertmonitoralterDB(splexdetailVo_list_insert)
                            result["status"] = "FAILED"
                            result["errormsg_rule"] = "This conflict resolution file is empty!"
                            print("This conflict resolution file is empty!")
                        else:
                            print("This conflict resolution file has rules!")
                    else:
                        result["status"] = "FAILED"
                        result["errormsg_rule"] = "Conflict resolution file not found!"
                        print("Conflict resolution file not found!")

                cmd2 = '''
cd %s/bin;
source %s;
./sp_ctrl << EOF | tee /tmp/%s
list param all post
EOF''' % (prod_dir, profile_name, param_postfile)
                res2, status2 = server.exec_command(cmd2)
                logger.info("port=%s on host=%s with result %s" % (splex_port, hostname, res2))

                cmd3 = "cat /tmp/%s |grep -B 1 'Default'|sed '/SP_/{N;s/\\n/\\t/}' |grep -v 'SP_SYS_LIC_\\|SP_OCT_ASM_SID\\|SP_ORD_LOGIN_O.ABLOOKUP_SPLEX'|grep 'SP_'|grep -v Queue|awk '{print $1,$2,$NF}'|grep -w 'SP_OPO_SUPPRESSED_OOS' " % (param_postfile)
                content3, status3 = server.exec_command(cmd3)
                collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                c1 = content3.split(' ')
                if len(c1) == 3:
                    kargs = {
                        "host_name": hostname,
                        "port_number": splex_port,
                        "param_category": "post",
                        "param_name": c1[0],
                        "queue_name": "None",
                        "actual_value": c1[1],
                        "default_value": c1[2],
                        "collect_time": collect_time,
                        "ismodified": "Y"
                    }
                    print("Actual Value is correct of %s %s %s" % (hostname, splex_port, c1[0]))
                elif len(c1) == 2:
                    kargs = {
                        "host_name": hostname,
                        "port_number": splex_port,
                        "param_category": "post",
                        "param_name": c1[0],
                        "queue_name": "None",
                        "actual_value": c1[1],
                        "default_value": c1[1],
                        "collect_time": collect_time,
                        "ismodified": "N"
                    }
                    result["status"] = "FAILED"
                    result["errormsg_param"] = "%s %s %s parameter is default value." % (hostname, splex_port, c1[0])
                    print("%s %s %s parameter is default value." % (hostname, splex_port, c1[0]))
                else:
                    cmd4 = "cat /tmp/%s |grep -C 1 'Queue' |sed '/SP_/{N;s/\\n/\\t/}'|sed '/SP_/{N;s/\\n/\\t/}'|awk '{print $1,$2,$NF}'|grep -w 'SP_OPO_SUPPRESSED_OOS' " % (
                        param_postfile)
                    content4, status4 = server.exec_command(cmd4)
                    collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    c4 = content4.split(' ')
                    if len(c4) == 3:
                        kargs = {
                            "host_name": hostname,
                            "port_number": splex_port,
                            "param_category": "post",
                            "param_name": c4[0],
                            "queue_name": "None",
                            "actual_value": c4[1],
                            "default_value": c4[2],
                            "collect_time": collect_time,
                            "ismodified": "Y"
                        }
                        print("Actual Value is correct of %s %s %s" % (hostname, splex_port, c4[0]))
                    elif len(c4) == 2:
                        kargs = {
                            "host_name": hostname,
                            "port_number": splex_port,
                            "param_category": "post",
                            "param_name": c4[0],
                            "queue_name": "None",
                            "actual_value": c4[1],
                            "default_value": c4[1],
                            "collect_time": collect_time,
                            "ismodified": "N"
                        }
                        result["status"] = "FAILED"
                        result["errormsg_param"] = "%s %s %s parameter is default value." % (
                        hostname, splex_port, c4[0])
                        print("%s %s %s parameter is default value." % (hostname, splex_port, c4[0]))
                    else:
                        result["status"] = "FAILED"
                        result["errormsg_param"] = "Actual Value is not correct of %s %s %s" % (hostname, splex_port, c4[0])
                        print("Actual Value is not correct of %s %s %s" % (hostname, splex_port, c4[0]))

                # splexdetailVo_l = splexdetailVo(**kargs)
                # self.insertsplexparamDB(splexdetailVo_l)

                wbxcrmonitorvo = WbxcrmonitordetailVo()
                wbxcrmonitorvo.host_name = hostname
                wbxcrmonitorvo.db_name = serverList["db_name"]
                wbxcrmonitorvo.trim_host = serverList["trim_host"]
                wbxcrmonitorvo.splex_port = str(splex_port)
                wbxcrmonitorvo.status = result["status"]
                wbxcrmonitorvo.errormsg = result["errormsg_param"]+result["errormsg_rule"]
                wbxcrmonitorvo.collecttime = wbxutil.getcurrenttime()

                self.insertcrmonitorDB(wbxcrmonitorvo)

        except Exception as e:
            self.auditdao.rollback()
            logger.info("check shareplex port %s CR on server %s failed" % (splex_port, hostname),
                exc_info=e)
        finally:
            server.close()
            self.auditdao.close()
        return result

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

    def insertcrmonitorDB(self, wbxcrmonitordetailVo):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.add_wbxcrmonitordetail_one(wbxcrmonitordetailVo)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    def deletecrmonitorDB(self,host_name,splex_port):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.delete_wbxcrmonitordetail_one(host_name,splex_port)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    def insertmonitoralterDB(self, wbxmonitoralertdetailVo_l):
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

    def insertsplexparamDB(self, wbxmonitoralertdetailVo):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.delete_splexparamdetail_byparamname(wbxmonitoralertdetailVo,wbxmonitoralertdetailVo.param_name)
            self.auditdao.commit()
            self.auditdao.add_splexparamdetail_one(wbxmonitoralertdetailVo)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    # send message to chatbot/email
    def sendAlertMsg(self, Target,server_list):
        msg = ' Send Alert Message'
        print(msg)
        hasError = False
        bot_server_list = []
        if len(server_list) > 0:
            bot_server_list = server_list[:21]
            if Target == "ChatBot":
                s_bot = self.getServerItemsForBot(bot_server_list)
                self.sendAlertToChatBot(s_bot)
            else:
                pass
        else:
            pass

    # send problem server list alert to chat bot
    def sendAlertToChatBot(self,rows_list):
        msg = "Send Alert Message To ChatBot"
        print(msg)
        hasError = False
        msg = "### CR on Webdb Monitor Job Found Suspected Problems,Please Check.\n"
        msg += "```\n {} \n```".format(rows_list)
        wbxchatbot().alert_msg_to_dbabot(msg)
        # wbxchatbot().alert_msg_to_dbateam(msg)
        # wbxchatbot().alert_msg_to_dbabot_by_roomId(msg, "Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")

    # use PrettyTable function to display chatbot alert
    def getServerItemsForBot(self,listdata):
        if len(listdata) == 0:
            return ""
        x = PrettyTable()
        title = ["Host Name", "Splex Port", "DB Name", "Status", "Error Message", "Collect Time"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title
        print(x)
        return str(x)

if __name__ == "__main__":
    job = CheckCRonWebDBJob()
    # job.initialize("ALL")
    job.initialize("mqdbormt020","18031")
    job.start()
