import logging
import os
import re
import time
import threading

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


class ShplexParamDetailJob(WbxJob):
    error = False
    success = True
    paramnameList = []

    def start(self):
        start_time = datetime.now()
        print("Now start to get params and insert data, time is %s" % (start_time))
        config = Config()
        self.check_host_name = ""
        self.check_port_number = ""
        self.email_pd = config.get_email_ch_pd_c()
        self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz"  # brook roomid
        if len(self.args) < 1:
            print("please input 'ALL' or correct hostname")
            return self.error
        if self.args[0] != "ALL":
            self.check_host_name = self.args[0]
            logger.info("check_host_name=%s" % (self.check_host_name))
            if len(self.args) > 1:
                self.check_port_number = self.args[1]
                logger.info("check_port_number=%s" % (self.check_port_number))
                self.main_single(self.check_host_name, self.check_port_number)
            else:
                self.main_single(self.check_host_name)
        else:
            self.main_all()
            ##check SP_OCT_DENIED_USERID and SP_OPO_DISABLE_OBJECT_NUM
            print("Start to Check Monitor Parameter: SP_OCT_DENIED_USERID")
            self.checkDenyUserIdNotMatch()
            print("Start to Check Monitor Parameter: SP_OPO_DISABLE_OBJECT_NUM")
            self.checkDisableObjectParam()
        print("Job completed, elapsed time is %s" % (datetime.now() - start_time))

    def main_all(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._errorchannelList = []
        gserverList = []
        kargs = {}
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            gserverList = self.auditdao.getShplexParamsServerList()
            logger.info("There are total %s servers" % len(gserverList))
            global paramnameList
            paramnameList = self.auditdao.getCustomizeParamsList()
            logger.info("There are total %s customize params" % len(paramnameList))
            self.auditdao.commit()

        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

        try:
            # Muti-thread call
            executor = ThreadPoolExecutor(max_workers=5)
            data = []
            start_time = datetime.now()
            print("Now start to insert data, time is %s" % (start_time))
            i = 1
            for res1 in executor.map(self.getMonitorParamsDetail, gserverList):
                print("in server {}: Call getMonitorParamsDetail result is {}".format(i, res1))
                i += 1
            print("Insert data completed, elapsed time is %s" % (datetime.now() - start_time))
            executor.shutdown(wait=True)

        except Exception as e:
            print(e)

    def main_single(self, host_name, port_number=None):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._errorchannelList = []
        passwd = ""
        gserverList = []
        kargs = {}
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            global paramnameList
            paramnameList = self.auditdao.getCustomizeParamsList()
            logger.info("There are total %s customize params" % len(paramnameList))

            passwd = self.auditdao.getOracleUserPwdByHostname(host_name)
            logger.info("get oracle user passwd on %s" % host_name)
            self.auditdao.commit()

            server_info = {"host_name": host_name, "password": passwd, "port_number": port_number, "port_list": None}
            self.getMonitorParamsDetail(server_info)
            logger.info("Collect single server:%s port:%s shareplex parameters completed." % (host_name, port_number))

        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

    # get all monitor params detail data
    def getMonitorParamsDetail(self, serverlist):
        res = {"status": "SUCCESS", "errormsg": "", "data": None}
        param_prefix = ["SP_ANL", "SP_CAP", "SP_OCT", "SP_DEQ",
                        "SP_CMP", "SP_OCF", "SP_COP", "SP_OSY", "SP_CPY",
                        "SP_XPT", "SP_IMP", "SP_SLG", "SP_OPO", "SP_OPX", "SP_QUE", "SP_ORD", "SP_SNMP", "SP_SYS"]
        param_dict = {
            'SP_ANL': 'analyze',
            'SP_OCT': 'capture',
            'SP_CAP': 'capture',
            'SP_DEQ': 'compare',
            'SP_CMP': 'compare',
            'SP_OCF': 'config',
            'SP_COP': 'cop',
            'SP_OSY': 'copy',
            'SP_CPY': 'copy',
            'SP_XPT': 'export',
            'SP_IMP': 'import',
            'SP_SLG': 'logging',
            'SP_OPO': 'post',
            'SP_OPX': 'post',
            'SP_QUE': 'queue',
            'SP_ORD': 'read',
            'SP_SNMP': 'SNMP',
            'SP_SYS': 'system',
        }
        server = None
        hostname = serverlist["host_name"]
        password = serverlist["password"]
        username = "oracle"
        portnumber = []

        try:
            logger.info("Start to Connect Server %s..." % hostname)
            print("Start to Connect Server %s..." % hostname)
            server = wbxssh(hostname, ssh_port=22, login_user=username, login_pwd=password)
            server.connect()
            if server.isconnected == False:
                logger.info("Server %s can't access" % hostname)
                print("Server %s can't access" % hostname)
                return self.error
            splexdetailVo_list_a = []
            shplex_port_s = []
            if serverlist["port_list"] is None:
                portnumber = serverlist["port_number"]
                if portnumber is not None and portnumber != "":
                    logger.info("*************** get parameters of hostname:%s, port: %s ***************" % (hostname, portnumber))
                    print("*************** get parameters of hostname:%s, port: %s ***************" % (hostname, portnumber))
                    splexdetailVo_list = self.getSplexParamDetail(server, param_prefix, param_dict, hostname, portnumber)
                    if len(splexdetailVo_list) > 0:
                        self.insertParamDataToDB(splexdetailVo_list, hostname, portnumber)
                        print("Insert server: %s port: %s shareplex parameters to DB successfully!" % (hostname, portnumber))
                else:
                    cmd = "ps -efa | grep sp_cop | grep -v grep | awk {'print $NF'} | egrep -v '20001|20002'"
                    res_port, res_status = server.exec_command(cmd)
                    if res_status != True:
                        res["status"] = "FAILED"
                        res["errormsg"] = "Can't find any port on server: host_name={0}".format(hostname)
                        return res
                    shplex_port_s = res_port.split('\n')
                    splexdetailVo_list_insert = []
                    logger.info("*************** get parameters of hostname:%s ***************" % (hostname))
                    print("*************** get parameters of hostname:%s ***************" % (hostname))
                    for port in shplex_port_s:
                        # print(port)
                        # logger.info("*************** get parameters of port: %s ***************" % port)
                        splexdetailVo_list = self.getSplexParamDetail(server, param_prefix, param_dict, hostname,
                                                                      port)
                        splexdetailVo_list_insert += splexdetailVo_list
                    if len(splexdetailVo_list_insert) > 0:
                        self.insertParamDataToDB(splexdetailVo_list_insert, hostname)
                        # splexdetailVo_list_insert.clear()
                        print("Insert server: %s shareplex parameters to DB successfully!" % (hostname))
                    print("Insert server: %s shareplex parameters to DB successfully! total ports: %s" % (
                        hostname, len(shplex_port_s)))
            else:
                shplex_port = serverlist["port_list"].split(",")
                if len(shplex_port) == 0:
                    logger.info("No SharePlex ports on Server %s" % hostname)
                    return self.error
                splexdetailVo_list_insert = []
                logger.info("*************** get parameters of hostname:%s ***************" % (hostname))
                print("*************** get parameters of hostname:%s ***************" % (hostname))
                for port in shplex_port:
                    # print(port)
                    logger.info("*************** get parameters of port: %s ***************" % port)
                    splexdetailVo_list = self.getSplexParamDetail(server, param_prefix, param_dict, hostname,
                                                                  port)
                    splexdetailVo_list_insert += splexdetailVo_list
                if len(splexdetailVo_list_insert) > 0:
                    self.insertParamDataToDB(splexdetailVo_list_insert, hostname)
                    # splexdetailVo_list_insert.clear()
                    print("Insert server: %s shareplex parameters to DB successfully!" % (hostname))
                    # print("Insert server: %s shareplex parameters to DB successfully! total ports: %s" % (hostname,len(shplex_port)))

        except Exception as e:
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred,parse parameters , host_name={0}, e={1}, ".format(hostname, str(e))
            return res
        finally:
            if server is not None:
                server.close()

    def getSplexParamDetail(self, server, param_prefix, param_dict, hostname, port):
        splexdetailVo_list_m = []
        param_a = "all"
        logfile = "params_%s.log" % port

        parameters_file = os.path.join("/tmp", logfile)
        if os.path.isfile(parameters_file):
            os.remove(parameters_file)

        res = self.getSplexPortParams(server, hostname, port, logfile, param_a)
        param_content = res["content"]
        if res["status"] == "FAILED":
            return self.error
        else:
            bin_path = res["bin_path"]

        cmd1 = "cat /tmp/%s |grep -B 1 'Default'|sed '/SP_/{N;s/\\n/\\t/}' |grep -v 'SP_SYS_LIC_\\|SP_OCT_ASM_SID\\|SP_ORD_LOGIN_O.ABLOOKUP_SPLEX'|grep 'SP_'|grep -v Queue|awk '{print $1,$2,$NF}'" % (
            logfile)
        content1, status1 = server.exec_command(cmd1)

        cmd2 = "cat /tmp/%s |grep -A 1 -B 1 -w '\\  Queue\\b'|grep -v '\\-\\-'|sed 'N;N;s/\\n/ /g'|awk '{print $1,$2, $NF, $(NF-3)}'" % (
            logfile)
        content2, status2 = server.exec_command(cmd2)

        content3 = ""
        status3 = False
        if len(paramnameList) > 0:
            param_l = []
            for i in range(len(paramnameList)):
                if paramnameList[i]["param_name"] not in content1:
                    param_category = paramnameList[i]["param_category"]
                    param_name = paramnameList[i]["param_name"]
                    param_l.append(param_name)
            str = '|'.join(param_l)
            cmd3 = "cat /tmp/%s |grep -E '%s' |awk '{print $1,$2}'" % (logfile, str)
            content3, status3 = server.exec_command(cmd3)
            if content3 == "" or status3 == False:
                print("Can't find the customize parsing parameter of %s %s %s" % (hostname, port, str))

        if status2 == True:
            content = '\n'.join([content1, content2])
            if status3 == True:
                content = '\n'.join([content, content3])
            contents = content.split('\n')
        else:
            if status3 == True:
                content = '\n'.join([content1, content3])
                contents = content.split('\n')
            else:
                contents = content1.split('\n')

        cmd4 = "cat /tmp/%s |grep 'SharePlex Version' | awk -F '=' '{print $2}'" % (logfile)
        content4, status4 = server.exec_command(cmd4)
        if content4 == "" or status4 == False:
            print("Can't find the SharePlex Version of %s %s" % (hostname, port))
        c4 = content4.split('-')
        version = c4[0]
        version_full = content4
        collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        kargs_version = {
            "host_name": hostname,
            "port_number": port,
            "param_category": "version",
            "param_name": "version",
            "queue_name": "None",
            "actual_value": version,
            "default_value": "None",
            "collect_time": collect_time,
            "ismodified": "N"
        }
        print("Customize: Parsing parameter of %s %s version" % (hostname, port))
        splexdetailVo_version = splexdetailVo(**kargs_version)
        splexdetailVo_list_m.append(splexdetailVo_version)
        kargs_version_full = {
            "host_name": hostname,
            "port_number": port,
            "param_category": "version",
            "param_name": "version_full",
            "queue_name": "None",
            "actual_value": version_full,
            "default_value": "None",
            "collect_time": collect_time,
            "ismodified": "N"
        }
        print("Customize: Parsing parameter of %s %s version_full" % (hostname, port))
        splexdetailVo_version_full = splexdetailVo(**kargs_version_full)
        splexdetailVo_list_m.append(splexdetailVo_version_full)

        cmd5 = "sed -nr '/Encryption/{n;p}' /tmp/%s " % (logfile)
        content5, status5 = server.exec_command(cmd5)
        if content5 == "" or status5 == False:
            print("Can't find the SharePlex Version of %s %s" % (hostname, port))
        # c5 = content5.split('-')
        # version = c5[0]
        encryption_key = content5
        # collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        kargs_version = {
            "host_name": hostname,
            "port_number": port,
            "param_category": "encryption_key",
            "param_name": "encryption_key",
            "queue_name": "None",
            "actual_value": encryption_key,
            "default_value": "None",
            "collect_time": collect_time,
            "ismodified": "Y"
        }
        print("Customize: Parsing parameter of %s %s encryption key" % (hostname, port))
        splexdetailVo_encryption_key = splexdetailVo(**kargs_version)
        splexdetailVo_list_m.append(splexdetailVo_encryption_key)

        param_list = []
        for line in contents:
            c1 = line.split(' ')
            for param in param_prefix:
                if param in c1[0]:
                    # print(param)
                    param_ctgy = param_dict.get(param)
                    collect_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    if len(c1) > 3 and c1[3] != "":
                        param_one = [hostname, port, param_ctgy, c1[0], c1[3], c1[1], c1[2], collect_time]
                        kargs = {
                            "host_name": hostname,
                            "port_number": port,
                            "param_category": param_ctgy,
                            "param_name": c1[0],
                            "queue_name": c1[3],
                            "actual_value": c1[1],
                            "default_value": c1[2],
                            "collect_time": collect_time,
                            "ismodified": "Y"
                        }
                        print("Modified: Parsing parameter of %s %s %s" % (hostname, port, c1[0]))
                    elif len(c1) == 3:
                        param_one = [hostname, port, param_ctgy, c1[0], "None", c1[1], c1[2], collect_time]
                        kargs = {
                            "host_name": hostname,
                            "port_number": port,
                            "param_category": param_ctgy,
                            "param_name": c1[0],
                            "queue_name": "None",
                            "actual_value": c1[1],
                            "default_value": c1[2],
                            "collect_time": collect_time,
                            "ismodified": "Y"
                        }
                        print("Modified: Parsing parameter of %s %s %s" % (hostname, port, c1[0]))
                    else:
                        param_one = [hostname, port, param_ctgy, c1[0], "None", c1[1], c1[1], collect_time]
                        if c1[1] == "Live" or c1[1] == "Restart":
                            kargs = {
                                "host_name": hostname,
                                "port_number": port,
                                "param_category": param_ctgy,
                                "param_name": c1[0],
                                "queue_name": "None",
                                "actual_value": "None",
                                "default_value": "None",
                                "collect_time": collect_time,
                                "ismodified": "N"
                            }
                        else:
                            kargs = {
                                "host_name": hostname,
                                "port_number": port,
                                "param_category": param_ctgy,
                                "param_name": c1[0],
                                "queue_name": "None",
                                "actual_value": c1[1],
                                "default_value": c1[1],
                                "collect_time": collect_time,
                                "ismodified": "N"
                            }
                        print("Customize: Parsing parameter of %s %s %s" % (hostname, port, c1[0]))
                    param_list.append(param_one)
                    logger.info("Try to query data on target, time={0}".format(collect_time))

                    splexdetailVo_l = splexdetailVo(**kargs)
                    splexdetailVo_list_m.append(splexdetailVo_l)
                    # self.auditdao.add_splexparamdetail(splexdetailVo_l.host_name,splexdetailVo_l.port_number,splexdetailVo_l.param_category,splexdetailVo_l.param_name,splexdetailVo_l.queue_name,splexdetailVo_l.actual_value,splexdetailVo_l.default_value,splexdetailVo_l.collect_time)
                    # self.auditdao.add_splexparamdetail1(splexdetailVo_l)
                    logger.info("insert_splexparamdetail:{0}".format(splexdetailVo_l.__dict__))

        return splexdetailVo_list_m

    def getSplexPortParams(self, server, hostname, port, logfile, param_c):
        res = {"status": "SUCCESS", "errormsg": "", "bin_path": None, "content": ""}
        cmd1 = "ps -ef | grep sp_cop | grep -v grep | grep %s | awk '{print $8}' | sed -n '1p'" % (port)
        logger.info(cmd1)
        res1, status1 = server.exec_command(cmd1)
        re1 = re.match(r'(.*).app-modules(.*)', res1)
        bin_path = ""
        if re1:
            bin_path = "%sbin" % (str(re1.groups()[0]))
            res["bin_path"] = bin_path
        if bin_path == "":
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred, find Shareplex bin Dir, port={0}, host_name={1}".format(port,
                                                                                                       hostname)
            return res
        cmd = """
source %s/.profile_%s;cd %s;
./sp_ctrl << EOF | tee /tmp/%s
list param %s
version full
show encryption key
EOF
""" % (bin_path, port, bin_path, logfile, param_c)
        content, status = server.exec_command(cmd)
        if status == False:
            res["status"] = "FAILED"
            res["errormsg"] = "Error occurred, can't get params using sp_ctrl, port={0}, host_name={1}".format(port,
                                                                                                               hostname)
            return res
        else:
            res["content"] = content
        logger.info("list param %s:" % param_c)
        logger.info(content)
        return res

    def insertParamDataToDB(self, server_params_list, host_name, splex_port=None):
        # self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.delete_splexparamdetail(host_name, splex_port)
            self.auditdao.commit()
            self.auditdao.add_splexparamdetail(server_params_list)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    # check SP_OPO_DISABLE_OBJECT_NUM, if modified, send alert
    def checkDisableObjectParam(self):
        # self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            res_list = self.auditdao.getSplexParamsList("", "", "Y", "SP_OPO_DISABLE_OBJECT_NUM")
            if res_list['count'] > 0:
                print("Found SP_OPO_DISABLE_OBJECT_NUM have set value")
                self.sendAlertMsg("ChatBot", "SP_OPO_DISABLE_OBJECT_NUM", res_list['paramslist'])
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    # check SP_OCT_DENIED_USERID, if modified, send alert

    def checkDenyUserIdNotMatch(self):
        # self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            res_list = self.auditdao.getDenyUserIdParamsNotMatch()
            not_match_list = []
            for rl in res_list:
                temp_list = [rl[0], rl[1], rl[2], rl[3], rl[4], rl[5], rl[6], rl[7]]
                not_match_list.append(temp_list)
            if len(not_match_list) > 0:
                print("Found SP_OCT_DENIED_USERID actual value not match DB UserId")
                self.sendAlertMsg("All", "SP_OCT_DENIED_USERID", not_match_list)
            else:
                print("All SP_OCT_DENIED_USERID actual value matched DB UserId")
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    # send message to chatbot/email
    def sendAlertMsg(self, Target, paramname, server_list):
        msg = ' Send Alert Message'
        print(msg)
        hasError = False

        # bot_server_list = []
        bot_server_list = server_list[:21]
        mail_server_list = server_list[:31]
        if Target == "All":
            s_bot = self.getServerItemsForBot(paramname, bot_server_list)
            self.sendAlertToChatBot(paramname, s_bot)
            s_mail = self.getServerItemsForMail(paramname, mail_server_list)
            self.sendAlertEmail(paramname, s_mail)
        elif Target == "ChatBot":
            s_bot = self.getServerItemsForBot(paramname, bot_server_list)
            self.sendAlertToChatBot(paramname, s_bot)
        elif Target == "Email":
            s_mail = self.getServerItemsForMail(paramname, mail_server_list)
            self.sendAlertEmail(paramname, s_mail)
        else:
            pass
        return hasError

    # send problem server list alert to chat bot
    def sendAlertToChatBot(self, paramname, rows_list):
        msg = "Send Alert Message To ChatBot"
        print(msg)

        hasError = False
        if paramname == "SP_OCT_DENIED_USERID":
            msg = "### SP_OCT_DENIED_USERID Monitor Job Found Problems,More Info Please Visit PCCP.\n"
            msg += "```\n {} \n```".format(rows_list)
        elif paramname == "SP_OPO_DISABLE_OBJECT_NUM":
            msg = "### SP_OPO_DISABLE_OBJECT_NUM Monitor Job Found Problems,More Info Please Visit PCCP\n"
            msg += "```\n {} \n```".format(rows_list)
        # wbxchatbot().alert_msg_to_dbabot(msg)
        wbxchatbot().alert_msg_to_dbabot_and_call_oncall(msg)
        # wbxchatbot().alert_msg_to_dbateam(msg)
        # wbxchatbot().alert_msg_to_dbabot_by_roomId(msg, "Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")
        return hasError

    def sendAlertEmail(self, paramname, rows_list):
        msg = ""
        if paramname == "SP_OCT_DENIED_USERID":
            msg = wbxemailmessage(
                emailtopic="Alert:SP_OCT_DENIED_USERID Monitor Job Found Problems,More Info Please Visit PCCP",
                receiver=self.email_pd,
                emailcontent=rows_list)
        elif paramname == "SP_OPO_DISABLE_OBJECT_NUM":
            msg = wbxemailmessage(
                emailtopic="Alert:SP_OPO_DISABLE_OBJECT_NUM Monitor Job Found Problems,More Info Please Visit PCCP",
                receiver=self.email_pd,
                emailcontent=rows_list)
        sendemail(msg)

    # use PrettyTable function to display chatbot alert
    def getServerItemsForBot(self, paramname, listdata):
        if len(listdata) == 0:
            return ""
        x = PrettyTable()
        title = []
        if paramname == "SP_OCT_DENIED_USERID":
            title = ["Host Name", "Param Name", "Port Number", "Actual Value", "UserDBID Value", "DB Name", "Schema Name", "Collect Time"]
        elif paramname == "SP_OPO_DISABLE_OBJECT_NUM":
            title = ["Host Name", "Param Name", "Port Number", "Queue Name", "Param Category", "Default Value", "Actual Value", "Collect Time"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title
        print(x)
        return str(x)

    # use PrettyTable function to display email alert
    def getServerItemsForMail(self, paramname, listdata):
        if len(listdata) == 0:
            return ""
        x = PrettyTable()
        title = []
        if paramname == "SP_OCT_DENIED_USERID":
            title = ["Host Name", "Param Name", "Port Number", "Actual Value", "UserDBID Value", "DB Name", "Schema Name", "Collect Time"]
        elif paramname == "SP_OPO_DISABLE_OBJECT_NUM":
            title = ["Host Name", "Param Name", "Port Number", "Queue Name", "Param Category", "Default Value", "Actual Value", "Collect Time"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title

        y = x.get_html_string()
        # print(y)
        return str(y)

    def test(self):
        self.checkDenyUserIdNotMatch()


if __name__ == "__main__":
    job = ShplexParamDetailJob()
    job.initialize("ALL")
    # job.test()
    # job.initialize("amdbormt026","19014")
    job.start()
