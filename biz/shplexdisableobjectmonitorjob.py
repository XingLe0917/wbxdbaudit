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


class ShplexDisableObjectMonitorJob(WbxJob):
    gserverList = []

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

            logger.info("There are total %s servers" % len(gserverList))

            # exacute use multi thread
            executor = ThreadPoolExecutor(max_workers=10)
            data = []
            i = 1
            for res1 in executor.map(self.launchDisableObjectMonitor, gserverList):
                print("in server {}: Call launchDisableObjectMonitor result is {}".format(i, res1))
                i += 1

            AllProblemServerList = []
            AllProblemServerList = auditdao.getProblemShplexDisableObjectMon()
            if len(AllProblemServerList) > 0:
                self.sendAlertMsg("All", AllProblemServerList)
                for ls in AllProblemServerList:
                    kargs = {
                        "task_type": "SP_OPO_DISABLE_OBJECT_NUM_LOG",
                        "db_name": "",
                        "host_name": ls["host_name"],
                        "splex_port": ls["port_number"],
                        "check_stat": ls["check_stat"],
                        "check_time": ls["check_time"],
                        "queue_name": ls["queue_name"],
                        "para_value": ls["para_value"],
                    }
                    wbxmonitoralertdetailVo = geWbxmonitoralertdetailVo(**kargs)
                    auditdao.insert_wbxmonitoralertdetail(wbxmonitoralertdetailVo)
            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
        finally:
            auditdao.close()
        return hasError

    # Launch Shareplex Disable Object Monitor shell scripts by connect server
    def launchDisableObjectMonitor(self, server_list):
        server = None
        hostname = server_list["host_name"]
        password = server_list["password"]
        username = server_list["user_name"]
        shplex_port = []
        shplex_port = server_list["port_list"].split(",")
        try:
            logger.info("Start to Connect Server %s..." % hostname)
            print("Start to Connect Server %s..." % hostname)
            server = wbxssh(hostname, ssh_port=22, login_user=username, login_pwd=password)
            server.connect()
            server.verifyConnection()

            if len(shplex_port) > 0:
                for port in shplex_port:
                    print(port)
                    cmd2 = "sh /u00/app/admin/dbarea/bin/splex_post_disable_object_monitor_4_one_port.sh %s" % port
                    res2, status2 = server.exec_command(cmd2)

                    # cmd3 = "cat /tmp/splex_deny_monitor_4_%s.log" % port
                    # res3, status3 = server.exec_command(cmd3)
                    print("************* server %s: port %s ****************" % (hostname, port))
                    # print("{0}".format(res3))

                    for line in res2.splitlines():
                        if line.find("WBXERROR") >= 0 or line.find("WBXWARNING") > 0:
                            logger.info(line)
                            print("Pls refer to log to check result.")

        except Exception as e:
            logger.error("error occured at host %s with password %s" % (hostname, password), exc_info=e)

        finally:
            if server is not None:
                server.close()
        return True

    # get unique hostname from all server list
    def getUniqueHostname(self, serverList):
        l1 = serverList
        l2 = []
        l2.append(l1[0])
        for dict in l1:
            # print len(l4)å
            k = 0
            for item in l2:
                # print 'item'
                if dict['src_host'] != item['src_host']:
                    k = k + 1
                    # continue
                else:
                    break
                if k == len(l2):
                    l2.append(dict)
        return l2

    # get ports by server hostname
    def getPortByHostname(self, host_name):
        port = []
        for list1 in gserverList:
            if host_name == list1["src_host"]:
                port.append(list1["shplex_port"])
                print("hostname is %s, port: %s" % (host_name, list1["shplex_port"]))

        return port

    # send message to chatbot/email
    def sendAlertMsg(self, Target, server_list):
        msg = ' Send Alert Message'
        print(msg)
        hasError = False

        # bot_server_list = []
        bot_server_list = server_list[:21]
        mail_server_list = server_list[:31]
        if Target == "All":
            s_bot = self.getServerItemsForBot(bot_server_list)
            self.sendAlertToChatBot(s_bot)
            s_mail = self.getServerItemsForMail(mail_server_list)
            self.sendAlertEmail(s_mail)
        elif Target == "ChatBot":
            s_bot = self.getServerItemsForBot(bot_server_list)
            self.sendAlertToChatBot(s_bot)
        elif Target == "Email":
            s_mail = self.getServerItemsForMail(mail_server_list)
            self.sendAlertEmail(s_mail)
        else:
            pass
        return hasError

    # send problem server list alert to chat bot
    def sendAlertToChatBot(self, rows_list):
        msg = "Send Alert Message To ChatBot"
        print(msg)

        hasError = False
        msg = "### Shareplex Disable Object Monitor Job found problems, more info please visit PCCP\n"
        msg += "```\n {} \n```".format(rows_list)
        # wbxchatbot().alert_msg_to_dbabot(msg)
        wbxchatbot().alert_msg_to_dbateam(msg)
        wbxchatbot().alert_msg_to_dbabot_by_roomId(msg, "Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")
        return hasError

    def sendAlertEmail(self, rows_list):
        msg = wbxemailmessage(emailtopic="Alert:Shareplex Disable Object Monitor Job Problems, More info:https://pccp.webex.com/#/shareplex/splex-Disable Object-user-monitor", receiver="brzhu@cisco.com",
                              emailcontent=rows_list)
        sendemail(msg)

    # use PrettyTable function to display chatbot alert
    def getServerItemsForBot(self, listdata):
        if len(listdata) == 0:
            return ""
        x = PrettyTable()
        title = ["Check Time", "Host Name", "Port Number", "Check Status", "Queue Name", "Para Value"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title
        print(x)
        return str(x)

    # use PrettyTable function to display email alert
    def getServerItemsForMail(self, listdata):
        if len(listdata) == 0:
            return ""
        x = PrettyTable()
        title = ["Check Time", "Host Name", "Port Number", "Check Status", "Queue Name", "Para Value"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title

        y = x.get_html_string()
        # print(y)
        return str(y)


if __name__ == "__main__":
    job = ShplexDisableObjectMonitorJob()
    job.initialize()
    job.start()
