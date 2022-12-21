import logging.config

from biz.wbxjob import WbxJob
from common.config import Config
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from common.wbxchatbot import wbxchatbot
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from prettytable import PrettyTable
import cx_Oracle
from concurrent.futures.thread import ThreadPoolExecutor
import sys

'''
Desc:   Check AllDB and WebDB->WebDB Shareplex ExcuteTime Delay time More Than Three Mins 
OutPut: 1. Alert ChatBot
        2. Email
'''

type = ""

# send message to chatbot/email
def sendAlertMsg(type,Target,server_list):
    msg = ' Send Alert Message'
    print(msg)
    hasError = False

    # bot_server_list = []
    bot_server_list = server_list[:21]
    mail_server_list = server_list[:31]
    if Target == "All":
        s_bot = getServerItemsForBot(bot_server_list)
        sendAlertToChatBot(type,s_bot)
        s_mail = getServerItemsForMail(mail_server_list)
        sendAlertEmail(type,s_mail)
    elif Target == "ChatBot":
        s_bot = getServerItemsForBot(bot_server_list)
        sendAlertToChatBot(type,s_bot)
    elif Target == "Email":
        s_mail = getServerItemsForMail(mail_server_list)
        sendAlertEmail(type,s_mail)
    else:
        pass
    return hasError


# send problem server list alert to chat bot
def sendAlertToChatBot(type,rows_list):
    msg = "Send Alert Message To ChatBot"
    print(msg)

    hasError = False
    if type == "W2W":
        msg = "### WEBDB->WEBDB Shareplex Job Delay More Than 10 Minutes\n"
        msg += "```\n {} \n```".format(rows_list)
        wbxchatbot().alert_msg_to_dbateam(msg)
    elif type == "ALLDB":
        msg = "### All DB Shareplex Job Delay More Than 30 Minutes\n"
        msg += "```\n {} \n```".format(rows_list)
        wbxchatbot().alert_msg_to_dbabot_and_call_oncall(msg)
    elif type == "13006":
        msg = "### 13006 Shareplex Job Delay More Than 30 Minutes\n"
        msg += "```\n {} \n```".format(rows_list)
        # wbxchatbot().alert_msg_to_dbateam(msg)
        wbxchatbot().alert_msg_to_dbabot_by_roomId(msg,"Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")
    # elif type == "2205":
    #     msg = "### 2205 Shareplex Job Delay More Than 30 Minutes\n"
    #     msg += "```\n {} \n```".format(rows_list)
    #     wbxchatbot().alert_msg_to_dbateam(msg)
    else:
        pass
    # msg += "```\n {} \n```".format(rows_list)
    # wbxchatbot().alert_msg_to_dbabot(msg)
    # wbxchatbot().alert_msg_to_dbateam(msg)
    # wbxchatbot().alert_msg_to_dbabot_by_roomId(msg,"Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")
    return hasError

# use PrettyTable function to display chatbot alert
def getServerItemsForBot(listdata):
    if len(listdata) == 0: return ""
    x = PrettyTable()
    title = ["PORT", "SRC_DB", "SRC_HOST", "REPLICATION_TO", "TGT_DB", "TGT_HOST", "LAG_BY",
             "LASTREP_TIME", "MONITOR_TIME"]
    for data in listdata:
        x.add_row(data)
    x.field_names = title
    print(x)
    return str(x)


# use PrettyTable function to display email alert
def getServerItemsForMail(listdata):
    if len(listdata) == 0: return ""
    x = PrettyTable()
    title = ["PORT", "SRC_DB", "SRC_HOST", "REPLICATION_TO", "TGT_DB", "TGT_HOST", "LAG_BY",
             "LASTREP_TIME", "MONITOR_TIME"]

    for data in listdata:
        x.add_row(data)
    x.field_names = title

    y = x.get_html_string()
    print(y)
    return str(y)


def sendAlertEmail(type,rows_list):
    if type == "W2W":
        msg1 = "### WEBDB->WEBDB Shareplex Job Delay More Than 10 Minutes\n"
    else:
        msg1 = "### All DB Shareplex Job Delay More Than 30 Minutes\n"

    msg = wbxemailmessage(emailtopic=msg1,
                          receiver="cwopsdba@cisco.com",
                          emailcontent=rows_list)
    sendemail(msg)

class CheckWebdbToWebdbShplexJob(WbxJob):

    def start(self):
        hasError = False
        global type
        type = "W2W"
        dbvolist = []
        config = Config()
        self.alertRoomId = config.getAlertRoomId()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        delay_min = 10
        try:
            msg = 'Get Svr Connection Url'
            print(msg)

            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getWebdbToWebdbShplexList(delay_min)
            if len(dbvolist) > 0:
                for data in dbvolist:
                    # print(data.getDBName(),data.getClusterName(),data.getDBType(),data.getApplnSupportCode())
                    print(data)
                self.auditdao.commit()
                sendAlertMsg("W2W","ChatBot", dbvolist)

        except Exception as e:
            self.auditdao.rollback()
            print(e)
            hasError = True
        finally:
            self.auditdao.close()
        return hasError

class CheckAlldbShplexJob(WbxJob):

    def start(self):
        hasError = False
        dbvolist = []
        config = Config()
        self.alertRoomId = config.getAlertRoomId()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        delay_min = 30
        try:
            msg = 'Get All DB ShplexJob Svr Connection Url '
            print(msg)

            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getAlldbShplexList(delay_min)
            if len(dbvolist) > 0:
                for data in dbvolist:
                    # print(data.getDBName(),data.getClusterName(),data.getDBType(),data.getApplnSupportCode())
                    print(data)
                self.auditdao.commit()
                sendAlertMsg("ALLDB","All",dbvolist)

        except Exception as e:
            self.auditdao.rollback()
            print(e)
            hasError = True
        finally:
            self.auditdao.close()
        return hasError

class CheckTempShplexJob(WbxJob):
    def start(self):
        hasError = False
        config = Config()
        self.alertRoomId = config.getAlertRoomId()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        delay_min = 30
        port_temp1 = '13006'
        # port_temp2 = '2205'
        try:
            msg = 'Get temp port list'
            print(msg)
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist_temp1 = self.auditdao.getTempDelaySplexList(delay_min,port_temp1)
            # dbvolist_temp2 = self.auditdao.getTempDelaySplexList(delay_min,port_temp2)
            if len(dbvolist_temp1) > 0:
                for data in dbvolist_temp1:
                    # print(data.getDBName(),data.getClusterName(),data.getDBType(),data.getApplnSupportCode())
                    print(data)
                self.auditdao.commit()
                sendAlertMsg(port_temp1,"ChatBot",dbvolist_temp1)

            # if len(dbvolist_temp2) > 0:
            #     for data in dbvolist_temp2:
            #         print(data)
            #     self.auditdao.commit()
            #     sendAlertMsg(port_temp2,"ChatBot",dbvolist_temp2)
            else:
                print("There is no delay on temp port")
        except Exception as e:
            self.auditdao.rollback()
            print(e)
            hasError = True
        finally:
            self.auditdao.close()
        return hasError

if __name__ == '__main__':
    # job = CheckWebdbToWebdbShplexJob()
    job = CheckTempShplexJob()
    job.initialize()
    job.start()
