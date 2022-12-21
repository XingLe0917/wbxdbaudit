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
Desc:   Check ConfigDB->WebDB Shareplex ExcuteTime Delay time More Than Three Mins 
OutPut: 1. Alert ChatBot
        2. Email
'''
class CheckConfigdbToWebdbShplexJob(WbxJob):

    def start(self):
        hasError = False
        dbvolist = []
        config = Config()
        self.alertRoomId = config.getAlertRoomId()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        delay_min = 3
        try:
            msg = 'Get Svr Connection Url '
            print(msg)

            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getConfigdbToWebdbShplexList(delay_min)
            if len(dbvolist) > 0:
                for data in dbvolist:
                    # print(data.getDBName(),data.getClusterName(),data.getDBType(),data.getApplnSupportCode())
                    print(data)
                self.auditdao.commit()
                self.sendAlertMsg("ChatBot", dbvolist)

        except Exception as e:
            self.auditdao.rollback()
            print(e)
            hasError = True
        finally:
            self.auditdao.close()
        return hasError

    #send message to chatbot/email
    def sendAlertMsg(self,Target,server_list):
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

    #send problem server list alert to chat bot
    def sendAlertToChatBot(self, rows_list):
        msg = "Send Alert Message To ChatBot"
        print(msg)

        hasError = False
        msg = "### CONFIGDB->WEBDB Shareplex Job Delay More Than Three Minutes\n"
        msg += "```\n {} \n```".format(rows_list)
        # wbxchatbot().alert_msg_to_dbabot(msg)
        wbxchatbot().alert_msg_to_dbateam(msg)
        # wbxchatbot().alert_msg_to_dbabot_by_roomId(msg,"Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")
        return hasError

    #use PrettyTable function to display chatbot alert
    def getServerItemsForBot(self,listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["PORT", "SRC_DB", "SRC_HOST", "REPLICATION_TO", "TGT_DB", "TGT_HOST", "LAG_BY",
                 "LASTREP_TIME","MONITOR_TIME"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title
        print(x)
        return str(x)

    #use PrettyTable function to display email alert
    def getServerItemsForMail(self,listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["PORT", "SRC_DB", "SRC_HOST", "REPLICATION_TO", "TGT_DB", "TGT_HOST", "LAG_BY",
                 "LASTREP_TIME","MONITOR_TIME"]

        for data in listdata:
            x.add_row(data)
        x.field_names = title

        y = x.get_html_string()
        print(y)
        return str(y)

    def sendAlertEmail(self, rows_list):
        msg = wbxemailmessage(emailtopic="Alert:CONFIGDB->WEBDB Shareplex Job Delay More Than Three Minutes, Please Check.", receiver="brzhu@cisco.com",
                               emailcontent=rows_list)
        sendemail(msg)

if __name__ == '__main__':
    job = CheckConfigdbToWebdbShplexJob()
    job.initialize()
    job.start()