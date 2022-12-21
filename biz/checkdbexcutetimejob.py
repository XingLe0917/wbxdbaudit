import logging.config

from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from common.wbxchatbot import wbxchatbot
import cx_Oracle
from concurrent.futures.thread import ThreadPoolExecutor
import sys

'''
Desc:   Check All DB Server ExcuteTime More Than One Hour
OutPut: 1  Alert ChatBot
'''
class CheckDBExcuteTimeJob(WbxJob):

    def start(self):
        hasError = False
        dbvolist = []
        allSvrExcuteTimeList = []
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            msg = 'Get Svr Connection Url '
            print(msg)

            auditdao.connect()
            auditdao.startTransaction()
            dbvolist = auditdao.getDBConnectionList("PROD","ALL","ALL","ALL","SYSTEM")
            # dbvolist = auditdao.getDBConnectionList("PROD","WEB","ALL","AMS01","app")
            auditdao.commit()

            msg = 'Get All DB Svr ExcuteSQL Now'
            print(msg)

            executor = ThreadPoolExecutor(max_workers=50)
            for data in executor.map(self.getSvrExceuteTime, dbvolist):
                if len(data)>0:
                    [allSvrExcuteTimeList.append(item) for item in data]

            msg = 'Get All DB Svr ExcuteSQL End'
            print(msg)

#            if len(allSvrExcuteTimeList) > 0 :
#                self.sendAlertMsg("ChatBot",*allSvrExcuteTimeList)
        except Exception as e:
            auditdao.rollback()
            print(e)
            hasError = True
        finally:
            auditdao.close()
        return hasError

    def getSvrExceuteTime(self,dbvo):
        SvrExceutetList = []
        conn = cursor = None
        try:
            connectionurl = dbvo.getConnectionurl()
            db_name = dbvo.getDBName()

            dao = dbdatamonitordao(connectionurl)
            connectionurl = connectionurl.replace(':', '/', 2)
            conn = cx_Oracle.connect('%s' % connectionurl)
            cursor = conn.cursor()
        except Exception as e:
            msg = '%s Get DB Conn Err:%s' % (dbvo.getDBName(),str(e))
            print(msg)

        if cursor is not None:
            try:
                res = dao.getDBExcuteTimeOneHourMore(cursor, db_name)
                print("[%s] is Done with result [%s] " % (db_name, str(len(res))))
                [SvrExceutetList.append({"db_name":x[0],
                                        "sid":x[1],
                                        "sql_id":x[2],
                                        "username":x[3],
                                        "machine":x[4],
                                        "osuser":x[5],
                                        "duration":x[6],
                                        "sql_text":x[7],
                                        "monitortime":x[8]
                                    }) for x in res]
                conn.commit()
            except Exception as e:
                print(e)
            finally:
                cursor.close()
                conn.close()
        return SvrExceutetList

    def sendAlertMsg(self,Target,*args):
        hasError = False
        if Target == "All":
            pass
        elif Target == "ChatBot":
            self.sendAlertToChatBot(*args)
        return hasError

    def sendAlertToChatBot(self,*args):
        msg="Send Alert Message To ChatBot"
        print(msg)


        hasError = False
        msg = "**************Excute Time More Than One Hour ******************"
        msg += "\n\t db_name \t\t sid \t\t sql_id \t\t username \t\t machine \t\t osuser \t\t duration(min) \t\t sql_text"
        for num,item in enumerate(args):
            msg += "\n\t %s \t\t %s  \t\t %s \t\t %s \t\t %s \t\t %s  \t\t %s \t\t %s" %(item["db_name"], item["sid"], str(item["sql_id"]), item["username"],
                         item["machine"], item["osuser"], str(item["duration"]), item["sql_text"].replace('\n','').replace('\r',''))
        # print(msg)
        #wbxchatbot().alert_msg(msg)
        # wbxchatbot().alert_msg_to_dbabot(msg)
        job = wbxchatbot()
        job.sendAlertToChatBot(msg, "Excute Time More Than One Hour", "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5", "")
        return hasError

    def sendAlertToEmail(self,*args):
        hasError = False
        return hasError

if __name__ == '__main__':
    job = CheckDBExcuteTimeJob()
    job.initialize()
    job.start()
