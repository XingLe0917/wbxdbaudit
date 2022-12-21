from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from dao.vo.dbdatamonitorvo import WbxdbWaitEventMonitorVO
from common.wbxchatbot import wbxchatbot
from sqlalchemy import and_
from concurrent.futures.thread import ThreadPoolExecutor
import cx_Oracle
'''
Desc:   Check All DB Server Wait Evet
OutPut: 1 Record info Into Wbxdbwaiteventmonitor
        2 Alert ChatBot
'''
class CheckDBWaitEventJob(WbxJob):

    def start(self):
        hasError = False
        dbvolist = []
        AllSvrWaitEventList=[]
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            msg = 'Get Svr Connection Url '
            print(msg)

            auditdao.connect()
            auditdao.startTransaction()
            dbvolist = auditdao.getDBConnectionList("PROD","ALL","ALL","ALL","SYSTEM")
            # dbvolist = auditdao.getDBConnectionList("PROD","WEB","ALL","AMS01","app")
            auditdao.commit()
            executor = ThreadPoolExecutor(max_workers=50)
            msg = 'Get All Svr WaitEvent is now'
            print(msg)
            for data in executor.map(self.getAllSvrWaitEvent, dbvolist):
                if len(data)>0:
                    [AllSvrWaitEventList.append(item) for item in data]
            msg = 'Get All Svr WaitEvent is End,Reay to Persistence Into Oracle...'
            print(msg)

            self.dataPersistenceIntoOracle(auditdao, *AllSvrWaitEventList)
            msg = 'Persistence Into Oracle Is Done'
            print(msg)

            if len(AllSvrWaitEventList) > 0:
                self.sendAlertMsg("ChatBot", *AllSvrWaitEventList)
        except Exception as e:
            auditdao.rollback()
            hasError = True
            print(e)
        finally:
            auditdao.close()
        return hasError


    def getAllSvrWaitEvent(self,dbvo):
        WaitEventList=[]
        conn=cursor=None
        try:
            connectionurl = dbvo.getConnectionurl()
            db_name=dbvo.getDBName()
            dao = dbdatamonitordao(connectionurl)
            connectionurl = connectionurl.replace(':','/',2)
            conn = cx_Oracle.connect('%s' %connectionurl)
            cursor = conn.cursor()
        except Exception as e:
            msg='%s Get DB Conn Err' %(dbvo.getDBName())
            print(msg)

        if cursor is not None:
            try:
                res = dao.getDBWaitEvent(db_name,cursor)
                [WaitEventList.append({"db_name":x[0],
                                    "instance_number":x[1],
                                    "event":x[2],
                                    "osuser":x[3],
                                    "machine":x[4],
                                    "sid":x[5],
                                    "program":x[6],
                                    "username":x[7],
                                    "sql_id":x[8],
                                    "sql_exec_start":x[9],
                                    "monitor_time":x[10],
                                    "duration":x[11]
                                    }) for x in res]
                conn.commit()
            except Exception as e:
                msg = ' %s Get ExcuteTime Err: %s' %(dbvo.getDBName(),str(e))
                print(msg)
            finally:
                cursor.close()
                conn.close()
        return WaitEventList

    def dataPersistenceIntoOracle(self,db,*args):
        #Data Persistence Into Oracle
        hasError = False
        try:
            db.startTransaction()
            for item in args:
                db_name=item["db_name"]
                sid=item["sid"]
                sql_exec_start=item["sql_exec_start"]
                dbvo = db.session.query(WbxdbWaitEventMonitorVO).filter(and_(WbxdbWaitEventMonitorVO.db_name == db_name, WbxdbWaitEventMonitorVO.sid == sid,WbxdbWaitEventMonitorVO.sql_exec_start == sql_exec_start)).first()
                if dbvo is not None:
                    dbvo.event=item["event"]
                    dbvo.program=item["program"]
                    dbvo.monitor_time=item["monitor_time"]
                    dbvo.duration=item["duration"]
                else:
                    dbvo = WbxdbWaitEventMonitorVO()
                    dbvo.db_name = db_name
                    dbvo.instance_number = item["instance_number"]
                    dbvo.event = item["event"]
                    dbvo.osuser = item["osuser"]
                    dbvo.machine = item["machine"]
                    dbvo.sid = sid
                    dbvo.program = item["program"]
                    dbvo.username = item["username"]
                    dbvo.sql_id = item["sql_id"]
                    dbvo.sql_exec_start = sql_exec_start
                    dbvo.monitor_time = item["monitor_time"]
                    dbvo.duration = item["duration"]
                    db.session.add(dbvo)
            db.commit()
        except Exception as e:
            if db is not None:
                db.rollback()
            msg=' dataPersistenceIntoOracle Error:%s' %str(e)
            print(msg)
            hasError=True
        finally:
            db.close()
        return hasError

    def sendAlertMsg(self,Target,*args):
        msg = ' Send Alert Message'
        print(msg)
        hasError = False

        if Target == "All":
            pass
        elif Target == "ChatBot":
            self.sendAlertToChatBot(*args)
        elif Target == "Email":
            self.sendAlertToEmail()
        return hasError

    def sendAlertToChatBot(self,*args):
        print("Send Alert Message To ChatBot")
        hasError = False
        msg = "************** Wait Event Monitor ******************"
        msg += "\n\t db_name \t\t program \t\t event"
        for num,item in enumerate(args):
            msg += "\n\t %s \t %s \t %s" % (item["db_name"], item["program"], item["event"])
        wbxchatbot().alert_msg_to_dbateam(msg)
        return hasError

    def sendAlertToEmail(self,*args):
        hasError = False
        return hasError

if __name__ == '__main__':
    job = CheckDBWaitEventJob()
    job.initialize()
    job.start()