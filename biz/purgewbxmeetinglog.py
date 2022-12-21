from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
import pymysql
from dao.dbdatamonitordao import dbdatamonitordao
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime
from common.wbxchatbot import wbxchatbot
import time
import logging
import cx_Oracle
from concurrent.futures import as_completed
logger = logging.getLogger("meetinglog")
'''
Desc:   Purge WBX Meeting log 
'''
class WbxMeetingLogPurge(WbxJob):

    def start(self):
        hasError = False
        try:
            logger.info("purge meeting start!!!!")
            auditdao = wbxauditdbdao(self.depot_connectionurl)

            auditdao.connect()
            auditdao.startTransaction()
            dbvolist = auditdao.getDBConnectionList("ALL", "OPDB", "PRI", "ALL", "app")
            mydcList = auditdao.getRunMyDBInfo()
            auditdao.commit()

            mydbList = []
            for dbvo in dbvolist:
                dbvodbname = dbvo.getDBName()
                for mydc in mydcList:
                    dbname = mydc[0]
                    locations = mydc[1]
                    if dbvodbname == dbname:
                        logger.info("get mysqldb info from [%s] and job run on locations [%s]" % (dbname,locations))
                        connectionurl = dbvo.getConnectionurl()
                        dao = dbdatamonitordao(connectionurl)
                        dao.connect()
                        dao.startTransaction()
                        rows = dao.wbxschmysqldb(locations)
                        for row in rows:
                            row["optype"] = dbvodbname
                            mydbList.append(row)
                        dao.commit()
            alertdata = []

            logger.info("purge meeting job init....")
            auditdao.connect()
            auditdao.startTransaction()
            self.purgejobinit(auditdao,*mydbList)
            auditdao.commit()
            executor = ThreadPoolExecutor(20)
            fs = [executor.submit(self.runOnEachDBSchema, **row) for row in mydbList]
            for f in as_completed(fs):
                res = f.result()
                self.updjobstatus(**res)
                if res["status"] == "FAILED":
                    alertdata.append(res)

            if len(alertdata) > 0:
                logger.info("send alert email to chatbot")
                self.sendAlertToChatBot(*alertdata)

            logger.info("purge meeting end!!!!")
        except Exception as e:
            logger.error(str(e))
            auditdao.rollback()
            hasError = True
        finally:
            auditdao.close()
        return hasError

    def runOnEachDBSchema(self, **kargs):
        db = None
        status = "SUCCESS"
        errormsg = ""
        dnum = 50000
        drows = 0
        flag = True
        start = time.time()

        sql = "delete from wbxmeetinglog where INTFFLAG='C' {0} and TIMESTAMP < now()-interval 6 hour limit {1}"
        hostname = kargs["mydbip"]
        port = int(kargs["mydbport"])
        username = kargs["mydbuser"]
        passwd = kargs["mydbpassword"]
        dbschema = kargs["mydbschema"]
        dcname = kargs["location"]
        mydbname = kargs["mydbname"]
        optype = kargs["optype"]
        res = {"dcname": dcname, "dbname": mydbname, "hostname": hostname, "dbschema": dbschema, "status": status,
               "errormsg": errormsg}

        try:
            db = pymysql.connect(host=hostname, port=port, user=username, passwd=passwd, db=dbschema)
        except Exception as e:
            status = "FAILED"
            errormsg = str(e)

        logger.info("db [%s] on location [%s] schema start delete rows. " % (dbschema,mydbname))

        if db:
            # condsql = "and GWINTFFLAG='C'" if optype == "RACOPDB" else ""
            condsql = ""
            sql = sql.format(condsql, dnum)
            logger.info("[%s] db [%s] schema on [%s] with command [%s]..." % (mydbname, dbschema, optype, sql))
            try:
                cursor = db.cursor()
                while flag:
                    cnt = cursor.execute(sql)
                    drows = drows + cnt
                    if cnt < dnum:
                        flag = False
                    elif drows % (dnum * 10) == 0:
                        logger.info("[%s] db [%s] schema on [%s] has delete rows [%s] with command [%s]..." % (mydbname, dbschema, optype, str(drows), sql))
                logger.info("[%s] db [%s] schema on [%s] has deleted rows [%s] with command [%s]. " % (mydbname, dbschema, optype, str(drows), sql))
                sql = "OPTIMIZE table wbxmeetinglog"
                cursor.execute(sql)
            except Exception as e:
                print(e)
                status = "FAILED"
                errormsg = str(e)
            finally:
                cursor.close()
                db.close()
        end = time.time()
        Duration = (end - start)  # sec
        endtime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        res["status"] = status
        res["dnum"] = dnum
        res["errormsg"] = errormsg
        res["drows"] = drows
        res["Duration"] = Duration
        res["endtime"] = endtime
        return res

    def purgejobinit(self, auditdao, *args):
        for arg in args:
            hostname = arg["mydbip"]
            username = arg["mydbuser"]
            dbschema = arg["mydbschema"]
            dcname = arg["location"]
            mydbname = arg["mydbname"]
            kargs = {"dcname": dcname, "dbname": mydbname, "hostname": hostname, "dbschema": dbschema,
                     "username": username, "status": "RUNNING", "errormsg": "", "Duration": 0, "drows": 0,
                     }
            auditdao.InitWbxMySqlDBPurgeStatus(**kargs)

    def updjobstatus(self, **kargs):
        connectionurl = self.depot_connectionurl.replace(':', '/', 2)
        try:
            conn = cx_Oracle.connect('%s' % connectionurl)
            cursor = conn.cursor()
            vSQL = '''
                    begin
                        UPDATE wbxmysqldbpurge SET status='%s',duration=%s,drows=%s,dnum=%s,errormsg='%s',endtime=to_date('%s','yyyy-MM-dd HH24:mi:ss')
                        where mydbname='%s' and mydbschema='%s';
                        insert into wbxmysqldbpurge_his(JOBID,DCNAME,MYDBNAME,MYDBSCHEMA,MYDBIP,STATUS,DURATION,DROWS,DNUM,ERRORMSG,STARTTIME,ENDTIME)  
                        select JOBID,DCNAME,MYDBNAME,MYDBSCHEMA,MYDBIP,STATUS,DURATION,DROWS,DNUM,ERRORMSG,STARTTIME,ENDTIME 
                        from wbxmysqldbpurge where mydbname='%s' and mydbschema = '%s';
                    EXCEPTION when others then rollback ;
                    end;
                    ''' % (
                kargs["status"], kargs["Duration"], kargs["drows"], kargs["dnum"], kargs["errormsg"].replace('\'', ''),
                kargs["endtime"], kargs["dbname"], kargs["dbschema"], kargs["dbname"], kargs["dbschema"])
            cursor.execute(vSQL)
            conn.commit()
        except Exception as e:
            print(e)
        finally:
            cursor.close()
            conn.close()

    def sendAlertToChatBot(self, *args):
        msg = "Send Alert Message To ChatBot"
        print(msg)
        hasError = False
        titleList = ['location', 'dbname','mydbip', 'dbschema', 'status', 'errormsg']
        dataList = []
        msg = "### Purge MyDB Meeting Log Error \n"
        for num, item in enumerate(args):
            dataList.append([item["dcname"], item["dbname"], item["hostname"],item["dbschema"], str(item["status"]), item["errormsg"]])
        msg += wbxchatbot().address_alert_list(titleList, dataList)
        # wbxchatbot().alert_msg_to_dbateam(msg)
        wbxchatbot().alert_msg_to_dbabot_and_call_person(msg,"wentazha")
        return hasError

if __name__ == '__main__':
    job = WbxMeetingLogPurge()
    job.initialize()
    job.start()
