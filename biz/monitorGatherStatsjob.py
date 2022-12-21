import logging
import math
import re
from datetime import datetime, date, timedelta
from concurrent.futures._base import as_completed
from prettytable import PrettyTable
from concurrent.futures.thread import ThreadPoolExecutor
from biz.wbxjob import WbxJob
from common.wbxssh import wbxssh
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from dao.vo.gatherstatsjobvo import Gatherstatsjob
from common.wbxutil import wbxutil
from common.wbxchatbot import wbxchatbot


logger = logging.getLogger("DBAMONITOR")

class MonitorGatherStatsJob(WbxJob):

    def start(self):
        # yesterday = date.today() - timedelta(days=1)
        dblist = []
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dblist = self.auditdao.getDBConnectionListForGatherStats("ALL", "ALL", "ALL", "ALL", "SYSTEM")
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

            all_appln_pool_problem = []
            executor = ThreadPoolExecutor(max_workers=5)
            for data in executor.map(self.checkGatherStats, dblist):
                if len(data)>0:
                    [all_appln_pool_problem.append(item) for item in data]
            executor.shutdown(wait=True)
            print('All tasks finished!')
            self.sendAlertMsg("ChatBot",all_appln_pool_problem)


    def checkGatherStats(self,dbitem):
        datamonitordao = None
        gatherschemalist = []
        appln_shemalist = []
        applnnoschemalist = []
        try:
            gatherstatsvolist = []
            db_name = dbitem.getDBName()
            trim_host = dbitem.getTrimHost()
            collect_time = wbxutil.getcurrenttime()
            datamonitordao = dbdatamonitordao(dbitem.getConnectionurl())
            print("Check Gather Stats of DB: %s %s" % (dbitem.getTrimHost(),dbitem.getDBName()))
            datamonitordao.connect()
            datamonitordao.startTransaction()
            gatherstatslist = datamonitordao.getgatherstats()
            datamonitordao.commit()

            for list in gatherstatslist:
                a = list['job_action']
                # re2 = a.split('ownname=>')
                re2 = re.split('ownname=> |ownname => ', a)
                if re2 and len(re2) >=2:
                    re3 = re2[1].split(',')
                    schema = re3[0]
                    schema = schema.replace("'","")
                    schema = schema.replace('"','')
                    schema = schema.strip()
                    print(schema)
                    gatherschemalist.append(schema)

                    gatherstatsvo = Gatherstatsjob()
                    gatherstatsvo.db_name = db_name
                    gatherstatsvo.trim_host = trim_host
                    gatherstatsvo.collect_time = collect_time
                    gatherstatsvo.schema_name = schema
                    gatherstatsvo.job_name = list['job_name']
                    gatherstatsvo.last_start_date = list['last_start_date']
                    gatherstatsvo.last_run_duration = list['last_run_duration']
                    gatherstatsvo.job_action = list['job_action']
                    gatherstatsvo.repeat_interval = list['repeat_interval']
                    gatherstatsvolist.append(gatherstatsvo)
                else:
                    gatherstatsvo = Gatherstatsjob()
                    gatherstatsvo.db_name = db_name
                    gatherstatsvo.trim_host = trim_host
                    gatherstatsvo.collect_time = collect_time
                    gatherstatsvo.schema_name = "None"
                    gatherstatsvo.job_name = list['job_name']
                    gatherstatsvo.last_start_date = list['last_start_date']
                    gatherstatsvo.last_run_duration = list['last_run_duration']
                    gatherstatsvo.job_action = list['job_action']
                    gatherstatsvo.repeat_interval = list['repeat_interval']
                    gatherstatsvolist.append(gatherstatsvo)

            self.insertgatherstatsjob(trim_host,db_name,gatherstatsvolist)

        except Exception as e:
            datamonitordao.rollback()
            print(e)
        finally:
            if datamonitordao:
                datamonitordao.close()

        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            appln_shemalist = self.auditdao.getSchemaForGatherStats(dbitem.getTrimHost(),dbitem.getDBName())
            self.auditdao.commit()
            list1 = set(appln_shemalist).difference(set(gatherschemalist))
            if len(list1) == 0:
                print("%s %s db gather stats has all schema in appln_pool_info"% (dbitem.getTrimHost(),dbitem.getDBName()))
            else:
                print("%s %s db gather stats do not have all schema in appln_pool_info" % (dbitem.getTrimHost(),dbitem.getDBName()))
                for l1 in list1:
                    print(l1+",")
            list2 = set(gatherschemalist).difference(set(appln_shemalist))
            if len(list2) == 0:
                print("%s %s appln_pool_info has all schema in db gather stats"% (dbitem.getTrimHost(),dbitem.getDBName()))
            else:
                print("%s %s appln_pool_info do not have all schema in db gather stats" % (dbitem.getTrimHost(),dbitem.getDBName()))
                for l2 in list2:
                    print(l2+"_insert")
                    item = [dbitem.getTrimHost(),dbitem.getDBName(),l2]
                    applnnoschemalist.append(item)
                    # self.insertapplnpoolinfo(dbitem,l2)
            print("Complete ...\n")
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()
        return applnnoschemalist


    def insertapplnpoolinfo(self,dbitem,schema):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            password = self.auditdao.getDBSchemaPwdInfoByDBname(dbitem.getTrimHost(),dbitem.getDBName(),schema)
            if password != "":
                self.auditdao.insert_appln_pool_info(dbitem.getTrimHost(),dbitem.getDBName(),dbitem.getApplnSupportCode(),schema,password)
                print("Insert %s %s data to appln_pool_info, schema is: %s " % (dbitem.getTrimHost(), dbitem.getDBName(), schema))
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    def insertgatherstatsjob(self,trim_host,db_name,gatherstatsjoblistvo):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.delete_gatherstatsjob(trim_host,db_name)
            self.auditdao.insert_gatherstatsjob_all(gatherstatsjoblistvo)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    # send message to chatbot/email
    def sendAlertMsg(self,Target,server_list):
        msg = ' Send Alert Message'
        print(msg)
        hasError = False
        if len(server_list) >0 and len(server_list) <= 21:
            bot_server_list = server_list[:21]
            if Target == "ChatBot":
                s_bot = self.getServerItemsForBot(bot_server_list)
                self.sendAlertToChatBot(1,1,s_bot)
            else:
                pass
        else:
            step = 21
            t_num = math.ceil(len(server_list)/step)
            num= 0
            for i in range(0, len(server_list), step):
                divide_shplex_port = server_list[i:i + step]
                num = num + 1
                if Target == "ChatBot":
                    s_bot = self.getServerItemsForBot(divide_shplex_port)
                    self.sendAlertToChatBot(num,t_num,s_bot)
                else:
                    pass

    # send problem server list alert to chat bot
    def sendAlertToChatBot(self,num,total_num,rows_list):
        msg = "Send Alert Message To ChatBot"
        print(msg)
        hasError = False
        msg = "### (%s/%s)Appln_pool_info do not have these schema(schematype=app), but db gather stats has these schema jobs\n" %(num,total_num)
        msg += "```\n {} \n```".format(rows_list)
        # wbxchatbot().alert_msg_to_dbabot(msg)
        # wbxchatbot().alert_msg_to_dbateam(msg)
        wbxchatbot().alert_msg_to_dbabot_by_roomId(msg, "Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")

    # use PrettyTable function to display chatbot alert
    def getServerItemsForBot(self,listdata):
        if len(listdata) == 0:
            return ""
        x = PrettyTable()
        title = ["Trim Host", "DB Name", "schema_name"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title
        print(x)
        return str(x)


if __name__ == "__main__":
    job = MonitorGatherStatsJob()
    job.initialize()
    job.start()
