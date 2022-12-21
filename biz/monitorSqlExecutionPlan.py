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
from dao.vo.sqlexecutionplanmonvo import SqlExecutionPlanMon
from common.wbxutil import wbxutil
from common.wbxchatbot import wbxchatbot


logger = logging.getLogger("DBAMONITOR")

class MonitorSqlExecutionPlanJob(WbxJob):

    def start(self):
        dblist = []
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dblist = self.auditdao.getDBConnectionList("ALL", "ALL", "ALL", "ALL", "SYSTEM")
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

            sql_multi_plan_monitor = []
            executor = ThreadPoolExecutor(max_workers=10)
            for data in executor.map(self.checkSqlExecutionPlan, dblist):
                pass
                # if len(data)>0:
                #     [all_appln_pool_problem.append(item) for item in data]
            executor.shutdown(wait=True)
            print('All tasks finished!')
            self.sendAlertMsg("ChatBot",sql_multi_plan_monitor)


    def checkSqlExecutionPlan(self,dbitem):
        datamonitordao = None
        try:
            gatherstatsvolist = []
            db_name = dbitem.getDBName()
            trim_host = dbitem.getTrimHost()
            collect_time = wbxutil.getcurrenttime()
            datamonitordao = dbdatamonitordao(dbitem.getConnectionurl())
            print("Check Sql Execution Plan of DB: %s %s" % (dbitem.getTrimHost(),dbitem.getDBName()))
            datamonitordao.connect()
            datamonitordao.startTransaction()
            daproblemlist = datamonitordao.getsqlmultiplanAlert()
            if len(daproblemlist) > 0:
                self.sendAlertMsg("ChatBot",daproblemlist)
            datamonitordao.updateexcutionplanmonitortime()
            sqlmultiplan = datamonitordao.getsqlmultiplan()
            datamonitordao.commit()

            for list in sqlmultiplan:
                sqlexecutionplanmonvo = SqlExecutionPlanMon()
                sqlexecutionplanmonvo.trim_host = trim_host
                sqlexecutionplanmonvo.db_name = list['db_name']
                sqlexecutionplanmonvo.sql_id = list['sql_id']
                sqlexecutionplanmonvo.sql_plan_id = list['sql_plan_id']
                sqlexecutionplanmonvo.create_time = list['create_time']
                sqlexecutionplanmonvo.modify_time = list['modify_time']
                sqlexecutionplanmonvo.monitor_id = list['monitor_id']
                sqlexecutionplanmonvo.executions_delta = list['executions_delta']
                sqlexecutionplanmonvo.cost_cpu = list['cost_cpu']
                sqlexecutionplanmonvo.cost_logic = list['cost_logic']
                sqlexecutionplanmonvo.cost_physical = list['cost_physical']
                sqlexecutionplanmonvo.cost_time = list['cost_time']
                sqlexecutionplanmonvo.monitor_label = list['monitor_label']
                sqlexecutionplanmonvo.monitor_time = list['monitor_time']
                sqlexecutionplanmonvo.fix_label = list['fix_label']
                sqlexecutionplanmonvo.fix_time = list['fix_time']
                sqlexecutionplanmonvo.spl_profile = list['spl_profile']
                sqlexecutionplanmonvo.best_plan = list['best_plan']
                sqlexecutionplanmonvo.cpu_increase = list['cpu_increase']
                sqlexecutionplanmonvo.logic_increase = list['logic_increase']
                sqlexecutionplanmonvo.physical_increase = list['physical_increase']
                sqlexecutionplanmonvo.time_increase = list['time_increase']
                sqlexecutionplanmonvo.best_time = list['best_time']
                sqlexecutionplanmonvo.problem_label = list['problem_label']
                sqlexecutionplanmonvo.new_lable = list['new_lable']
                sqlexecutionplanmonvo.problem_time = list['problem_time']
                gatherstatsvolist.append(sqlexecutionplanmonvo)
            self.insertsqlexecutionplan(trim_host,db_name,gatherstatsvolist)

        except Exception as e:
            datamonitordao.rollback()
            print(e)
        finally:
            if datamonitordao:
                datamonitordao.close()

    def insertsqlexecutionplan(self,trim_host,db_name,sqlexecutionplanmonvo):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.delete_sqlexecutionmon(trim_host,db_name)
            self.auditdao.insert_sqlexecutionmon(sqlexecutionplanmonvo)
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
        msg = "### (%s/%s)These sql execution multi-plan has problems.\n" %(num,total_num)
        msg += "```\n {} \n```".format(rows_list)
        # wbxchatbot().alert_msg_to_dbabot(msg)
        # wbxchatbot().alert_msg_to_dbateam(msg)
        # wbxchatbot().alert_msg_to_dbabot_by_roomId(msg, "Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")
        #send to sqlexecutionplan
        wbxchatbot().alert_msg_to_dbabot_by_roomId(msg, "Y2lzY29zcGFyazovL3VzL1JPT00vMzMxNDQ0MDAtNTcwMi0xMWVjLWEzMWMtZjU1ZDhmOWQ2ODM4")

    # use PrettyTable function to display chatbot alert
    def getServerItemsForBot(self,listdata):
        if len(listdata) == 0:
            return ""
        x = PrettyTable()
        title = ["db_name","sql_id","sql_plan_id","cost_time","time_increase","create_time","modify_time"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title
        print(x)
        return str(x)


if __name__ == "__main__":
    job = MonitorSqlExecutionPlanJob()
    job.initialize()
    job.start()
