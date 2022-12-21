import logging
from concurrent.futures._base import as_completed
from concurrent.futures import ThreadPoolExecutor

from prettytable import PrettyTable
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")
# hour
sql_exec_time = 3
split_num = 30
'''
Desc:   query long-execution time SQL > 3 hours then send alert to dbabot
'''
class AlertForLongTimeSQL(WbxJob):
    def start(self):
        config = Config()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        # self.roomId = config.getAlertRoomId()
        #pccp
        # self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZjk1MmVkMjAtOWIyOC0xMWVhLTliMDQtODVlZDBhY2M0ZTNi"
        # me
        # self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5"
        # test to room (Test for Alert: query long-execution time SQL)
        self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vNzMxYWIxMjAtNmVmNy0xMWVkLTg3ZTYtYzU1ODZlMDU2MzMw"
        self.tns_vo ={}
        self.depotdb_url = None
        self.all_db_task = []
        self.alert_list = []
        self.error_list = []
        self.main()

    def main(self):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            tns_list = self.auditdao.getDBTnsInfo()
            self.tns_vo, self.depotdb_url = self.get_tns(tns_list)
            self.all_db_task = self.auditdao.get_all_dblink_monitor_list("")
            logger.info("db:%s" %(len(self.all_db_task)))
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

        with ThreadPoolExecutor(max_workers=10) as t:
            all_task = []
            for item in self.all_db_task:
                obj = t.submit(self.monitor_db_task, item['db_name'])
                all_task.append(obj)
            num = 0
            for future in as_completed(all_task):
                num += 1
                res = future.result()
                if num == len(self.all_db_task):
                    logger.info("All tasks finish!")
                    logger.info("alert_list:%s" %(len(self.alert_list)))
                    splitList = self.getSplitList(self.alert_list)
                    index = 0
                    #  send alert to dbabot which query long-execution time SQL > 3 hours
                    if len(self.alert_list) > 0:
                        logger.info("splitList={0}".format(len(splitList)))
                        job = wbxchatbot()
                        for split in splitList:
                            index += 1
                            content = self.getServerItemsForBot(split)
                            logger.info(content)
                            alert_title = "(%s/%s) Alert: query long-execution time SQL > %s hour" % (index, len(splitList),sql_exec_time)
                            job.sendAlertToChatBot(content, alert_title, self.roomId, "")

                    # send alert to lexing which exec monitor_db_task error
                    # if len(self.error_list)>0:
                    #     content = self.getServerItemsForError(self.error_list)
                    #     logger.info(content)
                    #     alert_title = "Error alert for job AlertForLongTimeSQL"
                    #     job.sendAlertToChatBot(content, alert_title, "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5","")


    def monitor_db_task(self, task_db_name):
        connectionurl = self.tns_vo[task_db_name]
        logger.info("============  monitor db :{0} ============ ".format(task_db_name))
        connect = None
        try:
            engine = create_engine(
                'oracle+cx_oracle://%s:%s@%s' % ("system", "sysnotallow", connectionurl),
                poolclass=NullPool, echo=False)
            connect = engine.connect()
            engine.connect().close()
            hour = int(sql_exec_time)*60*60
            SQL = '''
                select '%s' db_name,a.inst_id, a.last_call_et,
               trunc((sysdate-a.sql_exec_start)*3600*24) time_s,
               trunc((sysdate-a.sql_exec_start)*60*24) time_m,
               trunc((sysdate-a.sql_exec_start)*24) time_h,
                sysdate,a.sql_exec_start,
                   'ps -ef|grep ' || b.spid,       'kill -9 ' || b.spid,
               a.username,       
               a.sid,       b.spid,       a.sql_id,
               a.wait_class,       a.status,       a.event,       a.p1,       a.p2,
               a.blocking_instance blocl_inst,       a.blocking_session block_sessa,
               b.program,       a.logon_time,       a.module,
               a.osuser,       a.machine,       a.serial#,       a.wait_class,       a.sql_trace,
               a.sql_trace_waits,       a.state
          from gv$session a, gv$process b
         where a.paddr = b.addr and a.inst_id=b.inst_id  
         and ( a.type<>'BACKGROUND' )  
         and a.status='ACTIVE' 
         and a.last_call_et  >%s
         order by a.last_call_et desc 
            ''' %(task_db_name,hour)
            cursor = connect.execute(SQL)
            rows = cursor.fetchall()
            connect.connection.commit()
            for row in rows:
                self.alert_list.append(dict(row))
        except Exception as e:
            errormsg = '''Error msg (%s): %s''' % (task_db_name,str(e))
            error ={}
            error['db_name'] = task_db_name
            error['error_message'] = str(e)
            self.error_list.append(error)
            logger.error(errormsg)

    def get_tns(self,tns_list):
        tns_vo = {}
        for vo in tns_list:
            item  = dict(vo)
            db_name = item['db_name']
            trim_host = item['trim_host']
            listener_port = item['listener_port']
            service_name = "%s.webex.com" % item['service_name']
            # key = str(db_name + "_" + trim_host)
            key = str(db_name)
            value = '(DESCRIPTION ='
            if item['scan_ip1']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip1'], listener_port)
            if item['scan_ip2']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip2'], listener_port)
            if item['scan_ip3']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip3'], listener_port)
            value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
                value, service_name)
            tns_vo[key] = value
        depotdb_url = tns_vo['AUDITDB']
        return tns_vo,depotdb_url

    def getServerItemsForBot(self, listdata):
            if len(listdata) == 0: return ""
            x = PrettyTable()
            title = ["#","DB_NAME","SID","SQL_ID", "USERNAME","MACHINE","OSUSER","LAST_CALL_ET","SQL_EXEC_START","DURATION_TIME_S","DURATION_TIME_M","SPID","EVENT"]
            index = 1
            for data in listdata:
                line_content = [index,data["db_name"],data["sid"],data['sql_id'],data['username'],data['machine'],data['osuser'],data['last_call_et'],data['sql_exec_start'],data['time_s'],data['time_m'],data['spid'],data['event']]
                x.add_row(line_content)
                index += 1
            x.field_names = title
            return str(x)

    def getServerItemsForError(self, listdata):
            if len(listdata) == 0: return ""
            x = PrettyTable()
            title = ["#","DB_NAME","ERROR Message"]
            index = 1
            for data in listdata:
                line_content = [index,data["db_name"],data["error_message"]]
                x.add_row(line_content)
                index += 1
            x.field_names = title
            return str(x)

    def getSplitList(self, list):
        if len(list) > split_num:
            new_list = []
            last = []
            while len(list) > split_num:
                first = list[0:split_num]
                last = list[split_num:]
                new_list.append(first)
                list = last
            if len(last) > 0:
                new_list.append(last)
            return new_list
        else:
            return [list]


if __name__ == '__main__':
    job = AlertForLongTimeSQL()
    job.initialize()
    job.start()
