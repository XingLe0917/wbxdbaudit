import re
from common.wbxinfluxdb import wbxinfluxdb
from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from concurrent.futures.thread import ThreadPoolExecutor
from dao.dbdatamonitordao import dbdatamonitordao
from dao.wbxauditdbdao import wbxauditdbdao
import cx_Oracle


class WbxProcLogDataMonitor(WbxJob):
    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.main()

    def main(self):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getDBConnectionList("""PROD','BTS_PROD""", """CONFIG','WEB','OPDB','MEDIATE','TEO','TEL','LOOKUP','MMP','SYSTOOL','DIAGS','CI','CSP""", "ALL", "ALL", "SYSTEM")
            self.auditdao.commit()
            alertdata = []
            alldbdata = []
            executor = ThreadPoolExecutor(max_workers=50)
            for res in executor.map(self.check_proc_log_data, dbvolist):
                alldbdata.extend(res)
            alert_list = address_result_data(alldbdata)
            alert_msg(alert_list)
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

    def check_proc_log_data(self, dbitem):
        rst_list = []
        try:
            conn = cx_Oracle.connect(dbitem.getConnectionurl().replace(":", "/"))
            cursor = conn.cursor()
            v_SQL = """
            select procname, count(1) from {0}.wbxproclog where procname in ('CollectODMMonitorDataSecondly',
            'CollectODMMonitorDataMinutely','OutputODMMonitorData','CollectOSSMonitorData','OutputOSSMonitorData','CollectOWIMonitorData',
            'OutputOWIMonitorData','CollectSQLServiceNameMapData','CollectSQLMonitorData','OutputSQLMonitorData','CollectSessionMonitorData',
            'OutputSessionMonitorData','CollectArchivedLog','OutputArchivedLog','CollectRmanLog','OutputRmanLog','CollectOracleJobMonitorData',
            'OutputOracleJobMonitorData','CollectTableSpaceUsage','OutputTableSpaceUsage','CollectOSSTATMonitorData','OutputOSSTATMonitorData') 
            and logtime > sysdate - 10/60/24 group by procname
            """
            SQL = "select distinct owner from dba_scheduler_jobs where job_name like 'DB_MONITOR%'"
            # SQL = "select 1 from dual"
            # SQL = """
            # select distinct owner from dba_scheduler_jobs where job_name like 'DB_MONITOR%'
            # """
            rows = cursor.execute(SQL).fetchall()
            # print(dbitem.getDBName(), rows, '\n')
            for schema in rows:
                SQL = v_SQL.format(schema[0])
                ProcCount = cursor.execute(SQL).fetchall()
                # print(dbitem.getDBName(), ProcCount, '\n')
                if ProcCount:
                    rst_list.append({
                        "db_name": dbitem.getDBName(),
                        "db_type": dbitem.getDBType(),
                        "app_type": dbitem._application_type,
                        "schema": schema[0],
                        "ProcName": ProcCount[0][0],
                        "ProcCount": str(ProcCount[0][1])
                    })
            if rst_list:
                print(rst_list)
        except Exception as e:
            print(dbitem.getDBName(), e)
        finally:
            cursor.close()
            conn.close()
        return rst_list


def alert_msg(alert_list):
    if not alert_list:
        print("Nothing nunormal!")
        return True
    alert_times = len(alert_list) // 20 + 1
    for i in range(0, alert_times):
        msg = "### %s/%s DB Proc Log Alert\n" % (str(i + 1), str(alert_times))
        title = ["db_name", "db_type", "app_type", "schema", "ProcName", "ProcCount"]
        msg += wbxchatbot().address_alert_list(title, alert_list[i * 20: (i + 1) * 20])
        print(msg)
        wbxchatbot().alert_msg_to_dbateam(msg)


# db_name, datacenter, db_inst_name, db_type, host, db_awr_metric_name, db_awr_metric_value, time
def address_result_data(result):
    alert_list = []
    for item in result:
        alert_list.append([item["db_name"], item["db_type"], item["app_type"], item["schema"], item["ProcName"], item["ProcCount"]])
    return alert_list


if __name__ == '__main__':
    job = WbxProcLogDataMonitor()
    job.initialize()
    job.start()
