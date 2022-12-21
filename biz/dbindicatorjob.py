import re
from common.wbxinfluxdb import wbxinfluxdb
from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from concurrent.futures.thread import ThreadPoolExecutor
from dao.dbdatamonitordao import dbdatamonitordao
from dao.wbxauditdbdao import wbxauditdbdao
import cx_Oracle


class WbxDBIndicatorJob(WbxJob):
    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.main()

    def main(self):
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getDBConnectionList("""PROD','BTS_PROD""", "ALL", "ALL", "ALL", "SYSTEM")

            alertdata = []
            alldbdata = []
            executor = ThreadPoolExecutor(max_workers=50)
            for res in executor.map(self.get_indactor_data, dbvolist):
                alldbdata.extend(res)
            # alert_list = address_result_data(alldbdata)
            print("there is %s datas" % len(alldbdata))
            for item in alldbdata:
                self.auditdao.insert_data_to_wbxdbiundicator(item)
                self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

    def get_indactor_data(self, dbitem):
        rst_list = []
        error_list = []
        cursor = None
        conn = None
        try:
            conn = cx_Oracle.connect(dbitem.getConnectionurl().replace(":", "/"))
            cursor = conn.cursor()
            SQL = """
select version,host_name, instance_name,
date_time,
dbms_flashback.get_system_change_number current_scn,
indicator
from (select version, substr(host_name, 1,instr(host_name,'.')-1) as host_name, instance_name,
to_char(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') DATE_TIME,
((((((to_number(to_char(sysdate, 'YYYY')) - 1988) * 12 * 31 * 24 * 60 * 60) +
((to_number(to_char(sysdate, 'MM')) - 1) * 31 * 24 * 60 * 60) +
(((to_number(to_char(sysdate, 'DD')) - 1)) * 24 * 60 * 60) +
(to_number(to_char(sysdate, 'HH24')) * 60 * 60) +
(to_number(to_char(sysdate, 'MI')) * 60) +
(to_number(to_char(sysdate, 'SS')))) * (16 * 1024)) -
dbms_flashback.get_system_change_number) /
(16 * 1024 * 60 * 60 * 24)) indicator
from v$instance)
            """
            row = cursor.execute(SQL).fetchone()
            print(dbitem.getDBName(), row, '\n')
            rst_list.append({
                "version": row[0],
                "host_name": row[1],
                "instance_name": row[2],
                "date_time": row[3],
                "current_scn": row[4],
                "indicator": row[5]
            })
        except Exception as e:
            print(dbitem.getDBName(), e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        return rst_list


if __name__ == '__main__':
    job = WbxDBIndicatorJob()
    job.initialize()
    job.start()
