import re
from common.wbxinfluxdb import wbxinfluxdb
from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from concurrent.futures.thread import ThreadPoolExecutor
from dao.dbdatamonitordao import dbdatamonitordao
from dao.wbxauditdbdao import wbxauditdbdao
import cx_Oracle


class GetOracleDBSGAandPGAJob(WbxJob):
    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.main()

    def main(self):
        status = "SUCCESS"
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getDBConnectionList("""PROD','BTS_PROD""", "ALL", "ALL", "ALL", "SYSTEM")
            print(len(dbvolist))
            self.auditdao.commit()
            alertdata = []
            alldbdata = []
            executor = ThreadPoolExecutor(max_workers=50)
            for res in executor.map(self.get_oracle_sga_and_pga, dbvolist):
                alldbdata.extend(res)
            self.address_result_data(alldbdata)
        except Exception as e:
            self.auditdao.rollback()
            print(e)
            status = "FAIL"
        finally:
            self.auditdao.close()
        if status != "SUCCESS":
            exit(-1)

    def get_oracle_sga_and_pga(self, dbitem):
        rst_dict = {}
        cursor = None
        conn = None
        rst_list = []
        try:
            conn = cx_Oracle.connect(dbitem.getConnectionurl().replace(":", "/"))
            cursor = conn.cursor()
            SQL = "select inst_id, name, display_value from gv$parameter where name in ('sga_target', 'sga_max_size', 'pga_aggregate_target') order by inst_id, name"
            rows = cursor.execute(SQL).fetchall()
            if rows:
                for row in rows:
                    if "%s%s" % (dbitem.getDBName().lower(), row[0]) not in rst_dict.keys():
                        rst_dict.update({
                            "%s%s" % (dbitem.getDBName().lower(), row[0]): {
                                "db_name": dbitem.getDBName(),
                                "sga_target": "",
                                "sga_max_size": "",
                                "pga_aggregate_target": ""
                            }
                        })
                    if row[1] == "pga_aggregate_target":
                        rst_dict["%s%s" % (dbitem.getDBName().lower(), row[0])].update({"pga_aggregate_target": row[2]})
                        continue
                    if row[1] == "sga_max_size":
                        rst_dict["%s%s" % (dbitem.getDBName().lower(), row[0])].update({"sga_max_size": row[2]})
                        continue
                    if row[1] == "sga_target":
                        rst_dict["%s%s" % (dbitem.getDBName().lower(), row[0])].update({"sga_target": row[2]})
                        continue

            for instance_name, dbmetadata in rst_dict.items():
                rst_list.append({
                    "instance_name": instance_name.lower()
                })
                rst_list[-1].update(dbmetadata)
        except Exception as e:
            print(dbitem.getDBName(), e)
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        return rst_list

    def address_result_data(self, result):
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            auditdao.connect()
            auditdao.startTransaction()
            for row in result:
                print(row)
                auditdao.insert_oracle_sga_pga_info(row["instance_name"], row["db_name"], row["sga_target"], row["sga_max_size"], row["pga_aggregate_target"])
            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
            hasError = True
        finally:
            auditdao.close()


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
        wbxchatbot().alert_msg_to_dbateam(msg + "\n<@personId:Y2lzY29zcGFyazovL3VzL1BFT1BMRS9hNzJhY2UzNy1lOTFhLTRlNjktYjIzMC1jNmE2OGY0MTJhZmM>")


if __name__ == '__main__':
    job = GetOracleDBSGAandPGAJob()
    job.initialize()
    job.start()
