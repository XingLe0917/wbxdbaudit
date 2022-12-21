import logging

import sys
import os

project = "wbxdbaudit"
sys.path.append(os.getcwd().split(project)[0] + project)
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from dao.vo.dbdatamonitorvo import WebDomainDataMonitorVO,MeetingDataMonitorVO
from common.wbxutil import wbxutil
import json
from common.wbxchatbot import wbxchatbot


logger = logging.getLogger("dbaudit")


class EDRErrorMonitorJob(WbxJob):

    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.edrLogMonitorJob()

    def edrLogMonitorJob(self):


        opdb = None
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getDBConnectionList("ALL", "OPDB", "PRI", "ALL", "app")
            op_db = dbvolist
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
            raise Exception(e)
        finally:
            self.auditdao.close()
        print("start...")
        error_list = []
        for db_type, dbitem in {"OPDB_%s" % op_db_item.getDBType(): op_db_item for op_db_item in op_db}.items():
            try:
                datamonitordao = dbdatamonitordao(dbitem.getConnectionurl())
                datamonitordao.connect()
                datamonitordao.startTransaction()
                EDRLog = datamonitordao.getEDRErrorLog()
                alert_EDR_log(db_type, EDRLog)
                datamonitordao.commit()
            except Exception as e:
                self.auditdao.rollback()
                print(e)
                error_list.append("%s: %s" % (db_type, str(e)))
            finally:
                self.auditdao.close()
        if error_list:
            raise Exception("\n".join(error_list))


def alert_EDR_log(db_type, EDRLog):
    if not EDRLog:
        print("%s EDR is GOOD!" % db_type)
        return True
    title=["starttime","duration(M)","pkseq","mydbschema","mydbname","mydbip","status","description"]
    alert_list = []
    for num, item in enumerate(EDRLog):
        alert_list.append([item["starttime"], item["duration(M)"], item["pkseq"], item["mydbschema"], item["mydbname"], item["mydbip"], item["status"], item["description"]])
    alert_times = len(alert_list) // 20 + 1
    i = 0
    for i in range(0, alert_times):
        msg = "### %s/%s %s EDR ERROR Alert \n" % (str(i + 1), str(alert_times), db_type)
        msg += wbxchatbot().address_alert_list(title, alert_list[i * 20: (i + 1) * 20])
        wbxchatbot().alert_msg_to_dbabot(msg + "\n<@personId:Y2lzY29zcGFyazovL3VzL1BFT1BMRS9hNzJhY2UzNy1lOTFhLTRlNjktYjIzMC1jNmE2OGY0MTJhZmM>")
        print("%s EDR Alert: %s" % (db_type, str(item)))



if __name__ == '__main__':

    job = EDRErrorMonitorJob()
    job.initialize()
    job.auditdao = wbxauditdbdao(job.depot_connectionurl)
    job.edrLogMonitorJob()
