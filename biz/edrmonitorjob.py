import logging

import sys
import os

project = "wbxdbaudit"
sys.path.append(os.getcwd().split(project)[0] + project)
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from dao.vo.dbdatamonitorvo import WebDomainDataMonitorVO,MeetingDataMonitorVO
from common.wbxssh import wbxssh
import json
from common.wbxchatbot import wbxchatbot


logger = logging.getLogger("dbaudit")


class EDRMonitorJob(WbxJob):

    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.edrLogMonitorJob()
        self.edr_host_map = {
    "OPDB_PROD": {
        7: "edsj1edr011",
        8: "edsj1edr013",
        9: "edsj1edr012"
    },
    "OPDB_BTS_PROD": {9: "opdbbtsedr02"}
}

    def edrLogMonitorJob(self):


        opdb = None
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            dbvolist = self.auditdao.getDBConnectionList("ALL", "OPDB", "PRI", "ALL", "app")
            op_db = dbvolist
            self.edr_host_map = self.auditdao.get_edr_host_map()
            print(self.edr_host_map)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
            raise Exception(e)
        finally:
            if self.auditdao:
                self.auditdao.close()
        print("start...")
        error_list = []
        datamonitordao = None
        for db_type, dbitem in {"OPDB_%s" % op_db_item.getDBType(): op_db_item for op_db_item in op_db}.items():
            try:
                datamonitordao = dbdatamonitordao(dbitem.getConnectionurl())
                datamonitordao.connect()
                datamonitordao.startTransaction()
                EDRLog = datamonitordao.getEDRLog()
                alert_EDR_log(db_type, EDRLog)
                datamonitordao.commit()
                self.self_healing(db_type, EDRLog)
            except Exception as e:
                datamonitordao.rollback()
                print(e)
                error_list.append("%s: %s" % (db_type, str(e)))
            finally:
                if datamonitordao:
                    datamonitordao.close()
                if self.auditdao:
                    self.auditdao.close()
        if error_list:
            raise Exception("\n".join(error_list))

    def self_healing(self, db_type, EDRLog):
        if not EDRLog:
            return True
        address_dict = {}
        for item in EDRLog:
            if item["active"] not in self.edr_host_map[db_type]:
                wbxchatbot().alert_msg_to_dbateam("%s %s is not in depot record. Please check..." % (db_type, item["active"]))
                continue
            host_name = self.edr_host_map[db_type].get(item["active"])
            if host_name in address_dict.keys():
                address_dict[host_name].append(item["mydbname"])
            else:
                address_dict.update({
                    host_name: [item["mydbname"]]
                })
                print(address_dict)
        for host, mydblist in address_dict.items():
            wbxchatbot().alert_msg_to_dbabot_and_call_person("Starting to self-heal on server %s for db %s" % (host, str(mydblist)), "yejfeng")
            msg = self.healing_operation(host)
            wbxchatbot().alert_msg_to_dbabot_and_call_person(msg, "yejfeng")

    def healing_operation(self, host):
        msg = ""
        server = None
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            password = self.auditdao.getedrpasswordbyhostname(host)
            server = wbxssh(host, 22, 'edrsvr', password)
            cmd = """
            ps -ef | grep java | grep EDRMain | grep -v grep | awk '{print $2}'
            """
            res, res_bool = server.exec_command(cmd)
            print(res, res_bool)
            if not res_bool:
                raise Exception("There is no edr process in the server %s. Skiping..." % host)
            process_id = res.split("\n")[0]
            cmd = """
            sudo kill -9 %s""" % process_id
            res, res_bool = server.exec_command(cmd)
            if not res_bool:
                raise Exception("Failed to kill the edr process in the server %s. Please check." % host)
            msg = "Successfully restarted the edr process on %s." % host
        except Exception as e:
            self.auditdao.rollback()
            print(e)
            msg = str(e)
        finally:
            if self.auditdao:
                self.auditdao.close()
            if server:
                server.close()
        return msg


def alert_EDR_log(db_type, EDRLog):
    if not EDRLog:
        print("%s EDR is GOOD!" % db_type)
        return True
    title=["starttime","duration(M)","pkseq","mydbschema","mydbname","mydbip","active", "status","description"]
    alert_list = []
    for num, item in enumerate(EDRLog):
        alert_list.append([item["starttime"], item["duration(M)"], item["pkseq"], item["mydbschema"], item["mydbname"], item["mydbip"],item["active"], item["status"], item["description"]])
    alert_times = len(alert_list) // 20 + 1
    i = 0
    for i in range(0, alert_times):
        msg = "### %s/%s %s EDR HUNG Alert \n" % (str(i + 1), str(alert_times), db_type)
        msg += wbxchatbot().address_alert_list(title, alert_list[i * 20: (i + 1) * 20])
        wbxchatbot().alert_msg_to_dbabot_and_call_person(msg, "yejfeng")
        print("%s EDR Alert: %s" % (db_type, str(item)))



if __name__ == '__main__':

    job = EDRMonitorJob()
    job.initialize()
    job.auditdao = wbxauditdbdao(job.depot_connectionurl)
    job.edrLogMonitorJob()
