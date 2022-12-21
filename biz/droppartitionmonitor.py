import logging

import datetime
import threading
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from common.wbxssh import wbxssh
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from common.wbxexception import wbxexception
from common.wbxutil import wbxutil
import json
from common.wbxchatbot import wbxchatbot
from common.wbxinfluxdb import wbxinfluxdb
from common.wbxmonitoralertdetail import geWbxmonitoralertdetailVo


logger = logging.getLogger("dbaudit")


class droppartitionMonitorJob(WbxJob):

    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.drop_partition_monitor_job()

    def drop_partition_monitor_job(self):
        self.alert_list = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            # for host in host_list:
            #     data = self.check_telegraf_status(host)
            #     if data:
            #         self.alert_list.append(data)
            errorList = self.auditdao.getDropPartitionError()
            self.auditdao.commit()
            self.alert_drop_partition_log(errorList)
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

    def alert_drop_partition_log(self, alert_list):
        if not alert_list:
            print("nothing abnormal!")
            msg = "### Drop Partition Alert\nnothing unnormal!"
            wbxchatbot().alert_msg_to_dbabot_and_call_person(msg, "yejfeng")
            return True
        msg = "### Drop Partition Alert\n"
        title = ["db_name", "trim_host", "host_name", "status", "starttime", "endtime", "duration"]
        rst = []
        for num, item in enumerate(alert_list):
            rst.append([item["db_name"], item["trim_host"], item["host_name"], item["status"], item["starttime"], item["endtime"], item["duration"]])
            kargs = {
                "task_type": "DROP_PARTITIONS_TASK",
                "db_name": item["db_name"] or "",
                "host_name": item["host_name"] or "",
                "instance_name": "",
                "splex_port": "",
                "describe": "status: %s; start_time: %s; end_time: %s" % (item["status"], item["starttime"], item["endtime"])
            }
            wbxmonitoralertdetailVo = geWbxmonitoralertdetailVo(**kargs)
            try:
                self.auditdao.insert_wbxmonitoralertdetail(wbxmonitoralertdetailVo)
                self.auditdao.commit()
            except Exception as e:
                self.auditdao.rollback()
                print(e)
            finally:
                self.auditdao.close()
        msg += wbxchatbot().address_alert_list(title, rst)
        wbxchatbot().alert_msg_to_dbabot_and_call_person(msg, "yejfeng")


if __name__ == '__main__':

    job = droppartitionMonitorJob()
    job.initialize()
    job.start()
