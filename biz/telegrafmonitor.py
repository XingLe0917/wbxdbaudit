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


logger = logging.getLogger("dbaudit")


class telegrafMonitorJob(WbxJob):

    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.telegraf_monitor_job()

    def telegraf_monitor_job(self):
        self.alert_list = threading.local()
        self.alert_list = []
        influx_db_obj = wbxinfluxdb()
        host_list = influx_db_obj.get_all_host()
        if not host_list:
            raise wbxexception("get no host from telegraf!")
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            # for host in host_list:
            #     data = self.check_telegraf_status(host)
            #     if data:
            #         self.alert_list.append(data)
            with ThreadPoolExecutor(max_workers=5) as executor:
                for data in executor.map(self.check_telegraf_status, host_list):
                    if data:
                        self.alert_list.append(data)
            self.auditdao.commit()
            alert_telegraf_log(self.alert_list)
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

    def check_telegraf_status(self, host_name):
        status_map = {
            "uninstall": "2",
            "stopped": "0",
            "running": "1",
            "unnormal": "3"
        }
        ssh_pwd = self.auditdao.getOracleUserPwdByHostname(host_name)
        if not ssh_pwd:
            print("cannot find oracle pwd on server %s" % host_name)
            return {
                "host_name": host_name,
                "status": "unlogin",
                "errormsg": "cannot find oracle pwd on server %s" % host_name
            }
        server = wbxssh(host_name, 22, "oracle", ssh_pwd)
        telegraf_obj = wbxtelegraf(server)
        can_login = telegraf_obj.can_login()
        if can_login:
            return {
                "host_name": host_name,
                "status": "unlogin",
                "errormsg": can_login
            }
        status, errortime, errorlog = telegraf_obj.check_telegraf()
        self.auditdao.inserttelegrafstatus(host_name, status_map[status], errortime, errorlog)
        if status != "running":
            return {
                "host_name": host_name,
                "status": status,
                "errormsg": errortime + ": " + errorlog
            }


class wbxtelegraf:
    def __init__(self, server):
        self.host_name = server.host_name
        self.server = server
        self.log_file = "/var/log/telegraf/telegraf.log"
        self.install_script = "/staging/gates/telegraf/install_telegraf.sh"

    def close(self):
        self.server.close()

    def can_login(self):
        try:
            self.server.connect()
        except Exception as e:
            return "cannot login the server %s with password in depot" % self.host_name
        return ""

    def check_telegraf(self):
        isexisted_flag = self.check_telegraf_installed()
        if not isexisted_flag:
            return "uninstall", "", ""
        isrunning_flag = self.check_telegraf_running()
        if not isrunning_flag:
            return "stopped", "", ""
        cmd = "date '+%Y-%m-%d %H:%M:%S'"
        checknow, checknow_bool = self.server.exec_command(cmd)
        checknow = checknow.split("\n")[0]
        checknow = wbxutil.convertStringtoDateTime(checknow)
        checktime_list = []
        for i in range(0, 5):
            delta = datetime.timedelta(minutes=i)
            checktime = checknow - delta
            checktime_list.append(checktime.strftime("%Y-%m-%dT%H:%M"))
        for checktime in checktime_list:
            cmd = "sudo cat %s | grep %s | grep E!" % (self.log_file, checktime)
            error_log, error_log_bool = self.server.exec_command(cmd)
            if error_log_bool and error_log:
                error_log.replace(" ", "").replace("\n", "")
                error_time = error_log.split(" E! ")[0].strip().replace("T", " ").split("Z")[0]
                error_content = error_log.split(" E! ")[-1].strip()
                return "unnormal", error_time, error_content
        return "running", "", ""


    def check_telegraf_installed(self):
        cmd = "sudo service telegraf status"
        is_existed, is_existed_bool = self.server.exec_command(cmd)
        if "unrecognized service" in is_existed:
            return False
        return True

    def check_telegraf_running(self):
        cmd = "sudo service telegraf status"
        is_running, is_running_bool = self.server.exec_command(cmd)
        if "OK" not in is_running:
            return False
        return True


def alert_telegraf_log(alert_list):
    msg = "### Telegraf Alert\n"
    title = ["host_name", "status", "errormsg"]
    rst = []
    for num, item in enumerate(alert_list):
        rst.append([item["host_name"], item["status"], item["errormsg"]])
    msg += wbxchatbot().address_alert_list(title, rst)
    wbxchatbot().alert_msg_to_web_metric(msg)


if __name__ == '__main__':

    job = telegrafMonitorJob()
    job.initialize()
    job.start()
