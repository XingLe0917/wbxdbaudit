import re
from common.wbxinfluxdb import wbxinfluxdb
from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from dao.wbxauditdbdao import wbxauditdbdao

split_num = 100
# ccp dba team

# me
# roomid = 'Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5'

class CPUUtilizationMonitorDFW02Job(WbxJob):
    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.influx_db_obj = wbxinfluxdb()
        self.influx_db_obj._client = Config().getInfluxDB_DFW_client()[-1]
        self.main()

    def main(self):
        results = self.influx_db_obj.get_cpu_utilization_alert_data()
        alert_list = address_result_data(results)
        alert_msg(alert_list)


def alert_msg(alert_list):
    if not alert_list:
        print("Nothing nunormal!")
        return True
    alert_times = len(alert_list) // 20 + 1
    for i in range(0, alert_times):
        msg = "### %s/%s Host CPU Utilization Alert\n" % (str(i + 1), str(alert_times))
        title = ["db_name", "db_awr_metric_name", "db_awr_metric_value", "time", "datacenter", "db_inst_name", "db_type", "host"]
        msg += wbxchatbot().address_alert_list(title, alert_list[i * 20: (i + 1) * 20])
        print(msg)
        wbxchatbot().alert_msg_to_dbateam(msg)


# db_name, datacenter, db_inst_name, db_type, host, db_awr_metric_name, db_awr_metric_value, time
def address_result_data(result):
    alert_list = []
    for db_name, db_info in result.items():
        if "Host_CPU_Utilization_(%)" in db_info.keys() and "Host_CPU_Utilization_(%)_Secondly" in db_info.keys():
            Host_CPU_Utilization_var = db_info["Host_CPU_Utilization_(%)"]["value"]
            Host_CPU_Utilization_Secondly_var = db_info["Host_CPU_Utilization_(%)_Secondly"]["value"]
            if Host_CPU_Utilization_var >= 100:
                alert_list.append([db_name, "Host_CPU_Utilization_(%)", Host_CPU_Utilization_var, db_info["Host_CPU_Utilization_(%)"]["time"], db_info["datacenter"], db_info["db_inst_name"], db_info["db_type"], db_info["host"]])
            if Host_CPU_Utilization_Secondly_var >= 100:
                alert_list.append([db_name, "Host_CPU_Utilization_(%)_Secondly", Host_CPU_Utilization_Secondly_var, db_info["Host_CPU_Utilization_(%)_Secondly"]["time"], db_info["datacenter"], db_info["db_inst_name"], db_info["db_type"], db_info["host"]])
    return alert_list


if __name__ == '__main__':
    job = CPUUtilizationMonitorDFW02Job()
    job.initialize()
    job.start()
