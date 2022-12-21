from influxdb import InfluxDBClient
import logging
from datetime import datetime
import copy
from common.singleton import Singleton
from common.wbxutil import wbxutil
from common.config import Config

logger = logging.getLogger("DBAMONITOR")


@Singleton
class wbxinfluxdb:
    def __init__(self):

        self._username = "admin"
        self._pwd = b'aW5mbHV4QE5paGFv'
        self.influxdbip,self.influx_database,self._client = Config().getInfluxDB_SJC_client()

    def get_all_host(self):
        sql = "show tag values from system with key=host where datastore='ORACLE' and time > '2020-08-08'"
        data = self._client.query(sql)
        points = data.get_points()
        result = []
        for item in points:
            result.append(item["value"].split(".")[0])
        return result

    def get_cpu_utilization_alert_data(self):
        # sql = "select * from wbxdb_monitor_odm where time > now() - 5m limit 5"
        sql = "select time, datacenter, db_awr_metric_name, max(db_awr_metric_value), db_name, db_inst_name, db_type, host from wbxdb_monitor_odm where time > now() - 5m and db_awr_metric_name = 'Host_CPU_Utilization_(%)' or db_awr_metric_name = 'Host_CPU_Utilization_(%)_Secondly' group by db_name, db_awr_metric_name"
        data = self._client.query(sql)
        points = data.get_points()
        result = {}
        for item in points:
            if item["db_name"] in result.keys():
                result[item["db_name"]].update({
                    item["db_awr_metric_name"]: {
                        "value": item["max"],
                        "time": item["time"]
                    }
                })
            else:
                result.update({
                    item["db_name"]: {
                        item["db_awr_metric_name"]: {
                            "value": item["max"],
                            "time": item["time"]
                        },
                        "datacenter": item["datacenter"],
                        "db_inst_name": item["db_inst_name"],
                        "db_type": item["db_type"],
                        "host": item["host"]
                    }
                })
        return result

    def get_export_data(self, src_host, tgt_host, queue_name):
        # {'time': '2020-06-28T00:57:03Z', 'backlog': 0.0, 'db_name': None, 'delaytime': None, 'host': 'rsdboradiag002.webex.com', 'host_name': 'rsdboradiag002', 'operation': 600008.0, 'port': '19010', 'process_type': 'export', 'queuename': None, 'replication_to': 'NONE', 'src_db': None, 'src_host': 'rsdboradiag002-vip', 'src_queuename': 'Gnew_CFG2NewCV', 'status': 1.0, 'tgt_db': None, 'tgt_host': 'sjdbormt011-vip', 'tgt_queuename': 'Gnew_CFG2NewCV', 'tgt_sid': 'RACCVWEB_SPLEX'}
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'export' and " \
              "src_host = '%s-vip' and tgt_host = '%s-vip' and tgt_queuename = '%s' order by time asc" \
              % (src_host, tgt_host, queue_name)
        result = self._client.query(sql)
        return result

    def get_read_data(self, src_host, src_db):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'read' and src_host = '%s-vip' and src_db = '%s' order by time asc" % (
        src_host, src_db)
        result = self._client.query(sql)
        return result

    def get_capture_data(self, src_host, src_db):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'capture' and src_host = '%s-vip' and src_db = '%s' order by time asc" % (
            src_host, src_db)
        result = self._client.query(sql)
        return result

    def get_import_data(self, src_host, tgt_host, queue_name):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'import' and src_host = '%s-vip' and tgt_host = '%s-vip' and queuename = '%s' order by time asc" % (
            src_host, tgt_host, queue_name)
        result = self._client.query(sql)
        return result

    def get_post_data(self, src_db, tgt_db, queue_name):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'post' and src_db = '%s' and tgt_db = '%s' and queuename = '%s' order by time asc" % (
            src_db, tgt_db, queue_name)
        result = self._client.query(sql)
        return result

    def get_latest_export_data(self, src_host, tgt_host, replication_to):
        sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'export' and " \
              "src_host = '%s-vip' and tgt_host = '%s-vip' and replication_to = '%s' order by time desc limit 1" \
              % (src_host, tgt_host, replication_to)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["tgt_queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_latest_read_data(self, src_host, src_db):
        sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'read' " \
              "and src_host = '%s-vip' and src_db = '%s' order by time desc limit 1" % (
        src_host, src_db)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_latest_capture_data(self, src_host, src_db):
        sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'capture' " \
              "and src_host = '%s-vip' and src_db = '%s' order by time desc limit 1" % (
            src_host, src_db)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_latest_import_data(self, src_host, tgt_host, queue_name):
        sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'import' " \
                    "and src_host = '%s-vip' and tgt_host = '%s-vip' and queuename = '%s' order by time desc limit 1" % (
            src_host, tgt_host, queue_name)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def get_latest_post_data(self, src_db, tgt_db, queue_name):
        sql = "select * from shareplex_process where time > now() - 7d  and process_type = 'post' " \
              "and src_db = '%s' and tgt_db = '%s' and queuename = '%s' order by time desc limit 1" % (
            src_db, tgt_db, queue_name)
        result = self._client.query(sql)
        if not result:
            return {}
        points = result.get_points()
        rst_dict = {}
        for item in points:
            rst_dict["queuename"] = item["queuename"]
            rst_dict["operation"] = item["operation"]
            rst_dict["status"] = item["status"]
            rst_dict["delaytime"] = item["delaytime"]
            rst_dict["backlog"] = item["backlog"]
            rst_dict["process_type"] = item["process_type"]
        return rst_dict

    def test(self):
        # {'time': '2020-06-28T07:15:01Z', 'backlog': None, 'db_name': None, 'delaytime': None,
        #  'host': 'lndbormt027.webex.com', 'host_name': 'lndbormt027', 'operation': 600042.0, 'port': '19007',
        #  'process_type': 'import', 'queuename': 'nGCFG2NEWI', 'replication_to': None, 'src_db': None,
        #  'src_host': 'rsdboradiag002-vip', 'src_queuename': None, 'status': 1.0, 'tgt_db': None,
        #  'tgt_host': 'lndbormt027-vip', 'tgt_queuename': None, 'tgt_sid': None}
        # sql = "show tag values from WBXDBMONITOR_LISTENER_LOG with key=host"
        # sql = "delete from wbxdb_monitor_odm where db_inst_name='bdprdasj1(sjdbormt060.webex.com)' or db_inst_name='bdprdasj2(sjdbormt061.webex.com)' or db_inst_name='idprdsj1(sjdbormt060.webex.com)' or db_inst_name='idprdsj2(sjdbormt061.webex.com)'" # wbxdb_monitor_odm, wbxdb_monitor_owi
        sql = "show tag values from wbxdb_monitor_odm with key=db_inst_name where host='sjdbormt060.webex.com'"
        # sql = "delete from wbxdb_monitor_odm where host='sjdbormt060.webex.com' or host='sjdbormt061.webex.com'"
        # sql = "show tag values from osconfig with key=h
        # ost where datastore='ORACLE' and time > '2020-10-21'"
        # sql = "DROP SERIES from wbxdb_monitor_odm where db_inst_name='ttacomb61(tadbth391.webex.com)'"
        # sql = "show series from system"
        # sql = "select * from shareplex_process where time > now() - 1d  and process_type = 'export' and " \
        #       "src_host = 'sgdbormt014-vip' and tgt_host = 'jpdbormt015-vip' and port='19096' order by time desc limit 1"
        result = self._client.query(sql)
        return result


    def get_opteration_item(self, result):
        points = result.get_points()
        rst_dict = {}
        tem = 0
        for item in points:
            currtime = wbxutil.convertTZtimeToDatetime(item["time"])
            currtime = wbxutil.convertDatetimeToString(currtime)
            if item["operation"] - tem > 0:
                rst_dict[currtime] = item["operation"] - tem
            tem = item["operation"]
        return rst_dict

    def get_backlog_item(self, result):
        points = result.get_points()
        rst_dict = {}
        for item in points:
            currtime = wbxutil.convertTZtimeToDatetime(item["time"])
            currtime = wbxutil.convertDatetimeToString(currtime)
            if item["backlog"] > 0:
                rst_dict[currtime] = item["backlog"]
        return rst_dict

    def get_delaytime_item(self, result):
        points = result.get_points()
        rst_dict = {}
        for item in points:
            currtime = wbxutil.convertTZtimeToDatetime(item["time"])
            currtime = wbxutil.convertDatetimeToString(currtime)
            if item["delaytime"] > 0:
                rst_dict[currtime] = item["delaytime"]
        return rst_dict

    def get_queuename_item(self, x_result):
        points = x_result.get_points()
        rst_dict = []
        for item in points:
            rst_dict.append(item["tgt_queuename"])
        rst_dict = list(set(rst_dict))
        return rst_dict

    def insert_data_to_influxdb(self, table_name, row_data_list):
        if not row_data_list or not table_name:
            raise Exception("can not insert %s to table % in influxdb" % (row_data_list, table_name))
        json_body = []

        GMT_FORMAT = '%Y-%m-%dT%H:%M:%SZ'
        body_item = {
            "measurement": table_name,
            "tags": {},
            "time": "2009-11-10T23:00:00Z",
            "fields": {}
        }
        for row_data in row_data_list:
            body_item = copy.deepcopy(body_item)
            body_item["time"] = datetime.utcnow().strftime(GMT_FORMAT)
            for k, v in row_data.items():
                if k in ["host", "datacenter", "datastore", "db_env", "metric_type", "metric_name"]:
                    body_item["tags"].update({k: v})
                else:
                    body_item["fields"].update({k: v})
            json_body.append(body_item)
        insert_bool = self._client.write_points(json_body, database="telegraf")
        if not insert_bool:
            logging.error("insert %s failed by %s" % (str(row_data_list), str(insert_bool)))





if __name__ == '__main__':
    # influx_db_obj = wbxinfluxdb()
    # a = {"port":"24504","src_db":"RACINFRG","src_host":"tadborbf06","tgt_db":"RACINFRA","tgt_host":"sjdborbf06","replication_to":"gsb2p"}
    # # export_data = influx_db_obj.get_export_data(a["src_host"], a["tgt_host"], a["replication_to"])
    # data = influx_db_obj.test()
    # points = data.get_points()
    # for item in points:
    #     print(item)
    # print(data)
    influx_db_obj = wbxinfluxdb()
    influx_db_obj.influxdbip, influx_db_obj.influx_database, influx_db_obj._client = Config().getInfluxDB_DFW_client()
    # influx_db_obj._client = Config().getInfluxDBclient()
    a = {"port": "24504", "src_db": "RACINFRG", "src_host": "tadborbf06", "tgt_db": "RACINFRA",
         "tgt_host": "sjdborbf06", "replication_to": "gsb2p"}
    # export_data = influx_db_obj.get_export_data(a["src_host"], a["tgt_host"], a["replication_to"])
    data = influx_db_obj.test()
    points = data.get_points()
    for item in points:
        print(item)
    print(data)

# capture
# {'time': '2020-06-23T01:57:04Z', 'backlog': None, 'db_name': 'GCFGDB_SPLEX', 'delaytime': 0.0, 'host': 'rsdboradiag002.webex.com', 'host_name': 'rsdboradiag002', 'operation': 58836.0, 'port': '19086', 'process_type': 'capture', 'queuename': None, 'src_db': None, 'src_host': None, 'status': 1.0, 'tgt_db': None, 'tgt_host': None}
# export
# {'time': '2020-06-25T13:39:04Z', 'backlog': 0.0, 'db_name': None, 'delaytime': None, 'host': 'rsdboradiag002.webex.com', 'host_name': 'rsdboradiag002', 'operation': 31646.0, 'port': '17005', 'process_type': 'export', 'queuename': None, 'replication_to': 'NONE', 'src_db': None, 'src_host': 'rsdboradiag002-vip', 'src_queuename': 'GCFG2TTACOMB2', 'status': 1.0, 'tgt_db': None, 'tgt_host': 'tadbth441-vip', 'tgt_queuename': 'GCFG2TTACOMB2', 'tgt_sid': 'TTACOMB2_SPLEX'}
# post
# {'time': '2020-06-23T02:00:02Z', 'backlog': 0.0, 'db_name': None, 'delaytime': 0.0, 'host': 'tadbrpt1.webex.com', 'host_name': 'tadbrpt1', 'operation': 8399185.0, 'port': '21010', 'process_type': 'post', 'queuename': 'reftrptBign', 'src_db': 'RACOPDB_SPLEX', 'src_host': None, 'status': 1.0, 'tgt_db': 'RACTARPT_SPLEX', 'tgt_host': None}
# read
# {'time': '2020-06-23T02:00:01Z', 'backlog': 0.0, 'db_name': 'RACAMCSP_SPLEX', 'delaytime': None, 'host': 'amdbormt011', 'host_name': 'amdbormt011', 'operation': 135684.0, 'port': '19024', 'process_type': 'read', 'queuename': None, 'src_db': None, 'src_host': None, 'status': 1.0, 'tgt_db': None, 'tgt_host': None}
# import
# {'time': '2020-06-25T13:27:04Z', 'backlog': None, 'db_name': None, 'delaytime': None, 'host': 'rsdboradiag002.webex.com', 'host_name': 'rsdboradiag002', 'operation': 440.0, 'port': '24511', 'process_type': 'import', 'queuename': 'RACPTN_Nn', 'replication_to': None, 'src_db': None, 'src_host': 'sjdbormt062-vip', 'src_queuename': None, 'status': 1.0, 'tgt_db': None, 'tgt_host': 'rsdboradiag002-vip', 'tgt_queuename': None, 'tgt_sid': None}
