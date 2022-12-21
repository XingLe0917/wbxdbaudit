import base64
import configparser
import os
import json
import threading

from influxdb import InfluxDBClient

from common.wbxexception import wbxexception

class Config:
    _instance_lock = threading.Lock()

    def __init__(self):
        self.CONFIGFILE_DIR = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), "conf")
        self.loadDBAMonitorConfigFile()
        self.DEPOT_URL="depotdb_url"
        self.influxDB_ip_SJC = "influxDB_ip_SJC"
        self.DEPOT_USERNAME="depotdb_username"
        self.DEPORT_PASSWORD="depotdb_password"
        self.REDIS_SERVERS="redis-servers",
        self.InfluxDB_ip="influxDB_ip",
        self.InfluxDB_port= "influxDB_port",
        self.InfluxDB_user="influxDB_user",
        self.InfluxDB_pwd="influxDB_pwd"
        self.alertRoomId="roomId"
        self.auto_task_priority = "auto_task_priority"
        self.domain_name = self.dbconnectionDict["domain_name"]
        self.influxDB_china_pri_ip = "influxDB_china_pri_ip"
        self.influxDB_china_pri_port = "influxDB_china_pri_port"
        self.influxDB_china_pri_user = "influxDB_china_pri_user"
        self.influxDB_china_pri_pwd = "influxDB_china_pri_pwd"
        self.depotdb_china_url = "depotdb_china_url"
        self.depotdb_china_username = "depotdb_china_username"
        self.depotdb_china_password = "depotdb_china_password"

    def __new__(cls, *args, **kwargs):
        if not hasattr(Config, "_instance"):
            with Config._instance_lock:
                if not hasattr(Config, "_instance"):
                    Config._instance = object.__new__(cls)
        return Config._instance

    def loadDBAMonitorConfigFile(self):
        dbamonitor_config_file= self.getDBAMonitorConfigFile()
        with open(dbamonitor_config_file, "r") as f:
            self.dbconnectionDict = json.load(f)

    def getDBAMonitorConfigFile(self):
        dbamonitor_config_file = os.path.join(self.CONFIGFILE_DIR, "dbamonitortool_config.json")
        if not os.path.isfile(dbamonitor_config_file):
            raise wbxexception("%s does not exist" % dbamonitor_config_file)
        return dbamonitor_config_file

    def getDepotConnectionurl(self):
        return self.dbconnectionDict[self.DEPOT_USERNAME],self.dbconnectionDict[self.DEPORT_PASSWORD],self.dbconnectionDict[self.DEPOT_URL]

    def getRedisClusterConnectionInfo(self):
        # return self.dbconnectionDict[self.REDIS_SERVERS]
        return self.dbconnectionDict["redis-servers"]

    def getEmailTo(self):
        return self.dbconnectionDict["emailto"]

    def getShareplexMonitorDBList(self):
        return self.dbconnectionDict["shareplex_db_list"]

    def getShareplexMonitorInterval(self):
        return self.dbconnectionDict["shareplex_monitor_inteval_in_second"]

    def getShareplexMonitorEnv(self):
        return self.dbconnectionDict["shareplex_monitor_env"]

    def getInfluxDBclient(self):
        return InfluxDBClient(self.dbconnectionDict['influxDB_ip'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), 'telegraf')

    def getInfluxDBclientNew(self):
        database = "oraclemetric"
        return InfluxDBClient(self.dbconnectionDict['influxDB_ip_SJC'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), database)

    def getInfluxDB_SJC_client(self):
        # database = "telegraf"
        database = "oraclemetric"
        return self.dbconnectionDict['influxDB_ip_SJC'],database,InfluxDBClient(self.dbconnectionDict['influxDB_ip_SJC'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), database)

    def getInfluxDB_DFW_client(self):
        # database = "telegraf"
        database = "oraclemetric"
        return self.dbconnectionDict['influxDB_ip_DFW'],database,InfluxDBClient(self.dbconnectionDict['influxDB_ip_DFW'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), database)

    def getChinaInfluxDBclient(self):
        return InfluxDBClient(self.dbconnectionDict['influxDB_china_ip'], int(self.dbconnectionDict['influxDB_port']), self.dbconnectionDict['influxDB_user'],
                              base64.b64decode(self.dbconnectionDict['influxDB_pwd']).decode("utf-8"), 'telegraf')

    def getAlertRoomId(self):
        return self.dbconnectionDict["roomId"]

    def get_auto_task_priority(self):
        return self.dbconnectionDict["auto_task_priority"]


    def getChinaDepotConnectionurl(self):
        return self.dbconnectionDict[self.depotdb_china_username],self.dbconnectionDict[self.depotdb_china_password],self.dbconnectionDict[self.depotdb_china_url]

    def getChina_InfluxDB_client(self):
        database = "oraclemetric"
        return self.dbconnectionDict['influxDB_china_pri_ip'], database, InfluxDBClient(
            self.dbconnectionDict['influxDB_china_pri_ip'], int(self.dbconnectionDict['influxDB_china_pri_port']),
            self.dbconnectionDict['influxDB_china_pri_user'],
            self.dbconnectionDict['influxDB_china_pri_pwd'], database)

    def get_email_ch_pd_c(self):
        return self.dbconnectionDict["email_ch_pd_c"]