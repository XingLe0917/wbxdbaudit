import logging
import re
from datetime import datetime

import cx_Oracle
from prettytable import PrettyTable

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

'''
Desc:   Monitor Replication delay from mtgbt3wd(postgres) to RACBOPDB(oracle) ,if delay time more than 10 mins, then sent alert
        this replication use dcass not shareplex
'''

# delay time more than 10 mins, then sent alert
delay_second=10*60

class MonitorDelayforDcaas(WbxJob):
    def start(self):
        config = Config()
        self.roomId = config.getAlertRoomId()
        # self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5"
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.influxdbip,self.influx_database,self._client = config.getInfluxDB_SJC_client()
        self.opdb_db_host_list = {}
        self.tns_vo ={}
        self.dc_influxdb = 'SJC02'
        self.main()

    def main(self):
        sql = "select * from wbxdb_monitor_sp_delay where tgt_db_name='RACBOPDB' and src_db_name= 'mtgbt3wd' and time > now() - 2m "
        results = self._client.query(sql)
        points = results.get_points()
        listdata = []
        for data in points:
            item = dict(data)
            item['time']= datetime.strptime(item['time'], '%Y-%m-%dT%H:%M:%SZ')
            delayinsecond = item['delayinsecond']
            if delayinsecond >delay_second :
                print(item)
                listdata.append(item)
        if len(listdata)>0:
            print("alert count:%s" %(len(listdata)))
            content = self.getServerItemsForBot(listdata)
            job = wbxchatbot()
            job.sendAlertToChatBot(content, "MtgDB replication delay from PG to Oracle", self.roomId, "")
        else:
            print("no delay")

    def getServerItemsForBot(self, listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["#","Time", "Src Trim Host", "Src DB", "Tgt Host", "Tgt DB", "Delay(second)"]
        index = 1
        for data in listdata:
            line_content = [index, data["time"],data["src_trim_host"], data['src_db_name'],data['host'], data['tgt_db_name'], int(data['delayinsecond'])]
            x.add_row(line_content)
            index += 1
        x.field_names = title
        return str(x)


if __name__ == '__main__':
    job = MonitorDelayforDcaas()
    job.initialize()
    job.start()