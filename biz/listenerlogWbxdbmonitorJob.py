import re

from prettytable import PrettyTable

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from common.wbxmonitoralertdetail import geWbxmonitoralertdetailVo
from dao.wbxauditdbdao import wbxauditdbdao

split_num = 100

class ListenerlogWbxdbmonitorJob(WbxJob):
    def start(self):
        config = Config()
        # self.roomId = config.getAlertRoomId()
        self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZjk1MmVkMjAtOWIyOC0xMWVhLTliMDQtODVlZDBhY2M0ZTNi"
        # self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5" #me
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.influxdbip,self.influx_database,self._client = config.getInfluxDB_SJC_client()
        self.opdb_db_host_list = {}
        self.dc_influxdb = 'SJC02'
        self.main()

    def main(self):
        sql = "select * from WBXDBMONITOR_LISTENER_LOG where time > now() - 1h "
        results = self._client.query(sql)
        points = results.get_points()
        influxdb_db_host_list={}
        for data in points:
            item = dict(data)
            if item['db_name'] == 'RACAM1MM':
                item['db_name'] = 'RACAM1MMP'
            key1 = item['db_name'] + "_" + str(item['db_inst_name']).lower()
            instance_name = re.match(r'(.*)_(.*)[(](.*)[)]', key1).group(2)
            host_name = re.match(r'(.*)_(.*)[(](.*)[)]', key1).group(3)
            if '.webex.com' not in host_name:
                host_name = host_name + ".webex.com"
            key = host_name.split(".")[0]
            if key not in influxdb_db_host_list:
                influxdb_db_host_list[key] = True
        print("influxdb total={0}".format(len(influxdb_db_host_list)))

        rows = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            # rows = self.auditdao.getListenerlogMonitorDBInfo()
            rows = self.auditdao.getListenerlogMonitorServerInfo()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        for row in rows:
            host_name = row[0]
            key3 = host_name
            if key3 not in self.opdb_db_host_list:
                # print(key2)
                self.opdb_db_host_list[key3] = True
        print("opdb total={0}".format(len(self.opdb_db_host_list)))

        data_list = []
        for k in sorted(self.opdb_db_host_list):
            # print(k)
            is_influxdb = False
            if k in influxdb_db_host_list:
                is_influxdb = True
            data1 = {}
            # data1['db_name'] = str(k).split("_")[0]
            # data1['db_inst_name'] = str(k).split("_")[1]
            data1['in_fluxdb'] = is_influxdb
            data1['host_name'] = k
            if not is_influxdb:
                # print(data1)
                data_list.append(data1)
        # splitList = self.getSplitList(data_list)
        print("data_list={0}".format(len(data_list)))

        if len(data_list)>0:
            if len(data_list) > 0:
                lists = wbxchatbot().getSplitList(data_list, 50)
                index = 1
                for new_list in lists:
                    content = self.getServerItemsForBot(new_list)
                    alert_title = "Check WBXDBMONITOR_LISTENER_LOG no data in last 1 hour (%s/%s) " % (index, len(lists))
                    wbxchatbot().sendAlertToChatBot(content, alert_title, self.roomId,"")

    def getServerItemsForBot(self, listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["#","Host Name", "InfluxDB Status"]
        index = 1
        for data in listdata:
            line_content = [index,data["host_name"],data['in_fluxdb']]
            x.add_row(line_content)
            index += 1
        x.field_names = title
        return str(x)


if __name__ == '__main__':
    job = ListenerlogWbxdbmonitorJob()
    job.initialize()
    job.start()