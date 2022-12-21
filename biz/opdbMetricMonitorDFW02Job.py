import re

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from dao.wbxauditdbdao import wbxauditdbdao

split_num = 100

class OpdbMetricMonitorDFW02Job(WbxJob):
    def start(self):
        config = Config()
        self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZjk1MmVkMjAtOWIyOC0xMWVhLTliMDQtODVlZDBhY2M0ZTNi"
        # self.roomId = config.getAlertRoomId()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.influxdbip,self.influx_database,self._client = config.getInfluxDB_DFW_client()
        self.opdb_db_host_list = {}
        self.dc_influxdb = 'DFW02'
        self.main()

    def main(self):
        sql = "select * from wbxdb_monitor_odm where time > now() - 10m "
        results = self._client.query(sql)
        points = results.get_points()
        influxdb_db_host_list={}
        for data in points:
            item = dict(data)
            if item['db_name'] == 'RACAM1MM':
                item['db_name'] = 'RACAM1MMP'
            key = item['db_name'] + "_" + str(item['db_inst_name']).lower()
            instance_name = re.match(r'(.*)_(.*)[(](.*)[)]', key).group(2)
            host_name = re.match(r'(.*)_(.*)[(](.*)[)]', key).group(3)
            if '.webex.com' not in host_name:
                host_name = host_name+".webex.com"
            # key = item['db_name'] + "_" + str(item['db_inst_name']).lower()
            key = item['db_name']+"_"+instance_name+"("+host_name+")"
            if key not in influxdb_db_host_list:
                # print(key)
                influxdb_db_host_list[key] = True
        print("influxdb total={0}".format(len(influxdb_db_host_list)))

        rows = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.getMonitorDBInfo()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()
        for row in rows:
            db_name = row[0]
            host_name = row[1]
            key2 = db_name+"_"+host_name
            if key2 not in self.opdb_db_host_list:
                # print(key2)
                self.opdb_db_host_list[key2] = True
        print("opdb total={0}".format(len(self.opdb_db_host_list)))

        data_list = []
        for k in sorted(self.opdb_db_host_list):
            # print(k)
            is_influxdb = False
            if k in influxdb_db_host_list:
                is_influxdb = True
            data1 = {}
            data1['db_name'] = str(k).split("_")[0]
            data1['db_inst_name'] = str(k).split("_")[1]
            data1['in_fluxdb'] = is_influxdb
            data1['test'] = "test"
            if not is_influxdb:
                # print(data1)
                data_list.append(data1)
        splitList = self.getSplitList(data_list)
        print("splitList={0}" .format(len(splitList)))
        job = wbxchatbot()
        index = 0
        if len(data_list)>0:
            for split in splitList:
                index += 1
                content = "### ({0}/{1})Check new influxdb-{2}({3}) databases={4} in last 10 mins." .format(index,len(splitList),self.dc_influxdb,self.influxdbip,self.influx_database) + "\n"
                content += "\tDB Name\t\t\tDB Inst Name\t\t\t\t\t\t\t\tInfluxDB Status" + "\n"
                for data in split:
                    db_name = data['db_name']
                    db_inst_name = data['db_inst_name']
                    in_fluxdb = data['in_fluxdb']
                    content += "\t" + db_name + "\t\t"
                    if len(db_name) < 8:
                        content += '\t'
                    content += db_inst_name + "\t\t\t\t"
                    if len(db_inst_name) < 20:
                        content += '\t'
                    if len(db_inst_name) < 24:
                        content += '\t'
                    if len(db_inst_name) < 28:
                        content += '\t'
                    if len(db_inst_name) < 32:
                        content += '\t'
                    content += str(in_fluxdb) + "\n"
                job.alert_msg_to_dbabot_by_roomId(msg=content,
                                                  roomId=self.roomId)

    def getSplitList(self, list):
        if len(list) > split_num:
            new_list = []
            last = []
            while len(list) > split_num:
                first = list[0:split_num]
                last = list[split_num:]
                new_list.append(first)
                list = last
            if len(last) > 0:
                new_list.append(last)
            return new_list
        else:
            return [list]


if __name__ == '__main__':
    job = OpdbMetricMonitorDFW02Job()
    job.initialize()
    job.start()