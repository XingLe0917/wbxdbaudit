import logging
import re

import cx_Oracle
from prettytable import PrettyTable

from biz.wbxjob import WbxJob
from common.config import Config
from common.cxOracle import CXOracle
from common.wbxchatbot import wbxchatbot
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from common.wbxmonitoralertdetail import geWbxmonitoralertdetailVo
from dao.wbxauditdbdao import wbxauditdbdao

split_num = 100

logger = logging.getLogger("dbaudit")

class OpdbMetricMonitorSJC02Job(WbxJob):
    def start(self):
        config = Config()
        self.roomId = config.getAlertRoomId()
        self.email_pd = config.get_email_ch_pd_c()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.influxdbip,self.influx_database,self._client = config.getInfluxDB_SJC_client()
        self.opdb_db_host_list = {}
        self.tns_vo ={}
        self.dc_influxdb = 'SJC02'
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
                host_name = host_name + ".webex.com"
            key = item['db_name'] + "_" + instance_name + "(" + host_name + ")"
            if key not in influxdb_db_host_list:
                influxdb_db_host_list[key] = True
        print("influxdb total={0}".format(len(influxdb_db_host_list)))

        rows = []
        all_db_tns = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.getMonitorDBInfo()
            all_db_tns = self.auditdao.getDBTnsInfo()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        for row in rows:
            db_name = row[0]
            host_name = row[1]
            key2 = db_name+"_"+host_name
            if key2 not in self.opdb_db_host_list:
                self.opdb_db_host_list[key2] = True
        print("opdb total={0}".format(len(self.opdb_db_host_list)))
        print("all_db_tns total={0}".format(len(all_db_tns)))

        for vo in all_db_tns:
            item = dict(vo)
            key = str(item['db_name'] + "_" + item['trim_host'])
            value = self.db_connect_str(item['listener_port'], item['service_name'],item['scan_ip1'], item['scan_ip2'],
                                        item['scan_ip3'],item['host_ip'])
            self.tns_vo[key] = value

        data_list = []
        for k in sorted(self.opdb_db_host_list):
            is_influxdb = False
            if k in influxdb_db_host_list:
                is_influxdb = True
            data1 = {}
            data1['db_name'] = str(k).split("_")[0]
            data1['db_inst_name'] = str(k).split("_")[1]
            data1['in_fluxdb'] = is_influxdb
            data1['test'] = "test"
            if not is_influxdb:
                data_list.append(data1)
        splitList = self.getSplitList(data_list)
        print("data_list={0}".format(len(data_list)))
        index = 0
        check_db_list = []
        if len(data_list)>0:
            print("splitList={0}".format(len(splitList)))
            job = wbxchatbot()
            for split in splitList:
                index += 1
                content = self.getServerItemsForBot(split)
                alert_title = "({0}/{1})Check new influxdb-{2}({3}) databases={4} in last 10 mins." .format(index,len(splitList),self.dc_influxdb,self.influxdbip,self.influx_database) + "\n"
                job.sendAlertToChatBot(content, alert_title, self.roomId, "")

                for data in split:
                    db_name = data['db_name']
                    db_inst_name = data['db_inst_name']
                    instance_name = db_inst_name.split("(")[0]
                    host_name = str(db_inst_name.split("(")[1]).split(")")[0]
                    kargs = {
                        "task_type": "INFLUXDB_ISSUE_TASK",
                        "db_name": db_name,
                        "host_name": host_name,
                        "instance_name": instance_name,
                        "splex_port": "",
                        "describe": "test data"
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

                    trim_host_name = host_name.split(".")[0]
                    trim_host = host_name[0:len(trim_host_name) - 1]
                    tns_key = db_name + "_" + trim_host
                    if tns_key not in check_db_list:
                        print("====================")
                        print("check db status, tns_key:%s" %(tns_key))
                        tns = self.tns_vo[tns_key]
                        print(tns)
                        try:
                            conn = cx_Oracle.connect("system/sysnotallow@" + tns)
                            cursor = conn.cursor()
                            cursor.execute("select sysdate from dual")
                            row = cursor.fetchone()
                            curdate = row[0]
                            print(curdate)
                            conn.commit()
                        except Exception as e:
                            print("Database Error occurred! db_name=%s,tns=%s,%s" % (db_name,tns,e))
                            logger.error("Database Error occurred! db_name=%s,tns=%s,%s" % (db_name,tns,e))

                            emailcontent = "DB Name: %s, Host Name: %s " % (db_name, host_name)
                            msg = wbxemailmessage(emailtopic=wbxemailtype.EMAILTYPE_CHECH_DB_STATUS,
                                                  receiver=self.email_pd,
                                                  emailcontent=emailcontent)
                            sendemail(msg)
                        check_db_list.append(tns_key)
                    else:
                        logger.info("skip it tns_key:%s" %(tns_key))

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

    def getServerItemsForBot(self, listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["#","DB Name", "DB Inst Name","InfluxDB Status"]
        index = 1
        for data in listdata:
            line_content = [index,data["db_name"],data['db_inst_name'],data['in_fluxdb']]
            x.add_row(line_content)
            index += 1
        x.field_names = title
        return str(x)

    def db_connect_str(self,listener_port,service_name,scan_ip1,scan_ip2,scan_ip3,host_ip):
        service_name = "%s.webex.com" % service_name
        value = '(DESCRIPTION ='
        # if scan_ip1:
        #     value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip1, listener_port)
        # if scan_ip2:
        #     value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip2, listener_port)
        # if scan_ip3:
        #     value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, scan_ip3, listener_port)
        if host_ip:
            value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (value, host_ip, listener_port)
        value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
            value, service_name)
        return value


if __name__ == '__main__':
    job = OpdbMetricMonitorSJC02Job()
    job.initialize()
    job.start()