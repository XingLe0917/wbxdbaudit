
import logging
import re

import cx_Oracle
from prettytable import PrettyTable

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")
delay_min=30

class pgShareplexDelayAlert(WbxJob):
    def start(self):
        config = Config()
        # self.roomId = config.getAlertRoomId()
        self.roomId = 'Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5'
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.opdb_db_host_list = {}
        self.tns_vo ={}
        self.main()

    def main(self):
        rows = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            # get delay dates more than 30 mins
            rows = self.auditdao.getPGShareplexDelayAlert(delay_min)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)

        logger.info("alert count:{0}".format(len(rows)))

        data_list = []
        for row in rows:
            item = {}
            item['src_host'] = row[0]
            item['src_db'] = row[1]
            item['port'] = row[2]
            item['replication_to'] = row[3]
            item['tgt_host'] = row[4]
            item['tgt_db'] = row[5]
            item['lastreptime'] = row[6]
            item['montime'] = row[7]
            item['delay_secend'] = row[8]
            item['delay_day'] = row[9]
            item['delay_hour'] = row[10]
            item['delay_min'] = row[11]
            item['daley'] = str(row[9])+":"+""+str(row[10])+":"+str(row[11])
            data_list.append(item)

        if len(data_list)>0:
            content = self.getServerItemsForBot(data_list)
            alert_title = "Shareplex Delay for Postgres"
            job = wbxchatbot()
            job.sendAlertToChatBot(content, alert_title, self.roomId, "")


    def getServerItemsForBot(self, listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["#", "src host", "src db", "port", "replication_to", "tgt_host", "tgt_db", "daley(D:H:M)"]
        index = 1
        for data in listdata:
            line_content = [index, data["src_host"], data['src_db'], data['port'], data['replication_to'],
                            data['tgt_host'], data['tgt_db'], data['daley']]
            x.add_row(line_content)
            index += 1
        x.field_names = title
        return str(x)




if __name__ == '__main__':
    job = pgShareplexDelayAlert()
    job.initialize()
    job.start()