
import logging
import re

import cx_Oracle
from prettytable import PrettyTable

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from common.wbxmail import wbxemailmessage, sendemail
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

'''
Desc: send all postgres alert job according to the configured alert rules 
'''
class pgSendAlert(WbxJob):
    def start(self):
        config = Config()
        self.config = config
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.depotdb_url = self.config.dbconnectionDict["depotdb_url"]
        self.depotdb_username = self.config.dbconnectionDict["depotdb_username"]
        self.depotdb_password = self.config.dbconnectionDict["depotdb_password"]
        self.opdb_db_host_list = {}
        self.tns_vo ={}
        self.main()

    def main(self):
        alers_rows = []
        all_alert_rulus = []
        alertsUpdateStuaus = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            # Get the latest 5 minute data which status is NEW
            alers_rows = self.auditdao.getPGAlertlist(5)
            all_alert_rulus = self.auditdao.getPGAlertRule()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)

        rule_map = {}
        for rule in all_alert_rulus:
            item = dict(rule)
            key = item['alert_title']
            if key not in rule_map:
                rule_map[key] = []
                rule_map[key].append(rule)
            else:
                rule_map[key].append(rule)
        logger.info("rule_map:%s" % (len(rule_map)))
        alert_by_room = []
        alert_by_room_key = []
        alert_by_email = []
        alert_by_email_key= []
        alert_by_pd = []
        alert_by_pd_key = []
        logger.info("alers_rows:%s" %(len(alers_rows)))
        for row in alers_rows:
            item = dict(row)
            alert_type = row[0]
            alertid = row[1]
            alerttitle = row[2]
            alertsUpdateStuaus.append(("SENT",str(alertid)))
            rules = []
            if alerttitle in rule_map.keys():
                rules += rule_map[alerttitle]
            if alert_type in rule_map.keys():
                rules += rule_map[alert_type]
            item['rules'] = rules
            for rule in rules:
                new_item = {}
                new_item['alert_type'] = item['alert_type']
                new_item['alertid'] = item['alertid']
                new_item['alerttitle'] = item['alerttitle']
                new_item['host_name'] = item['host_name']
                new_item['db_name'] = item['db_name']
                new_item['splex_port'] = item['splex_port']
                new_item['createtime'] = item['createtime']
                new_item['parameter'] = item['parameter']
                alert_channel_type = dict(rule)['alert_channel_type']
                alert_channel_value = dict(rule)['alert_channel_value']
                new_item['alert_channel_type'] = alert_channel_type
                new_item['alert_channel_value'] = alert_channel_value
                if alert_channel_type == "room":
                    if new_item['alertid'] not in alert_by_room_key:
                        alert_by_room.append(new_item)
                        alert_by_room_key.append(new_item['alertid'])
                if alert_channel_type == "email":
                    if new_item['alertid'] not in alert_by_email_key:
                        alert_by_email.append(new_item)
                        alert_by_email_key.append(new_item['alertid'])
                if alert_channel_type == "pd":
                    if new_item['alertid'] not in alert_by_pd_key:
                        alert_by_pd.append(new_item)
                        alert_by_pd_key.append(new_item['alertid'])
        logger.info("alert_by_room:%s" % (len(alert_by_room)))
        logger.info("alert_by_email:%s" % (len(alert_by_email)))
        logger.info("alert_by_pd:%s" % (len(alert_by_pd)))

        # send alert by room
        if len(alert_by_room)>0:
            roomIdList = []
            for item in alert_by_room:
                alert_channel_value = dict(item)['alert_channel_value']
                roomIdList.append(alert_channel_value)
            for roomid in roomIdList:
                content = self.getServerItem(alert_by_room,"bot")
                alert_title = "Postgres Alert:"
                job = wbxchatbot()
                job.sendAlertToChatBot(content, alert_title, roomid, "")

        # send alert by email
        if len(alert_by_email)>0:
            emailList = []
            for item in alert_by_email:
                alert_channel_value = dict(item)['alert_channel_value']
                emailList.append(alert_channel_value)
            for emailto in emailList:
                content = self.getServerItem(alert_by_email,"email")
                msg = wbxemailmessage(emailtopic="Postgres Alert: ",receiver=emailto,emailcontent=content)
                logger.info("---- send email ---")
                logger.info("emailto:%s, content=%s" %(emailto,content))
                sendemail(msg)

        # send alert by pd
        if len(alert_by_pd) > 0:
            emailList = []
            for item in alert_by_email:
                alert_channel_value = dict(item)['alert_channel_value']
                emailList.append(alert_channel_value)
            for emailto in emailList:
                content = self.getServerItem(alert_by_email, "pd")
                msg = wbxemailmessage(emailtopic="Postgres Alert: ", receiver=emailto, emailcontent=content)
                logger.info(content)
                sendemail(msg)

        # update pg alert status to SENT
        if len(alertsUpdateStuaus)>0:
            logger.info("update pg alert status to SENT")
            try:
                constr = "%s/%s@%s" % (self.depotdb_username,self.depotdb_password,self.depotdb_url)
                srcconn = cx_Oracle.connect(constr)
                srccsr = srcconn.cursor()
                sql = "update wbxmonitoralertdetail set lastmodifiedtime=sysdate,status=:1 where alertid=:2 "
                srccsr.executemany(sql, alertsUpdateStuaus)
                srcconn.commit()
            except Exception as e:
                logger.error(e)


    # use PrettyTable function to display alert
    def getServerItem(self, listdata,type):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["#", "Alert Type", "Host Name", "DB Name", "Shareplex Port", "Alert Message", "Create Time"]
        index = 1
        for data in listdata:
            line_content = [index, data["alert_type"], data['host_name'], data['db_name'], data['splex_port'],
                            data['parameter'], data['createtime']]
            x.add_row(line_content)
            index += 1
        x.field_names = title
        res = ""
        if "bot" == type:
            res = str(x)
        if "email" == type or "pd" ==type:
            y = x.get_html_string()
            res = str(y)
        return res


if __name__ == '__main__':
    job = pgSendAlert()
    job.initialize()
    job.start()