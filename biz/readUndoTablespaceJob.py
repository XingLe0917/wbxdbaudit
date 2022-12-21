import datetime

import requests

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from dao.wbxauditdbdao import wbxauditdbdao

#read webex DB UNDO tablespace from telegraf and send alert email and send to chatbot
class ReadUndoTablespaceJob(WbxJob):
    def start(self):
        config = Config()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._client = config.getInfluxDBclientNew()
        self.alertList={}
        self.alertRoomId = config.getAlertRoomId()
        self.main()

    def main(self):
        sql = "select * from db_undostat where time > now() - 6m  order by time desc "
        results = self._client.query(sql)
        points = results.get_points()
        num = 0
        alert_list = {}
        for data in points:
            # print(data)
            num+=1
            vo = dict(data)
            t = str(vo['time']).split("T")
            vo['time'] = t[0] + " " + str(t[1])[0:8]
            vo['host_name'] = vo['host_name'].split(".")[0]
            db_name = vo['db_name']
            tablespace_name = vo['tablespace_name']
            key = db_name + "_" + tablespace_name
            if vo['used_ratio']>80 :
                if key not in alert_list:
                    print(data)
                    alert_list[db_name] = vo
        self.alertList=alert_list
        self.sendAlertEmail()
        self.sentAlertToBot()

    def sendAlertEmail(self):
        rows = self.alertList
        if len(rows) > 0:
            print("send alert email")
            mail_content = "<p>The following DB Used Ratio exceed 80%. </p>"
            mail_content += "<table border=\"1\" cellspacing=\"0\">"
            mail_content += "<tr >"
            mail_content += "<th>Time</th>"
            mail_content += "<th>DB Name</th>"
            mail_content += "<th>Host Name</th>"
            mail_content += "<th>INSTANCE_NAME</th>"
            mail_content += "<th>Used(GB)</th>"
            mail_content += "<th>Tablespace Name</th>"
            mail_content += "<th>Total(GB)</th>"
            mail_content += "<th>Used Ratio</th>"
            mail_content += "</tr>"
            for row in rows:
                mail_content += "<tr >"
                mail_content += "<td>" + str(rows[row]['time']) + "</td>"
                mail_content += "<td>" + str(rows[row]['db_name']) + "</td>"
                mail_content += "<td>" + str(rows[row]['host_name']) + "</td>"
                mail_content += "<td>" + str(rows[row]['instance_name']) + "</td>"
                mail_content += "<td>" + str(rows[row]['used']) + "</td>"
                mail_content += "<td>" + str(rows[row]['tablespace_name']) + "</td>"
                mail_content += "<td>" + str(rows[row]['total']) + "</td>"
                mail_content += "<td>" + str(rows[row]['used_ratio']) + "</td>"
                mail_content += "</tr>"
            mail_content += "</table>"
            msg1 = wbxemailmessage(emailtopic=wbxemailtype.EMAILTYPE_CHECH_DBUNDOjob_STATUS,
                                   receiver="lexing@cisco.com",
                                   emailcontent=mail_content)
            sendemail(msg1)

    def sentAlertToBot(self):
        rows = self.alertList
        if(len(self.alertList)>0):
            print("sentAlertToBot")
            content = "### Alert DB Undostat. The following DB Used Ratio exceed 80%." + "\n"
            content += "\tTime\t\t\t\t\tDB Name\t\tHost Name\t\tInstance Name\tFree \tTablespace Name\t\tTotal\tUsed Ratio" + "\n"
            for row in rows:
                content += "\t" + str(rows[row]['time']) + "\t\t" + rows[row]['db_name']
                if len(rows[row]['db_name']) < 8:
                    content += "\t"
                content += "\t" + str(rows[row]['host_name'])
                if len(rows[row]['host_name']) < 12:
                    content += "\t"
                content += "\t" + str(rows[row]['instance_name'])
                content += "\t\t" + str(rows[row]['free'])
                if len(str(rows[row]['free'])) < 4:
                    content += "\t"
                content += "\t" + rows[row]['tablespace_name'] + "\t\t\t" + str(rows[row]['total']) + "\t" + str(
                    rows[row]['used_ratio']) + "\n"
            job = wbxchatbot()
            job.alert_msg_to_dbabot_by_roomId(msg=content,roomId=self.alertRoomId)


if __name__ == '__main__':
    starttime = datetime.datetime.now()
    job = ReadUndoTablespaceJob()
    job.initialize()
    job.start()
    endtime = datetime.datetime.now()
    time = (endtime - starttime).seconds
    print("\n times:{0}".format(time))

