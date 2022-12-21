import requests
from prettytable import PrettyTable

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxchatbot import wbxchatbot
from common.wbxmail import wbxemailmessage, wbxemailtype, sendemail
from common.wbxssh import wbxssh
from dao.wbxauditdbdao import wbxauditdbdao

# check cron status
class CheckCronStatusJob(WbxJob):

    def start(self):
        config = Config()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.alertRoomId = config.getAlertRoomId()
        # self.alertRoomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5"
        self.main()

    def main(self):
        rows = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.getDbInfo()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

        sendToBot=[]
        for i, row in enumerate(rows):
            host_name = row[0]
            print("----------- {0}/{1} {2} ----------- ".format(i, len(rows), row[0]))
            if host_name in ("iddbormt01","iddbormt02","sjdbormt065","sjdbormt066"):
                print("skip %s" %(host_name))
            else:
                # cmd = '/sbin/service crond status'
                # cmd = 'crontab < /dev/null | sudo systemctl restart crond | sudo systemctl status crond'
                cmd = 'sudo systemctl restart crond | sudo systemctl status crond'
                user = row[2]
                pwd = row[3]
                # pwd = "o8ziaIZW%T5"
                is_exist_wbxjobinstance = row[4]
                server = wbxssh(host_name, 22, user, pwd)
                try:
                    server.connect()
                    res, status = server.exec_command(cmd)
                    print("{0}".format(res))
                    print("status={0}, is_exist_wbxjobinstance={1}".format(status, is_exist_wbxjobinstance))
                    resline = res.split("\n")
                    if len(resline)>1:
                        for line in resline:
                            if "Active:" in line:
                                res = line.strip()
                                break
                    print("res={0}" .format(res))
                    isExist = self.auditdao.getCronJobStatus(host_name)
                    if isExist:
                        self.auditdao.updateCronJobStatus(host_name,res,is_exist_wbxjobinstance)
                    else:
                        self.auditdao.insertCronJobStatus(host_name,res,is_exist_wbxjobinstance)
                    self.auditdao.commit()
                except Exception as e:
                    info = {}
                    info['host_name'] = host_name
                    info['errormsg'] = str(e)
                    sendToBot.append(info)
                    self.auditdao.rollback()
                    print(e)
                finally:
                    server.close()
                    self.auditdao.close()
        # delete records with monitor time exceeds one day
        try:
            self.auditdao.deleteInvalidCronJobStatus()
            self.sendAlertEmail()
            self.sendAlertProblemToBot(sendToBot)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()

    def sendAlertProblemToBot(self,problem_row):
        rows = self.auditdao.getAlertCronbList()
        if len(rows)>0 or len(problem_row)>0:
            content = self.getServerItemsForBot(rows)
            alert_title = "" \
                          "" % ()
            wbxchatbot().sendAlertToChatBot(content, alert_title, self.alertRoomId, "")

    def getServerItemsForBot(self, listdata):
        if len(listdata) == 0: return ""
        x = PrettyTable()
        title = ["#","Host Name","Crond Status", "DB Agent Status","Monitor Time"]
        index = 1
        for data in listdata:
            item = dict(data)
            db_agent_exist = "running"
            if item['db_agent_exist'] == "0":
                db_agent_exist = "stopped"
            line_content = [index,item["host_name"],item['status'],db_agent_exist,item['monitor_time']]
            x.add_row(line_content)
            index += 1
        x.field_names = title
        return str(x)

    def sendAlertEmail(self):
        rows = self.auditdao.getAlertCronbList()
        if len(rows)>0:
            print("send alert email")
            mail_content = "<p>Crond or job management status is abnormal, please check it. </p>"
            mail_content += "<table border=\"1\" cellspacing=\"0\">"
            mail_content += "<tr >"
            mail_content += "<th>Host Name</th>"
            mail_content += "<th>Crond Status</th>"
            mail_content += "<th>Job Management Status</th>"
            mail_content += "<th>Monitor Time</th>"
            mail_content += "</tr>"
            for row in rows:
                mail_content += "<tr >"
                mail_content += "<td>"+str(row[0])+"</td>"
                mail_content += "<td>" + str(row[1]) + "</td>"
                Job_status = "running"
                if str(row[2]) == "0":
                    Job_status = "stopped"
                mail_content += "<td>" + Job_status + "</td>"
                mail_content += "<td>" + str(row[3]) + "</td>"
                mail_content += "</tr>"
            mail_content += "</table>"
            msg1 = wbxemailmessage(emailtopic=wbxemailtype.EMAILTYPE_CHECH_CRONJOB_STATUS, receiver="cwopsdba@cisco.com",
                                   emailcontent=mail_content)
            sendemail(msg1)

    def test(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.alertRoomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5"
        self.main()


if __name__ == '__main__':
    job = CheckCronStatusJob()
    job.initialize()
    job.test()
