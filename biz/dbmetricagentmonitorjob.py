import requests
import json

from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from common.wbxutil import wbxutil
from common.config import Config
from common.wbxchatbot import wbxchatbot
from prettytable import PrettyTable

class DBMetricAgentMonitorJob(WbxJob):

    def start(self):
        hasError = False
        rows = []
        jobList = []
        config = Config()
        emailto = config.getEmailTo()
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            auditdao.connect()
            auditdao.startTransaction()
            rows = auditdao.listShutdownJobManagerInstance()
            jobList = auditdao.listFailedJob()
            # rows = [["sjdbormt046","FAILED","2021-08-11 08:01:01"]]
            # jobList = [["sjdbormt046","/u00/app/admin/dbarea/bin/splex_old_msg.sh ","2021-08-11 08:00:00","2021-08-12 02:00:00","{\"minute\":0,\"hour\":\"2,8\"}","2021-08-11 08:01:01","FAILED"]]
            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
            hasError = True
        finally:
            auditdao.close()

        emailmsg = ""
        chatbotmsg = ""
        topic = "Below jobmanager instance are down or have issues or has failed job, please check with Gates Liu"

        for row in rows:
            # This means the job is killed by someone, but not shutdown from page.
            if row[1] == "RUNNING":
                param = json.dumps({"host_name": row[0]})
                headers = {'Content-Type': 'application/json', "Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"}
                response = requests.post("http://10.252.4.88:9000/api/startjobmanagerinstance", param, headers=headers)
                emailmsg="%sStart metricAgent on host %s with response text %s, status %s<br><br>" % (emailmsg, row[0], response.text, response.status_code)
                chatbotmsg += "#### Start metricAgent on host %s with response text %s, status %s \n" % (row[0], response.text.replace("\n", ""), response.status_code)

        if len(rows) > 0:
            emailmsg = '''%s<table border=1><thead><tr><td>Host Name</td><td>Status</td><td>Lastupdatetime</td></tr></thead><tbody>''' % emailmsg
            # chatbotmsg += "\t\t Host Name \t\t Status \t\t Lastupdatetime \n"
            title_list = ["Host Name", "Status", "Lastupdatetime"]
            chatbotmsg += "### Down Agent List\n"
            chatbotmsg += wbxchatbot().address_alert_list(title_list, rows)
            for row in rows:
                emailmsg = "%s<tr><td>%s</td><td>%s</td><td>%s</td></tr>" %(emailmsg, row[0], row[1], row[2])
                # chatbotmsg += "\t\t %s \t %s \t\t %s \n" %(row[0], row[1], row[2])
            emailmsg="%s</tbody></table><br><br>" % emailmsg
            chatbotmsg += "\n\n"
        if chatbotmsg != "":
            # chatbotmsg = "## Job Manager Alert\n" + chatbotmsg
            self.sendAlertMsg("ChatBot","Down_Agent_List",rows)
        else:
            print("GOOD!")

        if len(jobList) > 0:
            emailmsg = '''%s<table border=1><thead><tr><td>Host Name</td><td>CommandStr</td><td>Last Run Time</td><td>Next Run Time</td><td>Job Frequency</td><td>Current Time</td><td>Status</td></tr></thead><tbody>''' % emailmsg
            # chatbotmsg += "\t\t Host Name \t\t\t\t CommandStr \t\t\t\t\t\t\t Last Run Time \t Next Run Time \t Job Frequency \t\t Current Time \t\t Status \n"
            title_list = ["Host Name", "CommandStr", "Last Run Time", "Next Run Time", "Job Frequency", "Current Time", "Status"]
            if chatbotmsg:
                chatbotmsg = "### Failed Job List\n"
            else:
                chatbotmsg = "## MetricAgent Alert \n"
                chatbotmsg += "### Failed Job List\n"
            # chatbotmsg += wbxchatbot().address_alert_list(title_list, jobList)
            for job in jobList:
                emailmsg = "%s<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (emailmsg, job[0], job[1], job[2], job[3], job[4], job[5], job[6])
                # chatbotmsg += "\t %s \t %s \t\t\t\t %s \t %s \t %s \t %s \t %s \n" % (job[0], job[1], job[2], job[3], job[4], job[5], job[6])
            emailmsg = "%s</tbody></table>" % emailmsg

        if emailmsg != "":
            wbxutil.sendmail(topic,emailmsg,emailto, "HTML")
        if chatbotmsg != "":
            # wbxchatbot().alert_msg(chatbotmsg)
            self.sendAlertMsg("ChatBot","Failed_Job_List",jobList)
        return hasError

    # send message to chatbot/email
    def sendAlertMsg(self, Target, alert_type, server_list):
        msg = ' Send Alert Message'
        print(msg)
        bot_server_list = server_list[:21]
        if len(server_list) > 0:
            if Target == "ChatBot":
                s_bot = self.getServerItemsForBot(alert_type, bot_server_list)
                self.sendAlertToChatBot(alert_type, s_bot)
            else:
                pass
        else:
            pass

    # send problem server list alert to chat bot
    def sendAlertToChatBot(self, alert_type, rows_list):
        msg = "Send Alert Message To ChatBot"
        print(msg)

        hasError = False
        if alert_type == "Down_Agent_List":
            msg = "### Job Manager Alert: Down Agent List\n"
            msg += "```\n {} \n```".format(rows_list)
        elif alert_type == "Failed_Job_List":
            msg = "### MetricAgent Alert: Failed Job List\n"
            msg += "```\n {} \n```".format(rows_list)
        wbxchatbot().alert_msg_to_dbabot(msg)
        # wbxchatbot().alert_msg_to_dbateam(msg)
        # wbxchatbot().alert_msg_to_dbabot_by_roomId(msg, "Y2lzY29zcGFyazovL3VzL1JPT00vMTkyYjViNDAtYTI0Zi0xMWViLTgyYTktZDU3NTFiMmIwNzQz")
        return hasError

    # use PrettyTable function to display chatbot alert
    def getServerItemsForBot(self, alert_type, listdata):
        if len(listdata) == 0:
            return ""
        x = PrettyTable()
        title = []
        if alert_type == "Down_Agent_List":
            title = ["Host Name", "Status", "Lastupdatetime"]
        elif alert_type == "Failed_Job_List":
            title = ["Host Name", "CommandStr", "Last Run Time", "Next Run Time", "Job Frequency", "Current Time", "Status"]
        for data in listdata:
            x.add_row(data)
        x.field_names = title
        print(x)
        return str(x)

if __name__ == "__main__":
    job = DBMetricAgentMonitorJob()
    job.initialize()
    job.start()