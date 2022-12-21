import sys
import smtplib
import http.client

from email.mime.text import MIMEText
from email.header import Header
import logging

from tabulate import tabulate
emailthreadpool = None
logger = logging.getLogger("DBAMONITOR")

class wbxemailtype:
    EMAILTYPE_CHECH_CRONJOB_STATUS = "Alert: check cronjob status result found problem"
    EMAILTYPE_CHECH_DBUNDOjob_STATUS = "Alert: check db undo status result found problem"
    EMAILTYPE_CHECH_ORACLE_PWD_STATUS = "Alert: check cronjob host name oracle pwd found problem"
    EMAILTYPE_CHECH_SESSION_STATUS="Alert: activate sessions(exclude background sessions) > 50"
    EMAILTYPE_CHECH_DB_STATUS = "Alert: DB is not accessible,please check it."
    EMAIL_FORMAT_PLAIN="plain"
    EMAIL_FORMAT_HTML = "html"

class wbxemailmessage:
    def __init__(self, emailtopic, emailcontent, priority = 10, receiver="zhiwliu@cisco.com", issendtospark="N", emailformat="plain"):
        self.priority = priority
        self.sender = "dbamonitortool@cisco.com"
        self.receiver = receiver
        self.emailtopic = emailtopic
        self.emailcontent = emailcontent
        self.emailformat = emailformat
        self.issendtospark = issendtospark

    def __str__(self):
        return self.emailcontent

#sendmail(self, from_addr, to_addrs, msg, mail_options=[],rcpt_options=[]):
def sendemail(emailmsg):
    if isinstance(emailmsg, wbxemailmessage):
        try:
            mailto = emailmsg.receiver.split(",")
            # message = MIMEText(emailmsg.emailcontent,_subtype = emailmsg.emailformat, _charset = 'utf-8')
            message = MIMEText(emailmsg.emailcontent, "html", "utf-8")
            message['From'] = Header(emailmsg.sender)
            message['To'] = Header(",".join(mailto))
            subject = emailmsg.emailtopic
            message['Subject'] = Header(subject)

            smtpObj = smtplib.SMTP(host='mda.webex.com:25')
            senderrs= smtpObj.sendmail(emailmsg.sender, mailto, message.as_string())
            if len(senderrs) > 0:
                logger.error("Unexpected error:{0}".format(senderrs))
            smtpObj.quit()
        except smtplib.SMTPException:
            logger.error("Unexpected error:", sys.exc_info()[0])


def sendtospark(message):
    try:
        conn = http.client.HTTPSConnection("stap.webex.com")
        message = message.replace("\n", "<br>")
        payload = "{\"message\":\"%s\"}" % message
        print(payload)
        headers = {
            'authorization': "Basic c3RhcDpFTkc=",
            'content-type': "application/json",
            'cache-control': "no-cache"
        }
        conn.request("POST", "/sparkAPI/client/sendMsg/Test%20INC", payload, headers)
        res = conn.getresponse()
        data = res.read()
    except Exception as e:
        logger.error("Unexpected error:", sys.exc_info()[0])

def sendalert(emailmsg):
    if emailmsg.receiver is not None:
        sendemail(emailmsg)
    if emailmsg.issendtospark == 'Y':
        sendtospark(emailmsg.emailcontent)

def sendemail_for_adbmon(emailmsg, rows, title, html_template):
    html = html_template
    mailto = emailmsg.receiver.split(",")
    headers=title

    html = html.format(table=tabulate(rows, headers=headers, tablefmt="html"))
    message = MIMEText(html, "html", "utf-8")

    message['From'] = Header(emailmsg.sender)
    message['To'] = Header(",".join(mailto))
    subject = emailmsg.emailtopic
    message['Subject'] = Header(subject)

    smtpObj = smtplib.SMTP(host='mda.webex.com:25')
    senderrs = smtpObj.sendmail(emailmsg.sender, mailto, message.as_string())
    if len(senderrs) > 0:
        logger.error("Unexpected error:{0}".format(senderrs))
    smtpObj.quit()


if __name__ == "__main__":
    volist = [{"username":'tahoe', "userhost":"sjdbbtsmct1.webex.com", "failedcount":1},
              {"username": 'tahoe', "userhost": "sjdbgridcontrol1.webex.com", "failedcount": 10}]

    emailcontent = "%-30s%-30s%-10s\n-----------------------------------------------------------\n" % ("username", "userhost", "failedtimes")

    for vo in volist:
        emailcontent = "%s%-50s%-50s%-30s\n" % (emailcontent, vo["username"], vo["userhost"], vo["failedcount"])

    msg1 = wbxemailmessage(emailtopic=wbxemailtype.EMAILTYPE_CHECH_CRONJOB_STATUS,receiver="lexing@cisco.com", emailcontent=emailcontent)
    sendemail(msg1)
