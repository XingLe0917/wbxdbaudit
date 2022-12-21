import time

from biz.wbxjob import WbxJob
from common.config import Config
from common.wbxssh import wbxssh
from common.wbxchatbot import wbxchatbot
from datetime import datetime

'''
Desc:  1. If wbxmonitor.log has no data in the last 5 minutes, then send alert to check.
       2. If wbxmonitor_errer.log has data in lastest 5 minutes, then send errer message to chatbot
'''
class wbxdbMonitorJob(WbxJob):
    def start(self):
        config = Config()
        # self.roomId = config.getAlertRoomId()
        self.roomId = "Y2lzY29zcGFyazovL3VzL1JPT00vZDI4MjcxMDgtMTZiNy0zMmJjLWE3NmUtNmVlNTEwMjU4NDk5"
        self.main()

    def main(self):
        host_name = 'sjgrcabt104'
        server = wbxssh(host_name, 22, "oracle", "Rman$1357")
        try:
            server.connect()
            cmd = "stat -c %Y /home/oracle/wbxmonitor/log/wbxmonitor.log"
            logfiletimestamp, status1 = server.exec_command(cmd)
            print("logfiletimestamp={0}, {1}" .format(logfiletimestamp,datetime.fromtimestamp(int(logfiletimestamp)).strftime('%Y-%m-%d %H:%M:%S')))
            cmd = "date +%s"
            nowtimestamp, status2 = server.exec_command(cmd)
            print("nowtimestamp={0}, {1}".format(nowtimestamp, datetime.fromtimestamp(int(nowtimestamp)).strftime('%Y-%m-%d %H:%M:%S')))
            log_interval = int(nowtimestamp) - int(logfiletimestamp)
            print("log_interval(s):{0}" .format(log_interval))
            interval = 300
            content = ""
            if log_interval>interval:
                content = "### Alert wbxdbMonitor. " +"\n"
                content += "The wbxdbMonitor job has issue!  Please check it "

            cmd = "stat -c %Y /home/oracle/wbxmonitor/log/wbxmonitor_errer.log"
            errorfiletimestamp, status1 = server.exec_command(cmd)
            print("errorfiletimestamp={0}, {1}".format(errorfiletimestamp, datetime.fromtimestamp(int(errorfiletimestamp)).strftime(
                '%Y-%m-%d %H:%M:%S')))
            errer_interval = int(nowtimestamp) - int(errorfiletimestamp)
            print("errer_interval(s):{0}".format(errer_interval))
            if errer_interval < interval:
                cmd = "cat /home/oracle/wbxmonitor/log/wbxmonitor_errer.log | tail -5"
                logs, status1 = server.exec_command(cmd)
                content = ""
                content = "### Error log from wbxdbMonitor job. Please check it. " + "\n"
                content += logs
            if content:
                job = wbxchatbot()
                print(content)
                job.alert_msg_to_dbabot_by_roomId(msg=content, roomId=self.roomId)
            cmd = "cd /home/oracle/wbxmonitor ;ps aux|grep wbxdbMonitor.py|grep -v grep | wc -l"
            # logs, status1 = server.exec_command(cmd)
            # print("wbxdbMonitor.py : %s" %(logs))
            if content:
                msg = "restart service wbxmonitor"
                cmd = "cd /home/oracle/wbxmonitor ;ps aux|grep wbxdbMonitor.py|grep -v grep | awk '{print $2}' | xargs kill -9 ; nohup python3 wbxdbMonitor.py &"
                server.exec_command(cmd)
                job = wbxchatbot()
                job.alert_msg_to_dbabot_by_roomId(msg=msg, roomId=self.roomId)
        except Exception as e:
            print(e)
        finally:
            server.close()

    def test(self):
        self.main()

if __name__ == '__main__':
    job = wbxdbMonitorJob()
    job.initialize()
    job.test()