import logging

import sys
import os
import pytz

project = "wbxdbaudit"
sys.path.append(os.getcwd().split(project)[0] + project)
from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from dao.vo.dbdatamonitorvo import WebDomainDataMonitorVO,MeetingDataMonitorVO
from common.wbxssh import wbxssh
import json
from common.wbxchatbot import wbxchatbot
import datetime


logger = logging.getLogger("dbaudit")
pst_timezone = pytz.timezone('US/Pacific')


class UpdateOncallChangeToChatbotJob(WbxJob):

    def start(self):
        self.alert_type = self.args[0]
        if not self.alert_type:
            raise Exception("please input the alert type")
        print(self.alert_type)
        self.shift_window_time_map_pst = {
            "CHINA": {
                "start_time": datetime.datetime.now(tz=pst_timezone).replace(hour=16, minute=0, second=0, microsecond=0),
                "end_time": datetime.datetime.now(tz=pst_timezone).replace(hour=16, minute=0, second=0, microsecond=0) + datetime.timedelta(hours=8)
            },
            "INDIA": {
                "start_time": datetime.datetime.now(tz=pst_timezone).replace(hour=0, minute=0, second=0, microsecond=0),
                "end_time": datetime.datetime.now(tz=pst_timezone).replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(hours=8)
            }
        }
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.updateOncallChangeToChatbotJob()

    def updateOncallChangeToChatbotJob(self):


        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            connectinfo = None
            dbvolist = self.auditdao.getDBConnectionList("BTS_PROD", "STAP", "ALL", "ALL", "SYSTEM")
            for dbvo in dbvolist:
                if dbvo.getDBName() == "STAPDB":
                    connectinfo = dbvo.getConnectionurl()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
            raise Exception(e)
        finally:
            if self.auditdao:
                self.auditdao.close()
        print("start...")
        error_info = ""
        datamonitordao = None
        if not connectinfo:
            raise Exception("Cannot find the connectioninfo by stapdb")
        now_time = datetime.datetime.now(tz=pst_timezone) #.strftime("%Y-%m-%d %H:%M:%S")
        logger.info("nowtime(pst): %s" % now_time.strftime("%Y-%m-%d %H:%M:%S"))
        shift_type = self.is_whose_shift(now_time)
        logger.info("this is %s's shift" % shift_type)
        if shift_type not in self.shift_window_time_map_pst.keys():
            raise Exception("the now time is %s, not CHINA/INDIA shift!" % now_time.strftime("%Y-%m-%d %H:%M:%S"))
        oncall_starttime = self.shift_window_time_map_pst[shift_type]["start_time"].strftime("%Y-%m-%d %H:%M:%S")#"2022-02-11 11:00:00"
        oncall_endtime = self.shift_window_time_map_pst[shift_type]["end_time"].strftime("%Y-%m-%d %H:%M:%S") #"2022-02-11 16:00:00"
        print(oncall_starttime, oncall_endtime)
        try:
            datamonitordao = dbdatamonitordao(connectinfo)
            datamonitordao.connect()
            datamonitordao.startTransaction()
            change_list = []
            if self.alert_type == "START":
                change_list = datamonitordao.getstapchangelist(oncall_starttime, oncall_endtime)
            if self.alert_type == "END":
                change_list = datamonitordao.getstapchangereport(oncall_starttime, oncall_endtime)
            logger.info(change_list)
            alert_change_list(change_list, oncall_starttime, oncall_endtime, self.alert_type)
            datamonitordao.commit()
        except Exception as e:
            datamonitordao.rollback()
            print(e)
            error_info = str(e)
        finally:
            if datamonitordao:
                datamonitordao.close()
            if self.auditdao:
                self.auditdao.close()
        if error_info:
            raise Exception(error_info)

    def is_whose_shift(self, nowtime):
        if self.alert_type == "START":
            if nowtime >= self.shift_window_time_map_pst["CHINA"]["start_time"] and nowtime < self.shift_window_time_map_pst["CHINA"]["end_time"]:
                return "CHINA"
            elif nowtime >= self.shift_window_time_map_pst["INDIA"]["start_time"] and nowtime < self.shift_window_time_map_pst["INDIA"]["end_time"]:
                return "INDIA"
        elif self.alert_type == "END":
            if nowtime >= self.shift_window_time_map_pst["CHINA"]["end_time"] - datetime.timedelta(hours=1) and nowtime < self.shift_window_time_map_pst["CHINA"]["end_time"] + datetime.timedelta(hours=1):
                return "CHINA"
            elif nowtime >= self.shift_window_time_map_pst["INDIA"]["end_time"] - datetime.timedelta(hours=1) and nowtime < self.shift_window_time_map_pst["INDIA"]["end_time"] + datetime.timedelta(hours=1):
                return "INDIA"



def alert_change_list(change_list, oncall_starttime, oncall_endtime, alert_type):
    if not change_list:
        print("There is no change list!")
        return True
    oncall_date = oncall_starttime.split(" ")[0]
    oncall_start_time = oncall_starttime.split(" ")[-1]
    oncall_end_time = oncall_endtime.split(" ")[-1]
    alert_msg = ""
    if alert_type == "START":
        alert_msg += "### {0} Scheduled DBA Changes Between {1} - {2} PST\n\n  ####  \n\n".format(oncall_date, oncall_start_time, oncall_end_time)
        for change_item in change_list:
            alert_msg += "* %s \n" % change_item
        wbxchatbot().alert_msg_to_DBA_Deployment_Notifications_and_call_oncall(alert_msg)
        # wbxchatbot().alert_msg_to_person(alert_msg,"yejfeng@cisco.com")
        logger.info("start Alert: %s" % (alert_msg))
    elif alert_type == "END":
        alert_msg += "### * * * DBA Change Status - {0} Between ({1} - {2}) PST * * * \n\n  ###  \n\n".format(oncall_date,
                                                                                           oncall_start_time,
                                                                                           oncall_end_time)
        for change_item in change_list:
            alert_msg += "* %s \n" % change_item
        wbxchatbot().alert_msg_to_DBA_Deployment_Notifications_and_call_oncall(alert_msg)
        # wbxchatbot().alert_msg_to_person(alert_msg, "yejfeng@cisco.com")
        logger.info("end Alert: %s" % (alert_msg))



if __name__ == '__main__':

    job = UpdateOncallChangeToChatbotJob()
    job.initialize(["START"])
    job.auditdao = wbxauditdbdao(job.depot_connectionurl)
    job.updateOncallChangeToChatbotJob()
