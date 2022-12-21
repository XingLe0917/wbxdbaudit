import logging

from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao
from dao.dbdatamonitordao import dbdatamonitordao
from dao.vo.dbdatamonitorvo import WebDomainDataMonitorVO,MeetingDataMonitorVO
from common.wbxutil import wbxutil

# In webdb more and more meetings are created; and more and more PCN access code are used;
# so we need to check whether the meeting data is right. and check whether PCN is used up;
class DBDataMonitorJob(WbxJob):

    def start(self):
        hasError = False
        dbvolist = []
        auditdao = wbxauditdbdao(self.depot_connectionurl)
        try:
            auditdao.connect()
            auditdao.startTransaction()
            dbvolist = auditdao.getDBConnectionList("PROD","WEB","ALL","ALL","app")
            auditdao.commit()
        except Exception as e:
            auditdao.rollback()
            print(e)
            hasError = True
        finally:
            auditdao.close()

        passcodedict = {}
        meetinguuiddict = {}
        for dbvo in dbvolist:
            logging.info("Get data from db %s_%s" %(dbvo.getTrimHost(), dbvo.getDBName()))
            dao = None
            try:
                connectionurl = dbvo.getConnectionurl()
                dao = dbdatamonitordao(connectionurl)
                dao.connect()
                dao.startTransaction()
                errorcount = dao.getPasscodeAllocationLog()
                passcodedict[dbvo.getClusterName()] = errorcount
                meetinguuiddict[dbvo] = dao.getMeetingUUIDDataWithDifferentConfID()
                dao.commit()
            except Exception as e:
                if dao is not None:
                    dao.rollback()
                hasError = True
            finally:
                dao.close()

        itemname = "PasscodeJobError"
        try:
            auditdao.connect()
            auditdao.startTransaction()
            for clusterName, errorcount in passcodedict.items():
                logging.info(clusterName)
                monitorvo = auditdao.getWebdomainDataMonitorVO(clusterName,itemname)
                if monitorvo is None:
                    monitorvo = WebDomainDataMonitorVO()
                    monitorvo.clustername = clusterName
                    monitorvo.itemname = itemname
                    monitorvo.itemvalue = errorcount
                    auditdao.newWebDomainDataMonitorVO(monitorvo)
                else:
                    monitorvo.itemvalue = errorcount

            for dbvo, valset in meetinguuiddict.items():
                (case1, case2, case3, case4, case5) = valset
                meetingDatavo = auditdao.getMeetingDataMonitorVO(dbvo.getTrimHost(), dbvo.getDBName())
                if meetingDatavo is not None:
                    meetingDatavo.case1 = case1
                    meetingDatavo.case2 = case2
                    meetingDatavo.case3 = case3
                    meetingDatavo.case4 = case4
                    meetingDatavo.case5 = case5
                    meetingDatavo.monitor_time = wbxutil.getcurrenttime()
                else:
                    meetingDatavo = MeetingDataMonitorVO()
                    meetingDatavo.trim_host = dbvo.getTrimHost()
                    meetingDatavo.db_name = dbvo.getDBName()
                    meetingDatavo.cluster_name = dbvo.getClusterName()
                    meetingDatavo.case1 = case1
                    meetingDatavo.case2 = case2
                    meetingDatavo.case3 = case3
                    meetingDatavo.case4 = case4
                    meetingDatavo.case5 = case5
                    meetingDatavo.monitor_time = wbxutil.getcurrenttime()
                    auditdao.newMeetingDataMonitorVO(meetingDatavo)
            auditdao.commit()
        except Exception as e:
            if auditdao is not None:
                auditdao.rollback()
            print(e)
            hasError = True
        finally:
            auditdao.close()
        return hasError