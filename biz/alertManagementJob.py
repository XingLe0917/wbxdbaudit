import logging
import uuid

from biz.wbxjob import WbxJob
from common.config import Config
from dao.vo.wbxautotaskjobvo import wbxautotaskvo, wbxautotaskjobvo
from dao.wbxauditdbdao import wbxauditdbdao

logger = logging.getLogger("dbaudit")

class AlertManagementJob(WbxJob):

    def start(self):
        config = Config()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.auto_task_priority = config.get_auto_task_priority()
        self.main()

    def main(self):
        rows = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.get_wbxmonitoralert2()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)

        try:
            logger.info("records={0}" .format(len(rows)))
            index = 1
            for row in rows:
                print("************ {0}/{1} ************" .format(index,len(rows)))
                item = dict(row)
                alertid = item['alertid']
                alert_type = item['alert_type']
                priority = 10
                if alert_type in self.auto_task_priority:
                    priority = self.auto_task_priority[alert_type]
                parameter = item['parameter']
                host_name = item['host_name']
                db_name = item['db_name']
                splex_port = item['splex_port']
                taskid = uuid.uuid4().hex
                taskvo = wbxautotaskvo(taskid=taskid,task_type=alert_type, parameter=parameter,priority=priority)
                logger.info("addWbxAutoTask:{0}".format(taskvo.__dict__))
                self.auditdao.addWbxAutoTask(taskvo)
                self.generateJobs(taskvo,host_name,db_name,splex_port)
                logger.info("update wbxmonitoralert2, alertid={0},taskid={1}" .format(alertid,taskid))
                self.auditdao.updateWbxmonitoralert2(alertid,taskid)
                self.auditdao.commit()
                index +=1
        except Exception as e:
            self.auditdao.rollback()
            logger.error(e)
        finally:
            self.auditdao.close()

    def generateJobs(self,taskvo,host_name,db_name,splex_port):
        taskid = taskvo.taskid
        job_action_list = ["preverify", "fix", "postverify"]
        processorder = 1
        try:
            for job_action in job_action_list:
                jobvo = wbxautotaskjobvo(jobid=self.getJobID(), taskid=taskid, db_name=db_name, host_name=host_name,
                                        splex_port=splex_port, parameter=taskvo.parameter,
                                          job_action=job_action, execute_method="SYNC", processorder=processorder, status="PENDING")
                processorder += 1
                logger.info("addAutoTaskJob:{0}" .format(jobvo.__dict__))
                self.auditdao.addAutoTaskJob(jobvo)
        except Exception as e:
            logger.error(e)

    def getJobID(self):
        return uuid.uuid4().hex

    def test(self):
        config = Config()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.auto_task_priority = config.get_auto_task_priority()
        self.main()

if __name__ == '__main__':
    job = AlertManagementJob()
    job.initialize()
    job.test()