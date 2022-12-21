from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao

jobmap = {"WbxCronjobMonitor":"jobname = args[-1]"}

class JobStatusPatch(WbxJob):
    def __init__(self, func):
        self.func = func
        self.initialize()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)

    def __call__(self, *args, **kwargs):
        jobname = args[0]
        if jobname in jobmap.keys():
            scop = {"args":args}
            exec(jobmap[jobname],scop)
            jobname = scop["jobname"]

        self.startJob(jobname)

        hasError = self.func(*args, **kwargs)

        status="FAILED" if hasError else "SUCCESS"
        self.endJob(jobname,status)

        return hasError

    def startJob(self,jobname):
        hasError = False
        try:
            sql = '''
                update pccpjobmonitor set 
                    starttime=sysdate,endtime='', status = 'RUNNING', scheduletime =fn_getcronnextbyjob(jobname),duration='0' where jobname ='%s'
                '''%(jobname)
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.session.execute(sql)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()
        return hasError

    def endJob(self,jobname,status):
        hasError = False
        try:
            sql = '''
                update pccpjobmonitor set 
                    endtime=sysdate, status = '%s',duration= (sysdate-starttime)*24*60 where jobname ='%s'
                ''' % (status,jobname)
            self.auditdao.connect()
            self.auditdao.startTransaction()
            self.auditdao.session.execute(sql)
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()
        return hasError

