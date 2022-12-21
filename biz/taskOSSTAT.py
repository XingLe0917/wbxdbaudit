import cx_Oracle

from biz.wbxjob import WbxJob
from common.config import Config
from dao.wbxauditdbdao import wbxauditdbdao


class taskOSSTAT(WbxJob):
    def start(self):
        config = Config()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.main()

    def main(self):
        rows = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.getWEBDBTnsInfo()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        print("taskOSSTAT db_name:{0}".format(len(rows)))

        tns_vo = {}
        for vo in rows:
            item = dict(vo)
            db_name = item['db_name']
            # trim_host = item['trim_host']
            listener_port = item['listener_port']
            service_name = "%s.webex.com" % item['service_name']
            key = str(db_name)
            value = '(DESCRIPTION ='
            if item['scan_ip1']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip1'], listener_port)
            if item['scan_ip2']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip2'], listener_port)
            if item['scan_ip3']:
                value = '%s (ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = %s))' % (
                    value, item['scan_ip3'], listener_port)
            value = '%s (LOAD_BALANCE = yes) (CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = %s)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 3)(DELAY = 5))))' % (
                value, service_name)
            tns_vo[key] = value
            item = {}
            item['db_name'] = db_name
            item['tns'] = tns_vo[db_name]
            self.exec_job(item)

    def exec_job(self, item):
        db_name = item['db_name']
        tns = item['tns']
        print("exec_job, db_name={0}".format(db_name))
        # sql = '''
        #         delete FROM test.WBXDB_MONITOR_OSSTAT WHERE PROCESS_STATUS='Pending' and sample_time < sysdate -1/24
        #         '''
        sql = '''
        select count(*) FROM test.WBXDB_MONITOR_OSSTAT WHERE PROCESS_STATUS='Pending' and sample_time < sysdate -1/24
        '''
        try:
            conn = cx_Oracle.connect("system/sysnotallow@" + tns)
            cursor = conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()

            print("db_name:{0},count={1}".format(db_name,results[0]))
            conn.commit()
            return list
        except Exception as e:
            print("exec_job error, db_name={0}, e={1},tns ={2}".format(db_name, e, tns))



if __name__ == '__main__':
    job = taskOSSTAT()
    job.initialize()
    job.start()
    job.main()