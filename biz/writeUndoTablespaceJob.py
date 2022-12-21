import datetime
from concurrent.futures._base import as_completed
from concurrent.futures.thread import ThreadPoolExecutor


import cx_Oracle

from biz.wbxjob import WbxJob
from common.config import Config
from dao.wbxauditdbdao import wbxauditdbdao

# Collect webex DB UNDO tablespace to telegraf
class UndoTablespaceJob(WbxJob):
    def start(self):
        config = Config()
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self._client = config.getInfluxDBclientNew()

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
        finally:
            self.auditdao.close()
        print("UndoTablespaceJob db_name:{0}".format(len(rows)))

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

        with ThreadPoolExecutor(max_workers=30) as t:
            all_task = []
            for db_name in tns_vo:
                item = {}
                item['db_name'] = db_name
                item['tns'] = tns_vo[db_name]
                try:
                    obj = t.submit(self.query_undo_tablespace, item)
                    all_task.append(obj)
                except Exception as e:
                    print("query_undo_tablespace error, db_name={0}, e={1},tns ={2}" .format(db_name,str(e),item['tns']))

            for future in as_completed(all_task):
                datas = future.result()
                if datas:
                    for data in datas:
                        json_body = [
                            {
                                "measurement": "db_undostat",
                                "tags": {
                                    "db_name": data['DB_NAME'],
                                    "host_name":data['HOST_NAME'],
                                    "instance_name": data['INSTANCE_NAME']
                                },
                                "fields": {
                                    "tablespace_name": data['TNM'],
                                    "total":float(data['TOTAL']),
                                    "free":float(data['FREE']),
                                    "used": float(data['USED']),
                                    "used_ratio":float(data['% USED'])
                                }
                            }
                        ]
                        try:
                            print(json_body)
                            self._client.write_points(json_body)
                        except Exception as e:
                            print("write to influxDB error, data={0}, e={1}".format(data, e))
                    # print(datas)


    def query_undo_tablespace(self,item):
        db_name = item['db_name']
        tns = item['tns']
        # sql = '''
        #          select a.tablespace_name TNM,
        #          trunc(a.bytes/1024/1024/1024,2) TOTAL,
        #          trunc(c.bytes/1024/1024/1024,2) FREE,
        #          trunc((a.bytes-c.bytes)/1024/1024/1024,2) "USED",
        #         trunc((a.bytes-c.bytes)*100/a.bytes,0) "% USED",
        #         --trunc((c.bytes*100)/a.bytes,0) "% FREE",
        #         I.host_name,I.instance_name
        #         from SYS.SM$TS_AVAIL A,SYS.SM$TS_FREE C,v$instance I
        #         WHERE A.TABLESPACE_NAME=C.TABLESPACE_NAME
        #         AND A.TABLESPACE_NAME IN (select distinct tablespace_name from dba_tablespaces)
        #         and a.tablespace_name like 'UNDO%'
        #         order by 4 desc
        #         '''

        sql = '''
        select a.tablespace_name TNM,
        a.total_size TOTAL,
        b.used_size USED,
        a.total_size - b.used_size FREE,
        round(b.used_size / a.total_size * 100, 2) "% USED",
        I.host_name,I.instance_name
        from (SELECT tablespace_name,
        round(sum(bytes) / 1024 / 1024 / 1024, 2) total_size
        FROM dba_data_files
        where tablespace_name like 'UNDO%'
        GROUP BY tablespace_name) a,
        (select tablespace_name,
        round(sum(ue.bytes) / 1024 / 1024 / 1024, 2) used_size
        from DBA_UNDO_EXTENTS ue
        where ue.status <> 'EXPIRED'
        group by tablespace_name) b,v$instance I
        where a.tablespace_name = b.tablespace_name
        
        '''

        # sql = '''
        # select a.*,I.host_name,I.instance_name
        # from (SELECT seg.tablespace_name "TNM",
        # round(ts.bytes / 1024 / 1024/ 1024,2) "TOTAL",
        # round((ts.bytes-sum(ue.bytes))/1024/1024/1024,2) "FREE",
        # round(sum(ue.bytes) / 1024 / 1024/1024, 2) "USED",
        # round(sum(ue.bytes) / ts.bytes * 100, 2) "% USED"
        # FROM dba_segments seg,
        # DBA_UNDO_EXTENTS ue,
        # (SELECT tablespace_name, sum(bytes) bytes
        # FROM dba_data_files
        # GROUP BY tablespace_name) ts
        # WHERE ue.segment_NAME = seg.segment_NAME
        # and seg.tablespace_name = ts.tablespace_name
        # and ue.status<>'EXPIRED'
        # GROUP BY seg.tablespace_name, ts.bytes
        # ORDER BY seg.tablespace_name)a ,v$instance I
        # '''
        try:
            conn = cx_Oracle.connect("system/sysnotallow@" + tns)
            cursor = conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            list = []
            col_name = cursor.description
            for row in results:
                d = {}
                d['DB_NAME'] = db_name
                for col in range(len(col_name)):
                    key = col_name[col][0]
                    value = row[col]
                    d[key] = value
                list.append(d)
            conn.commit()
            return list
        except Exception as e:
            print("query_undo_tablespace error, db_name={0}, e={1},tns ={2}".format(db_name,e,tns))

if __name__ == '__main__':
    starttime = datetime.datetime.now()
    job = UndoTablespaceJob()
    job.initialize()
    job.start()
    endtime = datetime.datetime.now()
    time = (endtime - starttime).seconds
    print("\n times:{0}".format(time))