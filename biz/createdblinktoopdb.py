from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor

import cx_Oracle
import datetime

from biz.wbxjob import WbxJob
from dao.wbxauditdbdao import wbxauditdbdao

test_dbs=[]
# test_dbs=['RACSJMMP']
new_tns="(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.8.228)(PORT = 1701))(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.8.229)(PORT = 1701))(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.8.230)(PORT = 1701))(LOAD_BALANCE = yes)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = racopdbha.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 5))))"
# new_tns=""

class Createdblinktoopdb(WbxJob):

    def start(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        self.db_schema_pwds={}
        self.main()

    def main(self):
        self.auditdao = wbxauditdbdao(self.depot_connectionurl)
        print("start Createdblinktoopdb")
        rows = []
        pwds = []
        try:
            self.auditdao.connect()
            self.auditdao.startTransaction()
            rows = self.auditdao.getDBTnsInfo()
            pwds = self.auditdao.getDBSchemaPwdInfo()
            self.auditdao.commit()
        except Exception as e:
            self.auditdao.rollback()
            print(e)
        finally:
            self.auditdao.close()
        print(len(rows))
        print(len(pwds))
        tns_vo = {}

        for vo in rows:
            item = dict(vo)
            db_name = item['db_name']
            trim_host = item['trim_host']
            listener_port = item['listener_port']
            service_name = "%s.webex.com" % item['service_name']
            key = str(db_name + "_" + trim_host)
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
        print("tns_vo:{0}".format(len(tns_vo)))

        db_schema_pwds = {}
        tgt_dbs=[]
        for pwd in pwds:
            item = dict(pwd)
            db_name = item['db_name']
            trim_host = item['trim_host']
            schema = item['schema']
            pwd = item['pwd']
            key = str(db_name + "_" +trim_host+"_"+schema)
            db_schema_pwds[key] = pwd
            if str(db_name + "_" + trim_host) not in tgt_dbs and db_name!="RACOPDB" and db_name!="AUDITDB" and db_name!="AUDITGSB":
                if len(test_dbs)>0:
                    if db_name in test_dbs:
                        tgt_dbs.append(str(db_name + "_" + trim_host))
                else:
                    tgt_dbs.append(str(db_name + "_" + trim_host))

        self.db_schema_pwds=db_schema_pwds

        print("tgt_dbs:{0}".format(len(tgt_dbs)))
        print("db_schema_pwd:{0}".format(len(db_schema_pwds)))

        tgt_dbs_dict={}

        with ThreadPoolExecutor(max_workers=20) as t:
            all_task = []
            for db in tgt_dbs:
                v={}
                v['db']= db
                if db not in tns_vo:
                    print("{0} lack of tns infos " .format(db))
                else:
                    v['tns']= tns_vo[db]
                    obj = t.submit(self.get_all_dba_db_links, v)
                    all_task.append(obj)

        # for future in as_completed(all_task):
        #     datas = future.result()
            # print(datas)


    def get_all_dba_db_links(self,item):
        # print(item)
        tns = item['tns']
        db = item['db']
        sql = "select owner,db_link,username,host from dba_db_links where db_link like '%OPDB%' and lower(host) not like '%bopdb%'"
        try:
            conn = cx_Oracle.connect("system/sysnotallow@" +tns)
            cursor = conn.cursor()
            cursor.execute(sql)
            results = cursor.fetchall()
            list = []
            col_name = cursor.description
            for row in results:
                d = {}
                for col in range(len(col_name)):
                    key = col_name[col][0]
                    value = row[col]
                    d[key] = value
                list.append(d)
            conn.commit()
            # conn.close()
            # cursor.close()
            # print(list)
            print("=========== {0} =============" .format(db))
            for dba_db_link in list:
                # print("====================== ")

                owner=str(dba_db_link['OWNER']).lower()
                key = db + "_" + owner
                db_link = str(dba_db_link['DB_LINK']).split(".")[0]
                username = str(dba_db_link['USERNAME']).lower()
                userpwd=""
                host = dba_db_link['HOST']
                if new_tns!="":
                    host = new_tns
                res=None
                #new opdb tns
                #host="(DESCRIPTION =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.8.228)(PORT = 1701))(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.8.229)(PORT = 1701))(ADDRESS = (PROTOCOL = TCP)(HOST = 10.252.8.230)(PORT = 1701))(LOAD_BALANCE = yes)(CONNECT_DATA =(SERVER = DEDICATED)(SERVICE_NAME = racopdbha.webex.com)(FAILOVER_MODE =(TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 5))))"
                tns_prefix=""
                try:
                    if owner=="public" or owner=="sys":
                        tns_prefix="sys/sysnotallow@"
                    elif owner=="wbxdba":
                        tns_prefix="wbxdba/china2000@"
                    else:
                        tns_prefix=owner+"/"+self.db_schema_pwds[key]+"@"
                    if owner=="sys" or owner=="public":
                        connect = cx_Oracle.connect(tns_prefix+tns, mode = cx_Oracle.SYSDBA)
                    else:
                        connect= cx_Oracle.connect(tns_prefix+tns)
                    cur = connect.cursor()
                    sql1=""
                    sql2=""
                    if username=="wbxdba":
                        userpwd="china2000"
                    elif username=="sys" or username=="system":
                        userpwd='sysnotallow'
                    else:
                        k = "RACOPDB_sjdbop_"+username
                        # print("k:{0}, userpwd:{1}".format(k,self.db_schema_pwds[k]))
                        userpwd=self.db_schema_pwds[k]

                    if owner == 'public':
                        sql1 = 'drop public database link %s' %(db_link)
                        sql2 = "create public database link %s connect to %s identified by %s using '%s'" % (
                            db_link,username, userpwd, host)
                    else:
                        sql1 = 'drop database link %s' % (db_link)
                        sql2 = "create database link %s connect to %s identified by %s using '%s'" % (
                            db_link, username, userpwd, host)


                    # print("\n"+"==== db:{0}, owner:{1}, db_link:{2}, username:{3} " .format(db,dba_db_link['OWNER'],dba_db_link['DB_LINK'],dba_db_link['USERNAME']))
                    # print(sql1)
                    # print(sql2)
                    cur.execute(sql1)
                    cur.execute(sql2)

                    # a=cur.execute("select sysdate from dual")
                    try:
                        cur.execute("select host_name from v$instance@%s " %(db_link))
                        res = cur.fetchall()
                        print("db_link:{0}, db:{1}, owner:{2}, username:{3}, userpwd:{4}, res:{5}".format(db_link,db,owner,username, userpwd,res))
                    except Exception as e:
                        print("notice! db_link:{0}, db:{1}, owner:{2}, username:{3}, userpwd:{4}, e:{5}".format(db_link,db,owner,username, userpwd, str(e)))
                    connect.commit()

                except Exception as e:
                    if not tns_prefix:
                        tns_prefix="user login information was not found"
                    print("error!!! create db link fail , db:{0}, owner:{1}, username:{2}, userpwd:{3}, key:{4}, "
                          "tns_prefix:{5}, tns:{6}, dba_db_link:{7} ,error :{8}\n".format(db,owner,username,userpwd,key,tns_prefix,tns,dba_db_link,str(e)))
            return list
        except Exception as e:
            print("get_all_dba_db_links fail e:{0},item:{1}\n ".format(str(e),item))



if __name__ == '__main__':

    starttime = datetime.datetime.now()
    job = Createdblinktoopdb()
    job.initialize()
    job.main()
    endtime = datetime.datetime.now()
    time = (endtime - starttime).seconds
    print("\n times:{0}".format(time))
